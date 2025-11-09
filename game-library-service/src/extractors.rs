use axum::{
    RequestPartsExt,
    body::Bytes,
    extract::{
        FromRef, FromRequest, FromRequestParts, Path, Request,
        rejection::{JsonRejection, QueryRejection}
    },
    http::request::Parts
};
use axum_extra::{
    TypedHeader,
    extract::QueryRejection as MultiQueryRejection,
    headers::{
        Authorization,
        authorization::Bearer
    }
};
use glc::discourse::parse_event;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use std::sync::Arc;

use crate::{
    app::DiscourseUpdateConfig,
    core::CoreArc,
    errors::AppError,
    jwt::{self, Claims, DecodingKey},
    model::{Admin, Flag, Owned, Owner, Package, Project, Release, User}
};

impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
    Arc<DecodingKey>: FromRef<S>
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        // get the bearer token from the Authorization header
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader::<Authorization<Bearer>>>()
            .await
            .or(Err(AppError::Unauthorized))?;

        // verify the token
        let key = Arc::<DecodingKey>::from_ref(state);
        let claims = jwt::verify(bearer.token(), &key)
            .or(Err(AppError::Unauthorized))?;

        Ok(claims)
    }
}

impl<S> FromRequestParts<S> for User
where
    S: Send + Sync,
    Arc<DecodingKey>: FromRef<S>
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        // check that the requester is authorized
        let claims = Claims::from_request_parts(parts, state).await?;
        // extract the user id
        Ok(User(claims.sub))
// TODO: insert missing users into users table
    }
}

impl<S> FromRequestParts<S> for Admin
where
    S: Send + Sync,
    Arc<DecodingKey>: FromRef<S>,
    Arc<Vec<User>>: FromRef<S>
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        // check that the requester is authorized
        let user = User::from_request_parts(parts, state).await?;

        let admins = Arc::<Vec<User>>::from_ref(state);

        admins.binary_search(&user)
            .and(Ok(Admin(user.0)))
            .or(Err(AppError::Unauthorized))
    }
}

async fn get_path_iter<S>(
    parts: &mut Parts,
    state: &S
) -> Result<impl Iterator<Item=String>, AppError>
where
    S: Send + Sync,
    CoreArc: FromRef<S>
{
    // extract however many path elements there are; should never fail
    Ok(
        Path::<Vec<(String, String)>>::from_request_parts(parts, state)
            .await
            .map_err(|e| AppError::InternalError(e.to_string()))?
            .0
            .into_iter()
            .map(|p| p.1)
    )
}

impl<S> FromRequestParts<S> for Project
where
    S: Send + Sync,
    CoreArc: FromRef<S>
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        let (proj, ) = get_path_iter(parts, state)
            .await?
            .next_tuple()
            .ok_or(AppError::InternalError("empty path iter".into()))?;

        let core = CoreArc::from_ref(state);

        // look up the project id
        Ok(core.get_project_id(&proj).await?)
    }
}

pub struct ProjectPackage(pub Project, pub Package);

impl<S> FromRequestParts<S> for ProjectPackage
where
    S: Send + Sync,
    CoreArc: FromRef<S>
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        let (proj, pkg) = get_path_iter(parts, state)
            .await?
            .next_tuple()
            .ok_or(AppError::InternalError("empty path iter".into()))?;

        let core = CoreArc::from_ref(state);

        // look up the project, package ids
        Ok(
            core.get_project_package_ids(&proj, &pkg)
                .await
                .map(|r| ProjectPackage(r.0, r.1))?
        )
    }
}

pub struct ProjectPackageRelease(pub Project, pub Package, pub Release);

impl<S> FromRequestParts<S> for ProjectPackageRelease
where
    S: Send + Sync,
    CoreArc: FromRef<S>
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        let (proj, pkg, release) = get_path_iter(parts, state)
            .await?
            .next_tuple()
            .ok_or(AppError::InternalError("empty path iter".into()))?;

        let core = CoreArc::from_ref(state);

        // look up the project, package, release ids
        Ok(
            core.get_project_package_release_ids(&proj, &pkg, &release)
                .await
                .map(|r| ProjectPackageRelease(r.0, r.1, r.2))?
        )
    }
}

impl<S> FromRequestParts<S> for Owned
where
    S: Send + Sync,
    Arc<DecodingKey>: FromRef<S>,
    CoreArc: FromRef<S>
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        // check that the requester is authorized
        let user = User::from_request_parts(parts, state).await?;

        // check that that project exists
        let proj = Project::from_request_parts(parts, state).await?;

        let core = CoreArc::from_ref(state);

        // check that that requester owns the project
        match core.user_is_owner(user, proj).await? {
            true => Ok(Owned(Owner(user.0), proj)),
            false =>  Err(AppError::Forbidden)
        }
    }
}

impl<S> FromRequestParts<S> for Flag
where
    S: Send + Sync,
    CoreArc: FromRef<S>
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        let (flag, ) = get_path_iter(parts, state)
            .await?
            .next_tuple()
            .ok_or(AppError::InternalError("empty path iter".into()))?;

        let core = CoreArc::from_ref(state);

        // flag id must be an integer
        let flag = flag.parse::<i64>()
            .or(Err(AppError::NotFound))?;

        // look up the flag id
        Ok(core.get_flag_id(flag).await?)
    }
}

pub struct DiscourseEvent<E>(pub E);

impl<S, T> FromRequest<S> for DiscourseEvent<T>
where
    S: Send + Sync,
    Arc<DiscourseUpdateConfig>: FromRef<S>,
    Bytes: FromRequest<S>,
    T: DeserializeOwned
{
    type Rejection = AppError;

    async fn from_request(
        req: Request,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        let (parts, body) = req.into_parts();
        let uc = Arc::<DiscourseUpdateConfig>::from_ref(state);
        let payload = parse_event(&parts.headers, body, &uc.secret).await?;
        Ok(DiscourseEvent(payload))
    }
}

impl From<JsonRejection> for AppError {
    fn from(err: JsonRejection) -> Self {
        match err {
            JsonRejection::MissingJsonContentType(_) => AppError::BadMimeType,
            _ => AppError::JsonError
        }
    }
}

impl From<QueryRejection> for AppError {
    fn from(_: QueryRejection) -> Self {
       AppError::MalformedQuery
    }
}

impl From<MultiQueryRejection> for AppError {
    fn from(_: MultiQueryRejection) -> Self {
       AppError::MalformedQuery
    }
}

pub struct Wrapper<E>(pub E);

impl<S, T> FromRequestParts<S> for Wrapper<T>
where
    S: Send + Sync,
    T: FromRequestParts<S>,
    AppError: From<<T as FromRequestParts<S>>::Rejection>
{
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        Ok(Wrapper(T::from_request_parts(parts, state).await?))
    }
}

impl<S, T> FromRequest<S> for Wrapper<T>
where
    S: Send + Sync,
    T: FromRequest<S>,
    AppError: From<<T as FromRequest<S>>::Rejection>
{
    type Rejection = AppError;

    async fn from_request(
        req: Request,
        state: &S
    ) -> Result<Self, Self::Rejection>
    {
        Ok(Wrapper(T::from_request(req, state).await?))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use async_trait::async_trait;
    use axum::{
        Router,
        body::Body,
        extract::State,
        http::{
            Method, StatusCode,
            header::AUTHORIZATION
        },
        routing::get
    };
    use glc::model::Users;
    use std::sync::Arc;
    use tower::ServiceExt; // for oneshot

    use crate::{
        app::AppState,
        core::{Core, GetIdError, GetOwnersError, UserIsOwnerError},
        jwt::EncodingKey
    };

    const KEY: &[u8] = b"@wlD+3L)EHdv28u)OFWx@83_*TxhVf9IdUncaAz6ICbM~)j+dH=sR2^LXp(tW31z";

    fn bob_ok() -> Claims {
        Claims {
            sub: 1,
            exp: 899999999999,
            iat: 0
        }
    }

    fn bob_expired() -> Claims {
        Claims {
            sub: 1,
            exp: 0,
            iat: 0
        }
    }

    fn token(key: &[u8], claims: &Claims) -> String {
        let ekey = EncodingKey::from_secret(key);
        let token = jwt::issue(
            &ekey,
            claims.sub,
            claims.iat,
            claims.exp
        ).unwrap();
        format!("Bearer {token}")
    }

    #[tokio::test]
    async fn claims_from_request_parts_ok() {
        let exp = bob_ok();
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(AUTHORIZATION, token(KEY, &exp))
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        assert_eq!(
            Claims::from_request_parts(&mut parts, &dkey).await.unwrap(),
            exp
        );
    }

    #[tokio::test]
    async fn claims_from_request_parts_expired() {
        let exp = bob_expired();
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(AUTHORIZATION, token(KEY, &exp))
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        let act = Claims::from_request_parts(&mut parts, &dkey).await;
        assert!(act.is_err());
    }

    #[tokio::test]
    async fn claims_from_request_parts_wrong_key() {
        let exp = bob_ok();
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(AUTHORIZATION, token(b"wrong key", &exp))
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        let act = Claims::from_request_parts(&mut parts, &dkey).await;
        assert!(act.is_err());
    }

    #[tokio::test]
    async fn claims_from_request_parts_no_token() {
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(AUTHORIZATION, "")
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        let act = Claims::from_request_parts(&mut parts, &dkey).await;
        assert!(act.is_err());
    }

    #[tokio::test]
    async fn claims_from_request_parts_no_auth_header() {
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        let act = Claims::from_request_parts(&mut parts, &dkey).await;
        assert!(act.is_err());
    }

    #[tokio::test]
    async fn user_from_request_parts_ok() {
        let exp = bob_ok();
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(AUTHORIZATION, token(KEY, &exp))
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        let act = User::from_request_parts(&mut parts, &dkey).await.unwrap();
        assert_eq!(act, User(exp.sub));
    }

    #[tokio::test]
    async fn user_from_request_parts_expired() {
        let exp = bob_expired();
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(AUTHORIZATION, token(KEY, &exp))
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        let act = User::from_request_parts(&mut parts, &dkey).await;
        assert!(act.is_err());
    }

    #[tokio::test]
    async fn user_from_request_parts_wrong_key() {
        let exp = bob_ok();
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(AUTHORIZATION, token(b"wrong key", &exp))
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        let act = User::from_request_parts(&mut parts, &dkey).await;
        assert!(act.is_err());
    }

    #[tokio::test]
    async fn user_from_request_parts_no_token() {
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(AUTHORIZATION, "")
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        let act = User::from_request_parts(&mut parts, &dkey).await;
        assert!(act.is_err());
    }

    #[tokio::test]
    async fn user_from_request_parts_no_auth_header() {
        let dkey = Arc::new(DecodingKey::from_secret(KEY));

        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(())
            .unwrap();

        let mut parts;
        (parts, _) = request.into_parts();

        let act = User::from_request_parts(&mut parts, &dkey).await;
        assert!(act.is_err());
    }

    fn make_state(core: impl Core + Send + Sync + 'static) -> AppState {
        AppState {
            key: Arc::new(DecodingKey::from_secret(KEY)),
            core: Arc::new(core) as CoreArc,
            admins: Arc::new(vec![]),
            discourse_update_config: Default::default()
        }
    }

    // We have to test Project::from_request_parts via a Router because
    // Path uses a private extension to get parameters from the request

    #[derive(Clone)]
    struct ProjectTestCore {}

    #[async_trait]
    impl Core for ProjectTestCore {
        async fn get_project_id(
            &self,
            proj: &str
        ) -> Result<Project, GetIdError>
        {
            match proj {
                "a_project" => Ok(Project(42)),
                _ => Err(GetIdError::NotFound)
            }
        }
    }

    async fn project_ok(
        proj: Project,
        State(_): State<AppState>
    )
    {
        assert_eq!(proj, Project(42));
    }

    async fn project_fail(
        _proj: Project,
        State(_): State<AppState>
    )
    {
        unreachable!();
    }

    #[tokio::test]
    async fn project_from_request_parts_ok() {
        let app = Router::new()
            .route("/{proj}", get(project_ok))
            .with_state(make_state(ProjectTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn project_from_request_parts_not_a_project() {
        let app = Router::new()
            .route("/{proj}", get(project_fail))
            .with_state(make_state(ProjectTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/not_a_project")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // We have to test Owner::from_request_parts via a Router because
    // Path uses a private extension to get parameters from the request

    #[derive(Clone)]
    struct OwnersTestCore {}

    #[async_trait]
    impl Core for OwnersTestCore {
        async fn get_project_id(
            &self,
            proj: &str
        ) -> Result<Project, GetIdError>
        {
            match proj {
                "a_project" => Ok(Project(42)),
                _ => Err(GetIdError::NotFound)
            }
        }

        async fn user_is_owner(
            &self,
            user: User,
            proj: Project
        ) -> Result<bool, UserIsOwnerError>
        {
            Ok(user == User(1) && proj == Project(42))
        }

        async fn get_owners(
            &self,
            _proj: Project
        ) -> Result<Users, GetOwnersError>
        {
            Ok(Users { users: vec!["bob".into()] })
        }
    }

    async fn owned_ok(
        owned: Owned,
        State(_): State<AppState>
    )
    {
        assert_eq!(owned, Owned(Owner(1), Project(42)));
    }

    async fn owned_fail(
        _: Owned,
        State(_): State<AppState>
    )
    {
        unreachable!();
    }

    #[tokio::test]
    async fn owners_from_request_parts_ok() {
        let exp = bob_ok();

        let app = Router::new()
            .route("/{proj}", get(owned_ok))
            .with_state(make_state(OwnersTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project")
                    .header(AUTHORIZATION, token(KEY, &exp))
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn owners_from_request_parts_not_owner() {
        let exp = Claims {
            sub: 2,
            exp: 899999999999,
            iat: 0
        };

        let app = Router::new()
            .route("/{proj}", get(owned_fail))
            .with_state(make_state(OwnersTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project")
                    .header(AUTHORIZATION, token(KEY, &exp))
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn owners_from_request_parts_expired() {
        let exp = bob_expired();

        let app = Router::new()
            .route("/{proj}", get(owned_fail))
            .with_state(make_state(OwnersTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project")
                    .header(AUTHORIZATION, token(KEY, &exp))
                    .body(Body::empty())
                    .unwrap()
             )
             .await
             .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn owners_from_request_parts_wrong_key() {
        let exp = bob_ok();

        let app = Router::new()
            .route("/{proj}", get(owned_fail))
            .with_state(make_state(OwnersTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project")
                    .header(AUTHORIZATION, token(b"wrong key", &exp))
                    .body(Body::empty())
                    .unwrap()
             )
             .await
             .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn owners_from_request_parts_no_token() {
        let app = Router::new()
            .route("/{proj}", get(owned_fail))
            .with_state(make_state(OwnersTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project")
                    .header(AUTHORIZATION, "")
                    .body(Body::empty())
                    .unwrap()
             )
             .await
             .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn owners_from_request_parts_no_auth_header() {
        let app = Router::new()
            .route("/{proj}", get(owned_fail))
            .with_state(make_state(OwnersTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project")
                    .body(Body::empty())
                    .unwrap()
             )
             .await
             .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // We have to test ProjectPackage::from_request_parts via a Router because
    // Path uses a private extension to get parameters from the request

    #[derive(Clone)]
    struct ProjectPackageTestCore {}

    #[async_trait]
    impl Core for ProjectPackageTestCore {
        async fn get_project_package_ids(
            &self,
            proj: &str,
            pkg: &str
        ) -> Result<(Project, Package), GetIdError>
        {
            match (proj, pkg) {
                ("a_project", "a_package") => Ok((Project(42), Package(42))),
                _ => Err(GetIdError::NotFound)
            }
        }
    }

    async fn project_package_ok(
        ProjectPackage(proj, pkg): ProjectPackage,
        State(_): State<AppState>
    )
    {
        assert_eq!(proj, Project(42));
        assert_eq!(pkg, Package(42));
    }

    async fn project_package_fail(
        ProjectPackage(_proj, _pkg): ProjectPackage,
        State(_): State<AppState>
    )
    {
        unreachable!();
    }

    #[tokio::test]
    async fn project_package_from_request_parts_ok() {
        let app = Router::new()
            .route("/{proj}/{pkg}", get(project_package_ok))
            .with_state(make_state(ProjectPackageTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project/a_package")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn project_package_from_request_parts_not_a_project() {
        let app = Router::new()
            .route("/{proj}/{pkg}", get(project_package_fail))
            .with_state(make_state(ProjectPackageTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/not_a_project/a_package")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn project_package_from_request_parts_not_a_package() {
        let app = Router::new()
            .route("/{proj}/{pkg}", get(project_package_fail))
            .with_state(make_state(ProjectPackageTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project/not_a_package")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn project_package_from_request_parts_urlencoded_package() {
        let app = Router::new()
            .route("/{proj}/{pkg}", get(project_package_ok))
            .with_state(make_state(ProjectPackageTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project/%61_%70%61%63%6B%61%67%65")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn project_package_from_request_parts_urlencoded_not_a_package() {
        let app = Router::new()
            .route("/{proj}/{pkg}", get(project_package_fail))
            .with_state(make_state(ProjectPackageTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project/not_%61_package")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn project_package_from_request_parts_urlencoded_bad_utf8() {
        let app = Router::new()
            .route("/{proj}/{pkg}", get(project_package_fail))
            .with_state(make_state(ProjectPackageTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project/%C3%B1")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // We have to test ProjectPackageRelease::from_request_parts via a Router
    // because Path uses a private extension to get parameters from the request

    #[derive(Clone)]
    struct ProjectPackageReleaseTestCore {}

    #[async_trait]
    impl Core for ProjectPackageReleaseTestCore {
        async fn get_project_package_release_ids(
            &self,
            proj: &str,
            pkg: &str,
            release: &str
        ) -> Result<(Project, Package, Release), GetIdError>
        {
            match (proj, pkg, release) {
                ("a_project", "a_package", "1.2.3") => Ok(
                    (Project(42), Package(42), Release(1))
                ),
                _ => Err(GetIdError::NotFound)
            }
        }
    }

    async fn project_package_release_ok(
        ProjectPackageRelease(proj, pkg, release): ProjectPackageRelease,
        State(_): State<AppState>
    )
    {
        assert_eq!(proj, Project(42));
        assert_eq!(pkg, Package(42));
        assert_eq!(release, Release(1));
    }

    async fn project_package_release_fail(
        ProjectPackageRelease(_proj, _pkg, _release): ProjectPackageRelease,
        State(_): State<AppState>
    )
    {
        unreachable!();
    }

    #[tokio::test]
    async fn project_package_release_from_request_parts_ok() {
        let app = Router::new()
            .route("/{proj}/{pkg}/{release}", get(project_package_release_ok))
            .with_state(make_state(ProjectPackageReleaseTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project/a_package/1.2.3")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn project_package_release_from_request_parts_not_a_project() {
        let app = Router::new()
            .route("/{proj}/{pkg}/{release}", get(project_package_release_fail))
            .with_state(make_state(ProjectPackageReleaseTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/not_a_project/a_package/1.2.3")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn project_package_release_from_request_parts_not_a_package() {
        let app = Router::new()
            .route("/{proj}/{pkg}/{release}", get(project_package_release_fail))
            .with_state(make_state(ProjectPackageReleaseTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project/not_a_package/1.2.3")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn project_package_release_from_request_parts_not_a_release() {
        let app = Router::new()
            .route("/{proj}/{pkg}/{release}", get(project_package_release_fail))
            .with_state(make_state(ProjectPackageReleaseTestCore {}));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/a_project/a_package/1.2.4")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
