use axum::{
    body::Bytes,
    extract::{Path, Request, State},
    response::{Json, Redirect}
};
use axum_extra::{
    extract::Query as MultiQuery,
    TypedHeader,
    headers::{ContentLength, ContentType}
};
use futures::{Stream, TryStreamExt};
use glc::model::{Flags, ProjectData, Projects, Publishers, Tags, Users};
use http_body_util::{BodyExt, Limited, LengthLimitError};
use std::{
    error::Error,
    io
};

use crate::{
    core::CoreArc,
    errors::AppError,
    extractors::{ProjectPackage, ProjectPackageRelease, Wrapper},
    input::{FlagPost, GalleryPatch, PackageDataPatch, PackageDataPost, ProjectDataPatch, ProjectDataPost},
    model::{Admin, Flag, Owned, Project, User},
    params::ProjectsParams,
};

pub async fn not_found() -> Result<(), AppError>
{
    Err(AppError::NotFound)
}

pub async fn forbidden() -> Result<(), AppError>
{
    Err(AppError::Forbidden)
}

pub async fn root_get() -> &'static str {
    "hello world"
}

pub async fn projects_get(
    Wrapper(MultiQuery(params)): Wrapper<MultiQuery<ProjectsParams>>,
    State(core): State<CoreArc>
) -> Result<Json<Projects>, AppError>
{
    Ok(Json(core.get_projects(params).await?))
}

pub async fn project_get(
    proj: Project,
    State(core): State<CoreArc>
) -> Result<Json<ProjectData>, AppError>
{
    Ok(Json(core.get_project(proj).await?))
}

pub async fn project_post(
    owner: User,
    Path(proj): Path<String>,
    State(core): State<CoreArc>,
    Wrapper(Json(proj_data)): Wrapper<Json<ProjectDataPost>>
) -> Result<(), AppError>
{
    Ok(core.create_project(owner, &proj, &proj_data).await?)
}

pub async fn project_patch(
    Owned(owner, proj): Owned,
    State(core): State<CoreArc>,
    Wrapper(Json(proj_data)): Wrapper<Json<ProjectDataPatch>>
) -> Result<(), AppError>
{
    Ok(core.update_project(owner, proj, &proj_data).await?)
}

pub async fn project_revision_get(
    proj: Project,
    Path((_, revision)): Path<(String, u32)>,
    State(core): State<CoreArc>
) -> Result<Json<ProjectData>, AppError>
{
    Ok(Json(core.get_project_revision(proj, revision as i64).await?))
}

pub async fn owners_get(
    proj: Project,
    State(core): State<CoreArc>
) -> Result<Json<Users>, AppError>
{
    Ok(Json(core.get_owners(proj).await?))
}

pub async fn owners_add(
    Owned(_, proj): Owned,
    State(core): State<CoreArc>,
    Wrapper(Json(owners)): Wrapper<Json<Users>>
) -> Result<(), AppError>
{
    Ok(core.add_owners(&owners, proj).await?)
}

pub async fn owners_remove(
    Owned(_, proj): Owned,
    State(core): State<CoreArc>,
    Wrapper(Json(owners)): Wrapper<Json<Users>>
) -> Result<(), AppError>
{
    Ok(core.remove_owners(&owners, proj).await?)
}

pub async fn players_get(
    proj: Project,
    State(core): State<CoreArc>
) -> Result<Json<Users>, AppError>
{
    Ok(Json(core.get_players(proj).await?))
}

pub async fn players_add(
    requester: User,
    proj: Project,
    State(core): State<CoreArc>
) -> Result<(), AppError>
{
    Ok(core.add_player(requester, proj).await?)
}

pub async fn players_remove(
    requester: User,
    proj: Project,
    State(core): State<CoreArc>,
) -> Result<(), AppError>
{
    Ok(core.remove_player(requester, proj).await?)
}

pub async fn package_post(
    Owned(owner, proj): Owned,
    Path((_, pkg)): Path<(String, String)>,
    State(core): State<CoreArc>,
    Wrapper(Json(pkg_data)): Wrapper<Json<PackageDataPost>>
) -> Result<(), AppError>
{
    Ok(core.create_package(owner, proj, &pkg, &pkg_data).await?)
}

pub async fn package_patch(
    Owned(owner, proj): Owned,
    ProjectPackage(_, pkg): ProjectPackage,
    State(core): State<CoreArc>,
    Wrapper(Json(pkg_data)): Wrapper<Json<PackageDataPatch>>
) -> Result<(), AppError>
{
    Ok(core.update_package(owner, proj, pkg, &pkg_data).await?)
}

pub async fn package_delete(
    Owned(owner, proj): Owned,
    ProjectPackage(_, pkg): ProjectPackage,
    State(core): State<CoreArc>
) -> Result<(), AppError>
{
    Ok(core.delete_package(owner, proj, pkg).await?)
}

pub async fn release_post(
    Owned(owner, proj): Owned,
    ProjectPackage(_, pkg): ProjectPackage,
    Path((_, _, version)): Path<(String, String, String)>,
    State(core): State<CoreArc>
) -> Result<(), AppError>
{
    Ok(core.create_release(owner, proj, pkg, &version).await?)
}

pub async fn release_delete(
    Owned(owner, proj): Owned,
    ProjectPackageRelease(_, _, release): ProjectPackageRelease,
    State(core): State<CoreArc>
) -> Result<(), AppError>
{
    Ok(core.delete_release(owner, proj, release).await?)
}

fn unpack_limited_error(e: Box<dyn Error + Sync + Send>) -> io::Error {
    // turn boxed error back into io::Error
    match e.downcast::<io::Error>() {
        Ok(e) => *e,
        Err(e) => match e.downcast::<LengthLimitError>() {
            Ok(e) => io::Error::new(io::ErrorKind::FileTooLarge, e),
            Err(e) => io::Error::other(e)
        }
    }
}

fn into_stream(
    request: Request,
    limit: usize
) -> Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
{
     Box::new(
        Limited::new(request.into_body(), limit)
            .into_data_stream()
            .map_err(unpack_limited_error)
    )
}

/*
  TODO: Sending a 413 repsonse before reading all the data causes clients
  using HTTP/1.1 to ignore the response and keep the connection open, waiting
  for a response until the gateway in front of the GLS times out and returns
  a 502. It would be more robust to wait until the client is done sending
  data if we're using HTTP/1.1.
*/

fn limit_content_length(
    content_length: Option<u64>,
    max_size: usize
) -> Result<(Option<u64>, usize), AppError>
{
    content_length
        .map_or(
            Some((None, max_size)),
            |cl| cl.try_into().map(|cl| (Some(cl as u64), cl)).ok()
        )
        .filter(|(_, lim)| *lim <= max_size)
        .ok_or(AppError::TooLarge)
}

pub async fn file_post(
    Owned(owner, proj): Owned,
    ProjectPackageRelease(_, _, release): ProjectPackageRelease,
    Path((_, _, _, filename)): Path<(String, String, String, String)>,
    content_type: Option<TypedHeader<ContentType>>,
    content_length: Option<TypedHeader<ContentLength>>,
    State(core): State<CoreArc>,
    request: Request
) -> Result<(), AppError>
{
    let (content_length, limit) = limit_content_length(
        content_length.map(|cl| cl.0.0),
        core.max_file_size()
    )?;

    Ok(
        core.add_file(
            owner,
            proj,
            release,
            &filename,
            content_type.map(|h| h.0.into()).as_ref(),
            content_length,
            into_stream(request, limit)
        ).await?
    )
}

pub async fn gallery_post(
    Owned(owner, proj): Owned,
    Path((_, img_name)): Path<(String, String)>,
    content_type: Option<TypedHeader<ContentType>>,
    content_length: Option<TypedHeader<ContentLength>>,
    State(core): State<CoreArc>,
    request: Request
) -> Result<(), AppError>
{

    let (content_length, limit) = limit_content_length(
        content_length.map(|cl| cl.0.0),
        core.max_file_size()
    )?;

    // NB: No ContentType header will result in BAD_REQUEST by default, so
    // have to make it optional and check manually
    Ok(
        core.add_gallery_image(
            owner,
            proj,
            &img_name,
            &content_type.ok_or(AppError::BadMimeType)?.0.into(),
            content_length,
            into_stream(request, limit)
        ).await?
    )
}

pub async fn gallery_patch(
    Owned(owner, proj): Owned,
    State(core): State<CoreArc>,
    Wrapper(Json(gallery_patch)): Wrapper<Json<GalleryPatch>>
) -> Result<(), AppError>
{
    Ok(core.update_gallery(owner, proj, &gallery_patch).await?)
}

pub async fn image_get(
    proj: Project,
    Path((_, img_name)): Path<(String, String)>,
    State(core): State<CoreArc>
) -> Result<Redirect, AppError>
{
    Ok(Redirect::to(&core.get_image(proj, &img_name).await?))
}

pub async fn image_revision_get(
    proj: Project,
    Path((_, img_name, revision)): Path<(String, String, u32)>,
    State(core): State<CoreArc>
) -> Result<Redirect, AppError>
{
    Ok(
        Redirect::to(
            &core.get_image_revision(proj, revision as i64, &img_name).await?
        )
    )
}

pub async fn image_post(
    Owned(owner, proj): Owned,
    Path((_, img_name)): Path<(String, String)>,
    content_type: Option<TypedHeader<ContentType>>,
    content_length: Option<TypedHeader<ContentLength>>,
    State(core): State<CoreArc>,
    request: Request
) -> Result<(), AppError>
{
    let (content_length, limit) = limit_content_length(
        content_length.map(|cl| cl.0.0),
        core.max_file_size()
    )?;

    // NB: No ContentType header will result in BAD_REQUEST by default, so
    // have to make it optional and check manually
    Ok(
        core.add_image(
            owner,
            proj,
            &img_name,
            &content_type.ok_or(AppError::BadMimeType)?.0.into(),
            content_length,
            into_stream(request, limit)
        ).await?
    )
}

pub async fn publishers_get(
    State(core): State<CoreArc>
) -> Result<Json<Publishers>, AppError>
{
    Ok(Json(core.get_publishers().await?))
}

pub async fn tags_get(
    State(core): State<CoreArc>
) -> Result<Json<Tags>, AppError>
{
    Ok(Json(core.get_tags().await?))
}

pub async fn flag_post(
    requester: User,
    proj: Project,
    State(core): State<CoreArc>,
    Wrapper(Json(flag)): Wrapper<Json<FlagPost>>,
) -> Result<(), AppError>
{
    Ok(core.add_flag(requester, proj, &flag).await?)
}

pub async fn admin_flag_close(
    admin: Admin,
    flag: Flag,
    State(core): State<CoreArc>
) -> Result<(), AppError>
{
    Ok(core.close_flag(admin, flag).await?)
}

pub async fn admin_flags_get(
    _admin: Admin,
    State(core): State<CoreArc>
) -> Result<Json<Flags>, AppError>
{
    Ok(Json(core.get_flags().await?))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn limit_content_length_under_limit() {
        let len = 20;
        assert_eq!(
            limit_content_length(Some(len as u64), len + 1).unwrap(),
            (Some(len as u64), len)
        );
    }

    #[test]
    fn limit_content_length_at_limit() {
        let len = 20;
        assert_eq!(
            limit_content_length(Some(len as u64), len).unwrap(),
            (Some(len as u64), len)
        );
    }

    #[test]
    fn limit_content_length_too_long() {
        let len = 20;
        assert_eq!(
            limit_content_length(Some((len as u64) + 1), len).unwrap_err(),
            AppError::TooLarge
        );
    }

    #[test]
    fn limit_content_length_way_too_long() {
        let len = 20;
        assert_eq!(
            limit_content_length(Some(u64::MAX), len).unwrap_err(),
            AppError::TooLarge
        );
    }

    #[test]
    fn limit_content_length_no_content_length() {
        let len = 20;
        assert_eq!(
            limit_content_length(None, len).unwrap(),
            (None, len)
        );
    }
}
