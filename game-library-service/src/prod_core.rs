use async_tempfile::TempFile;
use async_trait::async_trait;
use axum::body::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use futures_util::future::try_join_all;
use glc::{
    discourse::UserUpdateParams,
    model::{FileData, FlagData, Flags, GalleryImage, GameData, PackageData, ProjectData, Projects, ProjectSummary, Publishers, Range, ReleaseData, Tags, Users},
    pagination::{Anchor, Direction, Facet, Limit, SortBy, Pagination, Seek, SeekLink}
};
use mime::Mime;
use std::{
    future::Future,
    io,
    path::PathBuf
};
use tokio::io::{
    AsyncRead,
    AsyncReadExt,
    AsyncSeek,
    AsyncSeekExt
};
use tracing::info;
use unicode_ccc::{CanonicalCombiningClass, get_canonical_combining_class};
use unicode_normalization::UnicodeNormalization;
use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};

use crate::{
    content_type::{infer_image_type, infer_file_type, supported_image_type},
    core::{AddImageError, AddFileError, AddFlagError, AddOwnersError, AddPlayerError, CloseFlagError, Core, CreatePackageError, CreateProjectError, CreateReleaseError, DeletePackageError, DeleteReleaseError, GetFlagsError, GetIdError, GetImageError, GetPlayersError, GetProjectError, GetProjectsError, GetPublishersError, GetOwnersError, GetTagsError, RemoveOwnersError, RemovePlayerError, UpdateGalleryError, UpdatePackageError, UpdateProjectError, UpdateUserError, UserIsOwnerError},
    db::{DatabaseClient, DatabaseError, FileRow, FlagRow, MidField, PackageRow, ProjectRow, ProjectSummaryRow, ReleaseRow},
    input::{is_valid_package_name, slug_for, ConsecutiveWhitespace, FlagPost, GameDataPatch, GameDataPost, GalleryPatch, PackageDataPatch, PackageDataPost, ProjectDataPatch, ProjectDataPost},
    model::{Admin, Flag, Owner, Package, Project, Release, User},
    module::{dump_moduledata, versions_in_moduledata},
    params::ProjectsParams,
    time::{self, nanos_to_rfc3339, rfc3339_to_nanos},
    upload::{Uploader, safe_filename, stream_to_writer},
    version::Version
};

#[derive(Clone)]
pub struct ProdCore<C: DatabaseClient, U: Uploader> {
    pub db: C,
    pub uploader: U,
    pub now: fn() -> DateTime<Utc>,
    pub max_file_size: usize,
    pub max_image_size: usize,
    pub upload_dir: PathBuf
}

#[async_trait]
impl<C, U> Core for ProdCore<C, U>
where
    C: DatabaseClient + Send + Sync,
    U: Uploader + Send + Sync
{
    fn max_file_size(&self) -> usize {
        self.max_file_size
    }

    fn max_image_size(&self) -> usize {
        self.max_image_size
    }

    async fn get_user_id(
         &self,
        username: &str
    ) -> Result<User, GetIdError>
    {
        self.db.get_user_id(username)
            .await?
            .ok_or(GetIdError::NotFound)
    }

    async fn get_project_id(
         &self,
        proj: &str
    ) -> Result<Project, GetIdError>
    {
        self.db.get_project_id(proj)
            .await?
            .ok_or(GetIdError::NotFound)
    }

    async fn get_owners(
        &self,
        proj: Project
    ) -> Result<Users, GetOwnersError>
    {
        Ok(self.db.get_owners(proj).await?)
    }

    async fn add_owners(
        &self,
        owners: &Users,
        proj: Project
    ) -> Result<(), AddOwnersError>
    {
        Ok(self.db.add_owners(owners, proj).await?)
    }

    async fn remove_owners(
        &self,
        owners: &Users,
        proj: Project
    ) -> Result<(), RemoveOwnersError>
    {
        Ok(self.db.remove_owners(owners, proj).await?)
    }

    async fn user_is_owner(
        &self,
        user: User,
        proj: Project
    ) -> Result<bool, UserIsOwnerError>
    {
        Ok(self.db.user_is_owner(user, proj).await?)
    }

    async fn get_projects(
        &self,
        params: ProjectsParams
    ) -> Result<Projects, GetProjectsError>
    {
        let ProjectsParams { seek, limit } = params;

        let (prev, next, projects, total) = self.get_projects_from(
            seek, limit.unwrap_or_default()
        ).await?;

        let prev_page = prev.map(|prev| SeekLink::new(&prev, limit));
        let next_page = next.map(|next| SeekLink::new(&next, limit));

        Ok(
            Projects {
                projects,
                meta: Pagination {
                    prev_page,
                    next_page,
                    total
                }
            },
        )
    }

    async fn get_project(
        &self,
        proj: Project
    ) -> Result<ProjectData, GetProjectError>
    {
        self.get_project_impl(
            proj,
            self.db.get_project_row(proj).await?,
            self.db.get_project_tags(proj).await?,
            self.db.get_gallery(proj).await?,
            self.db.get_packages(proj).await?,
            |pc, pkg| pc.db.get_releases(pkg),
            |pc, rel| pc.db.get_files(rel)
        ).await
    }

// TODO: length limits on strings

    async fn create_project(
        &self,
        user: User,
        proj: &str,
        proj_data: &ProjectDataPost
    ) -> Result<(), CreateProjectError>
    {
        let now = self.now_nanos()?;

        check_project_name(&proj_data.name)?;
        check_project_slug(proj, &proj_data.name)?;

        let proj_data = ProjectDataPost {
            game: GameDataPost{
                title_sort_key: title_sort_key(&proj_data.game.title),
                ..proj_data.game.clone()
            },
            ..proj_data.clone()
        };

        Ok(self.db.create_project(user, proj, &proj_data, now).await?)
    }

    async fn update_project(
        &self,
        owner: Owner,
        proj: Project,
        proj_data: &ProjectDataPatch
    ) -> Result<(), UpdateProjectError>
    {
        let now = self.now_nanos()?;

        let proj_data = if let Some(title) = &proj_data.game.title {
            &ProjectDataPatch {
                game: GameDataPatch {
                    title_sort_key: Some(title_sort_key(title)),
                    ..proj_data.game.clone()
                },
                ..proj_data.clone()
            }
        }
        else {
            proj_data
        };

        Ok(self.db.update_project(owner, proj, proj_data, now).await?)
    }

    async fn get_project_revision(
        &self,
        proj: Project,
        revision: i64
    ) -> Result<ProjectData, GetProjectError>
    {
        let proj_row = self.db.get_project_row_revision(proj, revision)
            .await?;
        let mtime = proj_row.modified_at;

        let tags = self.db.get_project_tags_at(proj, mtime).await?;
        let gallery = self.db.get_gallery_at(proj, mtime).await?;
        let package_rows = self.db.get_packages_at(proj, mtime).await?;

        self.get_project_impl(
            proj,
            proj_row,
            tags,
            gallery,
            package_rows,
            |pc, pkg| pc.db.get_releases_at(pkg, mtime),
            |pc, rel| pc.db.get_files_at(rel, mtime)
        ).await
    }

    async fn get_package_id(
         &self,
        proj: Project,
        pkg: &str
    ) -> Result<Package, GetIdError>
    {
        self.db.get_package_id(proj, pkg)
            .await?
            .ok_or(GetIdError::NotFound)
    }

    async fn get_project_package_ids(
         &self,
        proj: &str,
        pkg: &str
    ) -> Result<(Project, Package), GetIdError>
    {
        self.db.get_project_package_ids(proj, pkg)
            .await?
            .ok_or(GetIdError::NotFound)
    }

    async fn create_package(
        &self,
        owner: Owner,
        proj: Project,
        pkg: &str,
        pkg_data: &PackageDataPost
    ) -> Result<(), CreatePackageError>
    {
        let now = self.now_nanos()?;
        check_package_name(&pkg_data.name)?;
        check_package_slug(pkg, &pkg_data.name)?;
        Ok(self.db.create_package(owner, proj, pkg, pkg_data, now).await?)
    }

    async fn update_package(
        &self,
        owner: Owner,
        proj: Project,
        pkg: Package,
        pkg_data: &PackageDataPatch
    ) -> Result<(), UpdatePackageError>
    {
        let now = self.now_nanos()?;

        if let Some(name) = &pkg_data.name {
            check_package_name(name)?;
        }

        Ok(self.db.update_package(owner, proj, pkg, pkg_data, now).await?)
    }

   async fn delete_package(
        &self,
        owner: Owner,
        proj: Project,
        pkg: Package,
    ) -> Result<(), DeletePackageError>
    {
        let now = self.now_nanos()?;
        Ok(self.db.delete_package(owner, proj, pkg, now).await?)
    }

    async fn get_release_id(
        &self,
        proj: Project,
        pkg: Package,
        release: &str
    ) -> Result<Release, GetIdError>
    {
        self.db.get_release_id(proj, pkg, release)
            .await?
            .ok_or(GetIdError::NotFound)
    }

    async fn get_project_package_release_ids(
        &self,
        proj: &str,
        pkg: &str,
        release: &str
    ) -> Result<(Project, Package, Release), GetIdError>
    {
        self.db.get_project_package_release_ids(proj, pkg, release)
            .await?
            .ok_or(GetIdError::NotFound)
    }

    async fn create_release(
        &self,
        owner: Owner,
        proj: Project,
        pkg: Package,
        version: &str
    ) -> Result<(), CreateReleaseError>
    {
        let now = self.now_nanos()?;
        let version = version.parse::<Version>()?;
        Ok(self.db.create_release(owner, proj, pkg, &version, now).await?)
    }

    async fn delete_release(
        &self,
        owner: Owner,
        proj: Project,
        rel: Release
    ) -> Result<(), DeleteReleaseError>
    {
        let now = self.now_nanos()?;
        Ok(self.db.delete_release(owner, proj, rel, now).await?)
    }

    async fn add_file(
        &self,
        owner: Owner,
        proj: Project,
        release: Release,
        filename: &str,
        content_type: Option<&Mime>,
        content_length: Option<u64>,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
    ) -> Result<(), AddFileError>
    {
        info!("starting add_file");
        let now = self.now_nanos()?;

        // ensure the filename is valid
        let filename = safe_filename(filename)
            .or(Err(AddFileError::InvalidFilename))?;

        // write the stream to a file
        let mut file = TempFile::new_in(&*self.upload_dir)
            .await
            .map_err(io::Error::other)?;

        info!("created temp file {}", file.file_path().display());

        let stream = Box::into_pin(stream);

        let (sha256, size) = stream_to_writer(stream, &mut file)
            .await?;

        info!("wrote temp file {}", file.file_path().display());

        // check that the content length, if given, matches what was read
        if content_length.is_some_and(|cl| cl != size) {
            // we know the stream is shorter because were it longer, it
            // would already have failed from hitting the limit
            return Err(
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream shorter than Content-Length"
                )
            )?;
        }

        // try to infer the MIME type
        let ext = filename.rsplit_once('.').map(|(_, ext)| ext);
        let buf = get_magic(&mut file).await?;
        let inferred_type = infer_file_type(ext, &buf)
            .map_err(|_| AddFileError::BadMimeType)?;

        let content_type = match (&inferred_type, content_type) {
            // we detected the MIME type
            (Some(it), _) => it,
            // stated type can be inferred but we didn't detect it
            (None, Some(ct)) if infer::is_mime_supported(ct.as_ref()) =>
                return Err(AddFileError::BadMimeType),
            // stated type cannot be inferred, use it
            (None, Some(ct)) => ct,
            // default to application/octet_stream
            (None, None) => &mime::APPLICATION_OCTET_STREAM
        };

        // We've already verified that any of vlog, vmdx, vmod, vsav
        // is a ZIP archive by this point, and that any ZIP archive
        // has one of those extensions or zip.

        // check uploaded file for moduledata
        let requires = match ext {
            Some(ext @ "vlog") |
            Some(ext @ "vmdx") |
            Some(ext @ "vmod") |
            Some(ext @ "vsav") => {
                file.rewind().await?;
                match dump_moduledata(file.file_path()).await {
                    // we found moduledata
                    Ok(md) => {
                        let (mver, vver) = check_moduledata(&md, ext).await?;

                        if ext == "vmdx" {
                            // extensions need only a valid version
                            Ok(None)
                        }
                        else {
                            // modules, saves, logs must match release version
                            let rver = self.db.get_release_version(release)
                                .await?;
                            check_versions(mver, rver, vver, ext)
                        }
                    },
                    // modules, extensions, saves, logs must have moduledata
                    Err(e) => Err(AddFileError::ModuleError(e))
                }?
            },
            _ => None
        };

        info!("checked version of temp file {}", file.file_path().display());

        // add hash prefix to file upload path
        let bucket_path = format!(
            "{0}/{1}/{filename}",
            &sha256[0..1],
            &sha256[1..2]
        );

        info!("going to rewind temp file {}", file.file_path().display());

        file.rewind().await?;

        info!("starting to upload temp file {}", file.file_path().display());

        let url = self.uploader.upload_with_content_type(
            &bucket_path,
            &mut file,
            content_type.as_ref()
        )
        .await?;

        info!("finished upload of temp file {}", file.file_path().display());

        // update record
        self.db.add_file_url(
            owner,
            proj,
            release,
            filename,
            size as i64,
            &sha256,
            content_type.as_ref(),
            requires.as_deref(),
            &url,
            now
        ).await?;

        Ok(())
    }

    async fn get_players(
        &self,
        proj: Project
    ) -> Result<Users, GetPlayersError>
    {
        Ok(self.db.get_players(proj).await?)
    }

    async fn add_player(
        &self,
        player: User,
        proj: Project
    ) -> Result<(), AddPlayerError>
    {
        Ok(self.db.add_player(player, proj).await?)
    }

    async fn remove_player(
        &self,
        player: User,
        proj: Project
    ) -> Result<(), RemovePlayerError>
    {
        Ok(self.db.remove_player(player, proj).await?)
    }

    async fn add_gallery_image(
        &self,
        owner: Owner,
        proj: Project,
        filename: &str,
        content_type: &Mime,
        content_length: Option<u64>,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
    ) -> Result<(), AddImageError>
    {
        let now = self.now_nanos()?;

        // do the upload
        let url = self.upload_image(
            filename,
            content_type,
            content_length,
            stream,
            now
        ).await?;

        // update record
        self.db.add_gallery_image(
            owner,
            proj,
            filename,
            &url,
            content_type.as_ref(),
            now
        ).await?;

        Ok(())
    }

    async fn update_gallery(
        &self,
        owner: Owner,
        proj: Project,
        gallery_patch: &GalleryPatch
    ) -> Result<(), UpdateGalleryError>
    {
        let now = self.now_nanos()?;
        Ok(self.db.update_gallery(owner, proj, gallery_patch, now).await?)
    }

    async fn get_image(
        &self,
        proj: Project,
        img_name: &str
    ) -> Result<String, GetImageError>
    {
        self.db.get_image_url(proj, img_name)
            .await?
            .ok_or(GetImageError::NotFound)
    }

    async fn get_image_revision(
        &self,
        proj: Project,
        revision: i64,
        img_name: &str
    ) -> Result<String, GetImageError>
    {
        let proj_row = self.db.get_project_row_revision(proj, revision).await?;
        let mtime = proj_row.modified_at;
        self.db.get_image_url_at(proj, img_name, mtime)
            .await?
            .ok_or(GetImageError::NotFound)
    }

// TODO: tests
    async fn add_image(
        &self,
        owner: Owner,
        proj: Project,
        filename: &str,
        content_type: &Mime,
        content_length: Option<u64>,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
    ) -> Result<(), AddImageError>
    {
        let now = self.now_nanos()?;

        // do the upload
        let url = self.upload_image(
            filename,
            content_type,
            content_length,
            stream,
            now
        ).await?;

        // update record
        self.db.add_image_url(
            owner,
            proj,
            filename,
            &url,
            content_type.as_ref(),
            now
        ).await?;

        Ok(())
    }

    async fn get_publishers(&self) -> Result<Publishers, GetPublishersError> {
        Ok(Publishers { publishers: self.db.get_publishers().await? })
    }

    async fn get_tags(&self) -> Result<Tags, GetTagsError> {
        Ok(Tags { tags: self.db.get_tags_active().await? })
    }

    async fn get_flag_id(
         &self,
        flag: i64
    ) -> Result<Flag, GetIdError>
    {
        self.db.get_flag_id(flag)
            .await?
            .ok_or(GetIdError::NotFound)
    }

    async fn add_flag(
        &self,
        reporter: User,
        proj: Project,
        flag: &FlagPost
    ) -> Result<(), AddFlagError>
    {
        let now = self.now_nanos()?;
        Ok(self.db.add_flag(reporter, proj, flag, now).await?)
    }

    async fn close_flag(
        &self,
        admin: Admin,
        flag: Flag
    ) -> Result<(), CloseFlagError>
    {
        let now = self.now_nanos()?;
        Ok(self.db.close_flag(admin, flag, now).await?)
    }

    async fn get_flags(
        &self
    ) -> Result<Flags, GetFlagsError>
    {
        Ok(
            Flags {
                flags: self.db.get_flags()
                    .await?
                    .into_iter()
                    .map(FlagData::try_from)
                    .collect::<Result<Vec<_>, _>>()?
            }
        )
    }

    async fn update_user(
        &self,
        params: &UserUpdateParams
    ) -> Result<(), UpdateUserError> {
        Ok(self.db.update_user(params).await?)
    }
}

impl<C, U> ProdCore<C, U>
where
    C: DatabaseClient + Send + Sync,
    U: Uploader + Send + Sync
{
    fn now_nanos(&self) -> Result<i64, time::Error> {
        let dt = (self.now)();
        dt.timestamp_nanos_opt()
            .ok_or(time::Error::OutOfRangeDateTime(dt))
    }

    async fn make_file_data(
        &self,
        r: FileRow
    ) -> Result<FileData, GetProjectError>
    {
        Ok(
            FileData {
                filename: r.filename,
                url: r.url,
                size: r.size,
                sha256: r.sha256,
                published_at: nanos_to_rfc3339(r.published_at)?,
                published_by: r.published_by,
                requires: r.requires,
            }
        )
    }

    async fn make_release_data<'s, FF, FR>(
        &'s self,
        rr: ReleaseRow,
        get_files_rows: &FF
    ) -> Result<ReleaseData, GetProjectError>
    where
        FF: Fn(&'s Self, Release) -> FR,
        FR: Future<Output = Result<Vec<FileRow>, DatabaseError>>
    {
        let files = try_join_all(
            get_files_rows(self, Release(rr.release_id))
                .await?
                .into_iter()
                .map(|fr| self.make_file_data(fr))
        ).await?;

        Ok(
            ReleaseData {
                version: rr.version,
                files
            }
        )
    }

    async fn make_package_data<'s, RF, RR, FF, FR>(
        &'s self,
        pr: PackageRow,
        get_release_rows: &RF,
        get_files_rows: &FF
    ) -> Result<PackageData, GetProjectError>
    where
        RF: Fn(&'s Self, Package) -> RR,
        RR: Future<Output = Result<Vec<ReleaseRow>, DatabaseError>>,
        FF: Fn(&'s Self, Release) -> FR,
        FR: Future<Output = Result<Vec<FileRow>, DatabaseError>>
    {
        let releases = try_join_all(
            get_release_rows(self, Package(pr.package_id))
                .await?
                .into_iter()
                .map(|rr| self.make_release_data(
                    rr,
                    &get_files_rows
                ))
        ).await?;

        Ok(
            PackageData {
                name: pr.name,
                slug: urlencoding::encode(&pr.slug).into(),
                sort_key: pr.sort_key,
                description: "".into(),
                releases
            }
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_project_impl<'s, RF, RR, FF, FR>(
        &'s self,
        proj: Project,
        proj_row: ProjectRow,
        tags: Vec<String>,
        gallery: Vec<GalleryImage>,
        package_rows: Vec<PackageRow>,
        get_release_rows: RF,
        get_file_rows: FF,
    ) -> Result<ProjectData, GetProjectError>
    where
        RF: Fn(&'s Self, Package) -> RR,
        RR: Future<Output = Result<Vec<ReleaseRow>, DatabaseError>>,
        FF: Fn(&'s Self, Release) -> FR,
        FR: Future<Output = Result<Vec<FileRow>, DatabaseError>>
    {
        let owners = self.db.get_owners(proj)
            .await?
            .users;

        let packages = try_join_all(
            package_rows
                .into_iter()
                .map(|pr| self.make_package_data(
                    pr,
                    &get_release_rows,
                    &get_file_rows
                ))
        ).await?;

        Ok(
            ProjectData {
                name: proj_row.name,
                slug: urlencoding::encode(&proj_row.slug).into(),
                description: proj_row.description,
                revision: proj_row.revision,
                created_at: nanos_to_rfc3339(proj_row.created_at)?,
                modified_at: nanos_to_rfc3339(proj_row.modified_at)?,
                tags,
                game: GameData {
                    title: proj_row.game_title,
                    title_sort_key: proj_row.game_title_sort,
                    publisher: proj_row.game_publisher,
                    year: proj_row.game_year,
                    players: Range {
                        min: proj_row.game_players_min,
                        max: proj_row.game_players_max
                    },
                    length: Range {
                        min: proj_row.game_length_min,
                        max: proj_row.game_length_max
                    }
                },
                readme: proj_row.readme,
                image: proj_row.image,
                owners,
                packages,
                gallery
            }
        )
    }

    async fn get_projects_mid_window(
        &self,
        facets: &[Facet],
        sort_by: SortBy,
        dir: Direction,
        field: &str,
        id: u32,
        limit: u32
    ) -> Result<Vec<ProjectSummaryRow>, GetProjectsError>
    {
        let mf = match sort_by {
            SortBy::CreationTime |
            SortBy::ModificationTime => MidField::Timestamp(
                rfc3339_to_nanos(field)
                    .map_err(|_| GetProjectsError::MalformedQuery)?
            ),
            SortBy::Relevance => MidField::Weight(
                field.parse::<f64>()
                    .map_err(|_| GetProjectsError::MalformedQuery)?
            ),
            _ => MidField::Text(field)
        };

        Ok(
            self.db.get_projects_mid_window(
                facets,
                sort_by,
                dir,
                mf,
                id,
                limit
            ).await?
        )
    }

    async fn get_projects_window(
        &self,
        facets: &[Facet],
        anchor: &Anchor,
        sort_by: SortBy,
        dir: Direction,
        limit_extra: u32
    ) -> Result<Vec<ProjectSummaryRow>, GetProjectsError>
    {
        match anchor {
            Anchor::Start => Ok(
                self.db.get_projects_end_window(
                    facets,
                    sort_by,
                    dir,
                    limit_extra
                ).await?
            ),
            Anchor::After(field, id) =>
                self.get_projects_mid_window(
                    facets,
                    sort_by,
                    dir,
                    field,
                    *id,
                    limit_extra
                ).await,
            Anchor::Before(field, id) =>
                self.get_projects_mid_window(
                    facets,
                    sort_by,
                    dir.rev(),
                    field,
                    *id,
                    limit_extra
                ).await
        }
    }

    async fn get_projects_from(
        &self,
        seek: Seek,
        limit: Limit
    ) -> Result<
        (
            Option<Seek>,
            Option<Seek>,
            Vec<ProjectSummary>,
            i64
        ),
        GetProjectsError
    >
    {
        // unpack the seek
        let Seek { sort_by, dir, anchor, facets } = seek;

        // get the total number of responsive items
        let total = self.db.get_projects_count(&facets).await?;

        // try to get one extra so we can tell if we're at an endpoint
        let limit_extra = limit.get() as u32 + 1;

        // get the window
        let mut projects = self.get_projects_window(
            &facets,
            &anchor,
            sort_by,
            dir,
            limit_extra
        ).await?;

        // get the prev, next links
        let (prev, next) = get_links(
            &facets,
            &anchor,
            sort_by,
            dir,
            limit_extra,
            &mut projects
        )?;

        // get the tags
        let tags = try_join_all(
            projects.iter()
                .map(|p| self.db.get_project_tags(Project(p.project_id)))
        ).await?;

        // convert the rows and tags to summaries
        let pi = projects.into_iter()
            .zip(tags)
            .map(make_project_summary);

        let psums = match anchor {
            Anchor::Before(..) => pi.rev().collect::<Result<Vec<_>, _>>(),
            _ => pi.collect::<Result<Vec<_>, _>>()
        }?;

        Ok((prev, next, psums, total))
    }

    async fn upload_image(
        &self,
        filename: &str,
        content_type: &Mime,
        content_length: Option<u64>,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>,
        now: i64
    ) -> Result<String, AddImageError>
    {
        // MIME type should be an image
        if !supported_image_type(content_type) {
            return Err(AddImageError::BadMimeType);
        }

        // ensure the filename is valid
        let filename = safe_filename(filename)
            .or(Err(AddImageError::InvalidFilename))?;

        // write the stream to a file
        let mut file = TempFile::new_in(&*self.upload_dir)
            .await
            .map_err(io::Error::other)?;

        let stream = Box::into_pin(stream);

        let (sha256, size) = stream_to_writer(stream, &mut file)
            .await?;

        // check that the content length, if given, matches what was read
        if content_length.is_some_and(|cl| cl != size) {
            // we know the stream is shorter because were it longer, it
            // would already have failed from hitting the limit
            return Err(
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream shorter than Content-Length"
                )
            )?;
        }

        // add hash prefix to file upload path
        let bucket_path = format!(
            "{0}/{1}/{filename}",
            &sha256[0..1],
            &sha256[1..2]
        );

        file.rewind().await?;

        // check actual MIME type
        let ext = filename.rsplit_once('.').map(|(_, ext)| ext);
        let buf = get_magic(&mut file).await?;
        let content_type = infer_image_type(ext, &buf)
            .map_err(|_| AddImageError::BadMimeType)?;

// TODO: check dimensions? resize?

        file.rewind().await?;

        let url = self.uploader.upload_with_content_type(
            &bucket_path,
            &mut file,
            content_type.as_ref()
        )
        .await?;

        Ok(url)
    }
}

fn get_prev_for_before(
    facets: &[Facet],
    anchor: &Anchor,
    sort_by: SortBy,
    dir: Direction,
    limit_extra: u32,
    projects: &mut Vec<ProjectSummaryRow>
) -> Result<Option<Seek>, GetProjectsError>
{
    // make the prev link
    if projects.len() == limit_extra as usize {
        // there are more pages in the forward direction

        // remove the "extra" item which proves we are not at the end
        projects.pop();

        // the prev page is after the last item
        let last = projects.last().expect("element must exist");

        let prev_anchor = match anchor {
            Anchor::Before(..) => Anchor::Before(
                last.sort_field(sort_by)?,
                last.project_id as u32
            ),
            Anchor::Start |
            Anchor::After(..) => unreachable!()
        };

        Ok(Some(Seek {
            anchor: prev_anchor,
            sort_by,
            dir,
            facets: facets.into()
        }))
    }
    else {
        // there are no pages in the forward direction
        Ok(None)
    }
}

fn get_next_for_before(
    facets: &[Facet],
    anchor: &Anchor,
    sort_by: SortBy,
    dir: Direction,
    projects: &[ProjectSummaryRow]
) -> Result<Option<Seek>, GetProjectsError>
{
    // make the next link
    if projects.is_empty() {
        Ok(None)
    }
    else {
        // the next page is before the first item
        let first = projects.first().expect("element must exist");

        let next_anchor = match anchor {
            Anchor::Before(..) => Anchor::After(
                first.sort_field(sort_by)?,
                first.project_id as u32
            ),
            Anchor::Start |
            Anchor::After(..) => unreachable!()
        };

        Ok(Some(Seek {
            anchor: next_anchor,
            sort_by,
            dir,
            facets: facets.into()
        }))
    }
}

fn get_next_for_after(
    facets: &[Facet],
    anchor: &Anchor,
    sort_by: SortBy,
    dir: Direction,
    limit_extra: u32,
    projects: &mut Vec<ProjectSummaryRow>
) -> Result<Option<Seek>, GetProjectsError>
{
    // make the next link
    if projects.len() == limit_extra as usize {
        // there are more pages in the forward direction

        // remove the "extra" item which proves we are not at the end
        projects.pop();

        // the next page is after the last item
        let last = projects.last().expect("element must exist");

        let next_anchor = match anchor {
            Anchor::Start |
            Anchor::After(..) => Anchor::After(
                last.sort_field(sort_by)?,
                last.project_id as u32
            ),
            Anchor::Before(..) => unreachable!()
        };

        Ok(Some(Seek {
            anchor: next_anchor,
            sort_by,
            dir,
            facets: facets.into()
        }))
    }
    else {
        // there are no pages in the forward direction
        Ok(None)
    }
}

fn get_prev_for_after(
    facets: &[Facet],
    anchor: &Anchor,
    sort_by: SortBy,
    dir: Direction,
    projects: &[ProjectSummaryRow]
) -> Result<Option<Seek>, GetProjectsError>
{
    // make the prev link
    match anchor {
        _ if projects.is_empty() => Ok(None),
        Anchor::Start => Ok(None),
        Anchor::After(..) => {
            // the previous page is before the first item
            let first = projects.first().expect("element must exist");

            let prev_anchor = match anchor {
                Anchor::After(..) => Anchor::Before(
                    first.sort_field(sort_by)?,
                    first.project_id as u32
                ),
                Anchor::Start |
                Anchor::Before(..) => unreachable!()
            };

            Ok(Some(Seek {
                anchor: prev_anchor,
                sort_by,
                dir,
                facets: facets.into()
            }))
        },
        Anchor::Before(..) => unreachable!()
    }
}

fn get_links(
    facets: &[Facet],
    anchor: &Anchor,
    sort_by: SortBy,
    dir: Direction,
    limit_extra: u32,
    projects: &mut Vec<ProjectSummaryRow>
) -> Result<(Option<Seek>, Option<Seek>), GetProjectsError>
{
    match anchor {
        Anchor::Before(..) => {
            let prev = get_prev_for_before(
                facets,
                anchor,
                sort_by,
                dir,
                limit_extra,
                projects
            )?;

            let next = get_next_for_before(
                facets,
                anchor,
                sort_by,
                dir,
                projects
            )?;

            Ok((prev, next))
        },
        Anchor::Start |
        Anchor::After(..) => {
            let next = get_next_for_after(
                facets,
                anchor,
                sort_by,
                dir,
                limit_extra,
                projects
            )?;

            let prev = get_prev_for_after(
                facets,
                anchor,
                sort_by,
                dir,
                projects
            )?;

            Ok((prev, next))
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct InvalidPackageName;

fn check_package_name(name: &str) -> Result<(), InvalidPackageName> {
    match is_valid_package_name(name) {
        true => Ok(()),
        false => Err(InvalidPackageName)
    }
}

fn check_package_slug(
    slug: &str,
    name: &str
) -> Result<(), InvalidPackageName>
{
    match slug == slug_for(name) {
        true => Ok(()),
        false => Err(InvalidPackageName)
    }
}

impl From<InvalidPackageName> for CreatePackageError {
    fn from(_: InvalidPackageName) -> Self {
        CreatePackageError::InvalidPackageName
    }
}

impl From<InvalidPackageName> for UpdatePackageError {
    fn from(_: InvalidPackageName) -> Self {
        UpdatePackageError::InvalidPackageName
    }
}

#[derive(Debug, PartialEq, Eq)]
struct InvalidProjectName;

impl From<InvalidProjectName> for CreateProjectError {
    fn from(_: InvalidProjectName) -> Self {
        CreateProjectError::InvalidProjectName
    }
}

fn is_valid_project_name(name: &str) -> bool {
    // project names must be 5 to 64 characters long
    // project names must contain only L, M, N, P, Z category characters
    // reject project names with leading or trailing whitespace
    // reject project names with consecutive whitespace
    !(
        name.len() < 5 ||
        name.len() > 64 ||
        name != name.trim() ||
        name.chars().any(|c|
            ![
                GeneralCategoryGroup::Letter,
                GeneralCategoryGroup::Mark,
                GeneralCategoryGroup::Number,
                GeneralCategoryGroup::Punctuation,
                GeneralCategoryGroup::Separator
            ].contains(&c.general_category_group())
        ) ||
        name.has_consecutive_whitespace()
    )
}

fn check_project_name(name: &str) -> Result<(), InvalidProjectName> {
    match is_valid_project_name(name) {
        true => Ok(()),
        false => Err(InvalidProjectName)
    }
}

fn check_project_slug(
    slug: &str,
    name: &str
) -> Result<(), InvalidProjectName>
{
    match slug == slug_for(name) {
        true => Ok(()),
        false => Err(InvalidProjectName)
    }
}

fn split_title_sort_key(title: &str) -> (&str, Option<&str>) {
    match title.split_once(' ') {
        // Probably Spanish or French, "a" is not an article
        Some(("a", rest)) if rest.starts_with("la ")
            || rest.starts_with("las ") => (title, None),
        // Put leading article at end
        Some((art, rest)) if ["a", "an", "the"].contains(&art)
            => (rest, Some(art)),
        // Doesn't start with an article
        Some(_) | None => (title, None)
    }
}

fn normalize_title_sort_key(s: &str) -> String {
    s.nfkd()
        .flat_map(char::to_lowercase)
        // strip marks
        .filter(|c| get_canonical_combining_class(*c) == CanonicalCombiningClass::NotReordered)
        // skip initial punctuation
        .skip_while(
            |c| ![
                GeneralCategoryGroup::Letter,
                GeneralCategoryGroup::Number
            ].contains(&c.general_category_group())
        )
        .collect()
}

fn title_sort_key(title: &str) -> String {
    let sort_key = normalize_title_sort_key(title);
    match split_title_sort_key(&sort_key) {
        (_, None) => sort_key,
        (rest, Some(art)) => format!("{rest}, {art}")
    }
}

impl ProjectSummaryRow {
    fn sort_field(&self, sort_by: SortBy) -> Result<String, time::Error> {
        Ok(
            match sort_by {
                SortBy::ProjectName => self.name.clone(),
                SortBy::GameTitle => self.game_title_sort.clone(),
                SortBy::ModificationTime => nanos_to_rfc3339(self.modified_at)?,
                SortBy::CreationTime => nanos_to_rfc3339(self.created_at)?,
                SortBy::Relevance => self.rank.to_string()
            }
        )
    }
}

fn make_project_summary(
    t: (ProjectSummaryRow, Vec<String>)
) -> Result<ProjectSummary, time::Error>
{
    let (r, tags) = t;
    Ok(
        ProjectSummary {
            name: r.name,
            slug: r.slug,
            description: r.description,
            revision: r.revision,
            created_at: nanos_to_rfc3339(r.created_at)?,
            modified_at: nanos_to_rfc3339(r.modified_at)?,
            tags,
            game: GameData {
                title: r.game_title,
                title_sort_key: r.game_title_sort,
                publisher: r.game_publisher,
                year: r.game_year,
                players: Range {
                    min: r.game_players_min,
                    max: r.game_players_max
                },
                length: Range {
                    min: r.game_length_min,
                    max: r.game_length_max
                }
            }
        }
    )
}

impl TryFrom<FlagRow> for FlagData {
    type Error = GetFlagsError;

    fn try_from(r: FlagRow) -> Result<Self, Self::Error> {
        Ok(
            FlagData {
                flag_id: r.flag_id,
                project: r.project,
                slug: urlencoding::encode(&r.slug).into(),
                flag: r.flag,
                flagged_by: r.flagged_by,
                flagged_at: nanos_to_rfc3339(r.flagged_at)?,
                message: r.message
            }
        )
    }
}

async fn get_magic<F>(file: &mut F) -> Result<[u8; 64], io::Error>
where
    F: AsyncRead + AsyncSeek + Unpin
{
    file.rewind().await?;
    let mut buf = [0; 64];
    file.read_exact(&mut buf).await?;
    Ok(buf)
}

fn format_minimum_vassal_version(v_vstr: &str) -> Option<String> {
    v_vstr.parse::<Version>()
        .ok()
        .map(|v| format!(">= {}.{}.{}", v.major, v.minor, v.patch))
}

async fn check_moduledata(
    md: &str,
    ext: &str
) -> Result<(Version, Option<String>), AddFileError>
{
    let (mver, vver) = versions_in_moduledata(md)?;
    Ok((
        // enforce that module version is valid
        mver.unwrap_or("".into()).parse::<Version>()?,
        vver
    ))
}

fn check_versions(
    mod_version: Version,
    rel_version: Version,
    vas_version: Option<String>,
    ext: &str
) -> Result<Option<String>, AddFileError>
{
    match mod_version == rel_version {
        true => Ok(match ext == "vmod" {
            // set minimum required Vassal version
            true => vas_version.as_deref()
                .and_then(format_minimum_vassal_version),
                // not a module
            false => None
        }),
        false => Err(AddFileError::ReleaseVersionMismatch(
            mod_version, rel_version
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use once_cell::sync::Lazy;
    use tokio::io::AsyncRead;

    use crate::{
        input::{GameDataPatch, GameDataPost, RangePatch, RangePost},
        sqlite::{Pool, SqlxDatabaseClient},
        upload::UploadError
    };

    const NOW: &str = "2023-11-12T15:50:06.419538067Z";

    static NOW_DT: Lazy<DateTime<Utc>> = Lazy::new(||
        DateTime::parse_from_rfc3339(NOW)
            .unwrap()
            .with_timezone(&Utc)
    );

    fn fake_now() -> DateTime<Utc> {
        *NOW_DT
    }

    struct FakeUploader {}

    impl Uploader for FakeUploader {
        async fn upload<R>(
            &self,
            _filename: &str,
            _reader: R
        ) -> Result<String, UploadError>
        where
            R: AsyncRead + Unpin + Send
        {
            unreachable!();
        }

        async fn upload_with_content_type<R>(
            &self,
            _filename: &str,
            _reader: R,
            _content_type: &str
        ) -> Result<String, UploadError>
        where
            R: AsyncRead + Unpin + Send
        {
            unreachable!();
        }
    }

    fn make_core(
        pool: Pool,
        now: fn() -> DateTime<Utc>
    ) -> ProdCore<SqlxDatabaseClient<sqlx::sqlite::Sqlite>, FakeUploader>
    {
        ProdCore {
            db: SqlxDatabaseClient(pool),
            uploader: FakeUploader {},
            now,
            max_file_size: 256,
            max_image_size: 256,
            upload_dir: "uploads".into()
        }
    }

    fn fake_project_summary(name: &str) -> ProjectSummary {
        ProjectSummary {
            name: name.into(),
            slug: name.into(),
            description: "".into(),
            revision: 1,
            created_at: "1970-01-01T00:00:00Z".into(),
            modified_at: format!(
                "1970-01-01T00:00:00.0000000{:02}Z",
                name.as_bytes()[0] - b'a' + 1
            ),
            tags: vec![],
            game: GameData {
                title: "".into(),
                title_sort_key: "".into(),
                publisher: "".into(),
                year: "".into(),
                players: Range::default(),
                length: Range::default(),
            }
        }
    }

    #[test]
    fn check_package_name_ok() {
        let name = "acceptable_name";
        assert!(check_package_name(name).is_ok());
    }

    #[test]
    fn check_package_name_empty() {
        assert_eq!(
            check_package_name("").unwrap_err(),
            InvalidPackageName
        );
    }

    #[test]
    fn check_package_name_overlong() {
        assert_eq!(
            check_package_name(&"x".repeat(200)).unwrap_err(),
            InvalidPackageName
        );
    }

    #[test]
    fn check_package_name_untrimmed() {
        assert_eq!(
            check_package_name(" x ").unwrap_err(),
            InvalidPackageName
        );
    }

    #[test]
    fn check_package_name_consecutive_whitespace() {
        assert_eq!(
            check_package_name("x  x").unwrap_err(),
            InvalidPackageName
        );
    }

    #[test]
    fn check_project_name_ok() {
        let name = "acceptable_name";
        assert!(check_project_name(name).is_ok());
    }

    #[test]
    fn check_project_name_non_ascii() {
        assert_eq!(
            check_project_name("💩").unwrap_err(),
            InvalidProjectName
        );
    }

    #[test]
    fn check_project_name_leading_non_alphanumeric() {
        assert_eq!(
            check_project_name("-abc").unwrap_err(),
            InvalidProjectName
        );
    }

    #[test]
    fn check_project_name_too_short() {
        assert_eq!(
            check_project_name("").unwrap_err(),
            InvalidProjectName
        );
    }

    #[test]
    fn check_project_name_too_long() {
        assert_eq!(
            check_project_name(&"x".repeat(100)).unwrap_err(),
            InvalidProjectName
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_start_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                anchor: Anchor::Start,
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("a"),
                fake_project_summary("b"),
                fake_project_summary("c")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(prev, None);

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After("c".into(), 3),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_end_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Descending,
                anchor: Anchor::Start,
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("j"),
                fake_project_summary("i"),
                fake_project_summary("h")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(prev, None);

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After("h".into(), 8),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_after_asc_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                anchor: Anchor::After("a".into(), 1),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("b"),
                fake_project_summary("c"),
                fake_project_summary("d")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before("b".into(), 2),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After("d".into(), 4),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_after_desc_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Descending,
                anchor: Anchor::After("h".into(), 8),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("g"),
                fake_project_summary("f"),
                fake_project_summary("e")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before("g".into(), 7),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After("e".into(), 5),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_before_asc_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                anchor: Anchor::Before("e".into(), 5),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("b"),
                fake_project_summary("c"),
                fake_project_summary("d")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before("b".into(), 2),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After("d".into(), 4),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_before_desc_ok(pool: Pool) {
        let core = make_core(pool, fake_now);
        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Descending,
                anchor: Anchor::Before("e".into(), 5),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("h"),
                fake_project_summary("g"),
                fake_project_summary("f")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before("h".into(), 8),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After("f".into(), 6),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_before_asc_no_prev_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                anchor: Anchor::Before("d".into(), 4),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("a"),
                fake_project_summary("b"),
                fake_project_summary("c")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(prev, None);

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After("c".into(), 3),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_after_desc_no_prev_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Descending,
                anchor: Anchor::Before("g".into(), 7),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("j"),
                fake_project_summary("i"),
                fake_project_summary("h")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(prev, None);

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After("h".into(), 8),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_after_asc_no_next_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                anchor: Anchor::After("g".into(), 7),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("h"),
                fake_project_summary("i"),
                fake_project_summary("j")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before("h".into(), 8),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(next, None);
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_pname_after_desc_no_next_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Descending,
                anchor: Anchor::After("d".into(), 4),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("c"),
                fake_project_summary("b"),
                fake_project_summary("a")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before("c".into(), 3),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(next, None);
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_mtime_start_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ModificationTime,
                dir: Direction::Descending,
                anchor: Anchor::Start,
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("j"),
                fake_project_summary("i"),
                fake_project_summary("h")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(prev, None);

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After(
                        "1970-01-01T00:00:00.000000008Z".into(),
                        8
                    ),
                    sort_by: SortBy::ModificationTime,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_mtime_end_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Descending,
                anchor: Anchor::Start,
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("j"),
                fake_project_summary("i"),
                fake_project_summary("h")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(prev, None);

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After("h".into(), 8),
                    sort_by: SortBy::ProjectName,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_mtime_after_asc_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ModificationTime,
                dir: Direction::Ascending,
                anchor: Anchor::After(
                    "1970-01-01T00:00:00.000000001Z".into(),
                    1
                ),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("b"),
                fake_project_summary("c"),
                fake_project_summary("d")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before(
                        "1970-01-01T00:00:00.000000002Z".into(),
                        2
                    ),
                    sort_by: SortBy::ModificationTime,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After(
                        "1970-01-01T00:00:00.000000004Z".into(),
                        4
                    ),
                    sort_by: SortBy::ModificationTime,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_mtime_after_desc_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ModificationTime,
                dir: Direction::Descending,
                anchor: Anchor::After(
                    "1970-01-01T00:00:00.000000008Z".into(),
                    8
                ),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("g"),
                fake_project_summary("f"),
                fake_project_summary("e")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before(
                        "1970-01-01T00:00:00.000000007Z".into(),
                        7
                    ),
                    sort_by: SortBy::ModificationTime,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After(
                        "1970-01-01T00:00:00.000000005Z".into(),
                        5
                    ),
                    sort_by: SortBy::ModificationTime,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_mtime_before_asc_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ModificationTime,
                dir: Direction::Ascending,
                anchor: Anchor::Before(
                    "1970-01-01T00:00:00.000000005Z".into(),
                    5
                ),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("b"),
                fake_project_summary("c"),
                fake_project_summary("d")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before(
                        "1970-01-01T00:00:00.000000002Z".into(),
                        2
                    ),
                    sort_by: SortBy::ModificationTime,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After(
                        "1970-01-01T00:00:00.000000004Z".into(),
                        4
                    ),
                    sort_by: SortBy::ModificationTime,
                    dir: Direction::Ascending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "ten_projects"))]
    async fn get_projects_mtime_before_desc_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let (prev, next, summaries, total) = core.get_projects_from(
            Seek {
                sort_by: SortBy::ModificationTime,
                dir: Direction::Descending,
                anchor: Anchor::Before(
                    "1970-01-01T00:00:00.000000006Z".into(),
                    5
                ),
                facets: vec![]
            },
            Limit::new(3).unwrap()
        ).await.unwrap();

        assert_eq!(
            summaries,
            [
                fake_project_summary("h"),
                fake_project_summary("g"),
                fake_project_summary("f")
            ]
        );

        assert_eq!(total, 10);

        assert_eq!(
            prev,
            Some(
                Seek {
                    anchor: Anchor::Before(
                        "1970-01-01T00:00:00.000000008Z".into(),
                        8
                    ),
                    sort_by: SortBy::ModificationTime,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );

        assert_eq!(
            next,
            Some(
                Seek {
                    anchor: Anchor::After(
                        "1970-01-01T00:00:00.000000006Z".into(),
                        6
                    ),
                    sort_by: SortBy::ModificationTime,
                    dir: Direction::Descending,
                    facets: vec![]
                }
            )
        );
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners", "packages"))]
    async fn get_project_ok(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert_eq!(
            core.get_project(Project(42)).await.unwrap(),
            ProjectData {
                name: "test_game".into(),
                slug: "test_game".into(),
                description: "Brian's Trademarked Game of Being a Test Case".into(),
                revision: 3,
                created_at: "2023-11-12T15:50:06.419538067Z".into(),
                modified_at: "2023-12-14T15:50:06.419538067Z".into(),
                tags: vec![],
                game: GameData {
                    title: "A Game of Tests".into(),
                    title_sort_key: "game of tests, a".into(),
                    publisher: "Test Game Company".into(),
                    year: "1979".into(),
                    players: Range { min: None, max: Some(3) },
                    length: Range::default()
                },
                readme: "".into(),
                image: None,
                owners: vec!["alice".into(), "bob".into()],
                packages: vec![
                    PackageData {
                        name: "a_package".into(),
                        slug: "a_package".into(),
                        sort_key: 0,
                        description: "".into(),
                        releases: vec![
                            ReleaseData {
                                version: "1.2.4".into(),
                                files: vec![
                                    FileData {
                                        filename: "a_package-1.2.4".into(),
                                        url: "https://example.com/a_package-1.2.4".into(),
                                        size: 5678,
                                        sha256: "79fdd8fe3128f818e446e919cce5dcfb81815f8f4341c53f4d6b58ded48cebf2".into(),
                                        published_at: "2023-12-10T15:56:29.180282477Z".into(),
                                        published_by: "alice".into(),
                                        requires: Some(">= 3.7.12".into())
                                    },
                                ],
                            },
                            ReleaseData {
                                version: "1.2.3".into(),
                                files: vec![
                                    FileData {
                                        filename: "a_package-1.2.3".into(),
                                        url: "https://example.com/a_package-1.2.3".into(),
                                        size: 1234,
                                        sha256: "c0e0fa7373a12b45a91e4f4d4e2e186442fc6ee9b346caa2fdc1c09026a2144a".into(),
                                        published_at: "2023-12-09T15:56:29.180282477Z".into(),
                                        published_by: "bob".into(),
                                        requires: Some(">= 3.2.17".into())
                                    }
                                ]
                            }
                        ]
                    },
                    PackageData {
                        name: "b_package".into(),
                        slug: "b_package".into(),
                        sort_key: 1,
                        description: "".into(),
                        releases: vec![]
                    },
                    PackageData {
                        name: "c_package".into(),
                        slug: "c_package".into(),
                        sort_key: 2,
                        description: "".into(),
                        releases: vec![
                            ReleaseData {
                                version: "0.1.0".into(),
                                files: vec![
                                    FileData {
                                        filename: "c_package-0.1.0".into(),
                                        url: "https://example.com/c_package-0.1.0".into(),
                                        size: 123456,
                                        sha256: "a8f515e9e2de99919d1a987733296aaa951a4ba2aa0f7014c510bdbd60dc0efd".into(),
                                        published_at: "2023-12-15T15:56:29.180282477Z".into(),
                                        published_by: "chuck".into(),
                                        requires: None
                                    }
                                ]
                            }
                        ]
                    }
                ],
                gallery: vec![]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners", "packages"))]
    async fn get_project_revision_ok_current(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert_eq!(
            core.get_project_revision(Project(42), 3).await.unwrap(),
            ProjectData {
                name: "test_game".into(),
                slug: "test_game".into(),
                description: "Brian's Trademarked Game of Being a Test Case".into(),
                revision: 3,
                created_at: "2023-11-12T15:50:06.419538067Z".into(),
                modified_at: "2023-12-14T15:50:06.419538067Z".into(),
                tags: vec![],
                game: GameData {
                    title: "A Game of Tests".into(),
                    title_sort_key: "game of tests, a".into(),
                    publisher: "Test Game Company".into(),
                    year: "1979".into(),
                    players: Range { min: None, max: Some(3) },
                    length: Range::default()
                },
                readme: "".into(),
                image: None,
                owners: vec!["alice".into(), "bob".into()],
                packages: vec![
                    PackageData {
                        name: "a_package".into(),
                        slug: "a_package".into(),
                        sort_key: 0,
                        description: "".into(),
                        releases: vec![
                            ReleaseData {
                                version: "1.2.4".into(),
                                files: vec![
                                    FileData {
                                        filename: "a_package-1.2.4".into(),
                                        url: "https://example.com/a_package-1.2.4".into(),
                                        size: 5678,
                                        sha256: "79fdd8fe3128f818e446e919cce5dcfb81815f8f4341c53f4d6b58ded48cebf2".into(),
                                        published_at: "2023-12-10T15:56:29.180282477Z".into(),
                                        published_by: "alice".into(),
                                        requires: Some(">= 3.7.12".into())
                                    },
                                ]
                            },
                            ReleaseData {
                                version: "1.2.3".into(),
                                files: vec![
                                    FileData {
                                        filename: "a_package-1.2.3".into(),
                                        url: "https://example.com/a_package-1.2.3".into(),
                                        size: 1234,
                                        sha256: "c0e0fa7373a12b45a91e4f4d4e2e186442fc6ee9b346caa2fdc1c09026a2144a".into(),
                                        published_at: "2023-12-09T15:56:29.180282477Z".into(),
                                        published_by: "bob".into(),
                                        requires: Some(">= 3.2.17".into())
                                    }
                                ]
                            }
                        ]
                    },
                    PackageData {
                        name: "b_package".into(),
                        slug: "b_package".into(),
                        sort_key: 1,
                        description: "".into(),
                        releases: vec![]
                    },
                    PackageData {
                        name: "c_package".into(),
                        slug: "c_package".into(),
                        sort_key: 2,
                        description: "".into(),
                        releases: vec![]
                    }
                ],
                gallery: vec![]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners", "packages"))]
    async fn get_project_revision_ok_old(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert_eq!(
            core.get_project_revision(Project(42), 1).await.unwrap(),
            ProjectData {
                name: "test_game".into(),
                slug: "test_game".into(),
                description: "Brian's Trademarked Game of Being a Test Case".into(),
                revision: 1,
                created_at: "2023-11-12T15:50:06.419538067Z".into(),
                modified_at: "2023-11-12T15:50:06.419538067Z".into(),
                tags: vec![],
                game: GameData {
                    title: "A Game of Tests".into(),
                    title_sort_key: "game of tests, a".into(),
                    publisher: "Test Game Company".into(),
                    year: "1978".into(),
                    players: Range { min: None, max: Some(3) },
                    length: Range::default()
                },
                readme: "".into(),
                image: None,
                owners: vec!["alice".into(), "bob".into()],
                packages: vec![
                    PackageData {
                        name: "b_package".into(),
                        slug: "b_package".into(),
                        sort_key: 1,
                        description: "".into(),
                        releases: vec![],
                    },
                    PackageData {
                        name: "c_package".into(),
                        slug: "c_package".into(),
                        sort_key: 2,
                        description: "".into(),
                        releases: vec![],
                    }
                ],
                gallery: vec![]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn create_project_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let user = User(1);
        let name = "newproj";
        let data = ProjectData {
            name: name.into(),
            slug: name.into(),
            description: "A New Game".into(),
            revision: 1,
            created_at: NOW.into(),
            modified_at: NOW.into(),
            tags: vec![],
            game: GameData {
                title: "Some New Game".into(),
                title_sort_key: "some new game".into(),
                publisher: "XYZ Games".into(),
                year: "1999".into(),
                players: Range::default(),
                length: Range::default()
            },
            readme: "".into(),
            image: None,
            owners: vec!["bob".into()],
            packages: vec![],
            gallery: vec![]
        };

        let cdata = ProjectDataPost {
            name: name.into(),
            description: data.description.clone(),
            tags: vec![],
            game: GameDataPost {
                title: data.game.title.clone(),
                title_sort_key: data.game.title_sort_key.clone(),
                publisher: data.game.publisher.clone(),
                year: data.game.year.clone(),
                players: RangePost::default(),
                length: RangePost::default()
            },
            readme: "".into(),
            image: None
        };

        core.create_project(user, name, &cdata).await.unwrap();
        let proj = core.get_project_id(name).await.unwrap();
        assert_eq!(core.get_project(proj).await.unwrap(), data);
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn create_project_bad_name(pool: Pool) {
        let core = make_core(pool, fake_now);

        let user = User(1);
        let name = "  -  bad  ";
        let data = ProjectData {
            name: name.into(),
            slug: name.into(),
            description: "A New Game".into(),
            revision: 1,
            created_at: NOW.into(),
            modified_at: NOW.into(),
            tags: vec![],
            game: GameData {
                title: "Some New Game".into(),
                title_sort_key: "some new game".into(),
                publisher: "XYZ Games".into(),
                year: "1999".into(),
                players: Range::default(),
                length: Range::default()
            },
            readme: "".into(),
            image: None,
            owners: vec!["bob".into()],
            packages: vec![],
            gallery: vec![]
        };

        let cdata = ProjectDataPost {
            name: name.into(),
            description: data.description.clone(),
            tags: vec![],
            game: GameDataPost {
                title: data.game.title.clone(),
                title_sort_key: data.game.title_sort_key.clone(),
                publisher: data.game.publisher.clone(),
                year: data.game.year.clone(),
                players: RangePost::default(),
                length: RangePost::default()
            },
            readme: "".into(),
            image: None
        };

        assert_eq!(
            core.create_project(user, name, &cdata).await.unwrap_err(),
            CreateProjectError::InvalidProjectName
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn update_project_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let name = "test_game";
        let new_data = ProjectData {
            name: name.into(),
            slug: name.into(),
            description: "new description".into(),
            revision: 4,
            created_at: "2023-11-12T15:50:06.419538067Z".into(),
            modified_at: NOW.into(),
            tags: vec![],
            game: GameData {
                title: "Some New Game".into(),
                title_sort_key: "some new game".into(),
                publisher: "XYZ Games".into(),
                year: "1999".into(),
                players: Range { min: None, max: Some(3) },
                length: Range::default(),
            },
            readme: "".into(),
            image: None,
            owners: vec!["bob".into()],
            packages: vec![],
            gallery: vec![]
        };

        let cdata = ProjectDataPatch {
            description: Some(new_data.description.clone()),
            tags_add: None,
            tags_remove: None,
            game: GameDataPatch {
                title: Some(new_data.game.title.clone()),
                title_sort_key: Some(new_data.game.title_sort_key.clone()),
                publisher: Some(new_data.game.publisher.clone()),
                year: Some(new_data.game.year.clone()),
                players: RangePatch::default(),
                length: RangePatch::default()
            },
            readme: Some("".into()),
            image: None
        };

        let proj = core.get_project_id(name).await.unwrap();
        let old_data = core.get_project(proj).await.unwrap();
        core.update_project(Owner(1), Project(42), &cdata).await.unwrap();
        // project has new data
        assert_eq!(core.get_project(proj).await.unwrap(), new_data);
        // old data is kept as a revision
        assert_eq!(
            core.get_project_revision(proj, 3).await.unwrap(),
            old_data
        );
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn create_package_ok(pool: Pool) {
        let core = make_core(pool, fake_now);

        let owner = Owner(1);
        let proj = Project(42);
        let name = "Good Package Name";
        let slug = "Good-Package-Name";
        let data = PackageData {
            name: name.into(),
            slug: slug.into(),
            sort_key: 1,
            description: "".into(),
            releases: vec![]
        };

        let cdata = PackageDataPost {
            name: data.name.clone(),
            description: data.description.clone(),
            sort_key: -1
        };

        core.create_package(owner, proj, slug, &cdata).await.unwrap();
        // package was created if this succeeds
        core.get_package_id(proj, slug).await.unwrap();
    }

    #[sqlx::test(fixtures("users", "projects", "packages"))]
    async fn create_package_bad_name(pool: Pool) {
        let core = make_core(pool, fake_now);

        let owner = Owner(1);
        let proj = Project(42);
        let name = "  -  bad  ";
        let data = PackageData {
            name: name.into(),
            slug: name.into(),
            sort_key: 1,
            description: "".into(),
            releases: vec![]
        };

        let cdata = PackageDataPost {
            name: data.name,
            description: data.description.clone(),
            sort_key: 1
        };

        assert_eq!(
            core.create_package(owner, proj, name, &cdata).await.unwrap_err(),
            CreatePackageError::InvalidPackageName
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn get_owners_ok(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert_eq!(
            core.get_owners(Project(42)).await.unwrap(),
            Users { users: vec!["bob".into()] }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn user_is_owner_true(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert!(core.user_is_owner(User(1), Project(42)).await.unwrap());
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn user_is_owner_false(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert!(!core.user_is_owner(User(2), Project(42)).await.unwrap());
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn add_owners_ok(pool: Pool) {
        let core = make_core(pool, fake_now);
        let users = Users { users: vec!["alice".into()] };
        core.add_owners(&users, Project(42)).await.unwrap();
        assert_eq!(
            core.get_owners(Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into()
                ]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "two_owners"))]
    async fn remove_owners_ok(pool: Pool) {
        let core = make_core(pool, fake_now);
        let users = Users { users: vec!["bob".into()] };
        core.remove_owners(&users, Project(42)).await.unwrap();
        assert_eq!(
            core.get_owners(Project(42)).await.unwrap(),
            Users { users: vec!["alice".into()] }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "one_owner"))]
    async fn remove_owners_fail_if_last(pool: Pool) {
        let core = make_core(pool, fake_now);
        let users = Users { users: vec!["bob".into()] };
        assert_eq!(
            core.remove_owners(&users, Project(1)).await.unwrap_err(),
            RemoveOwnersError::CannotRemoveLastOwner
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn get_players_ok(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert_eq!(
            core.get_players(Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into()
                ]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn add_player_ok(pool: Pool) {
        let core = make_core(pool, fake_now);
        core.add_player(User(3), Project(42)).await.unwrap();
        assert_eq!(
            core.get_players(Project(42)).await.unwrap(),
            Users {
                users: vec![
                    "alice".into(),
                    "bob".into(),
                    "chuck".into()
                ]
            }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "players"))]
    async fn remove_player_ok(pool: Pool) {
        let core = make_core(pool, fake_now);
        core.remove_player(User(1), Project(42)).await.unwrap();
        assert_eq!(
            core.get_players(Project(42)).await.unwrap(),
            Users { users: vec!["alice".into()] }
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn get_image_ok(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert_eq!(
            core.get_image(Project(42), "img.png").await.unwrap(),
            "https://example.com/images/img.png"
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn get_image_not_a_project(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert_eq!(
            core.get_image(Project(1), "img.png").await.unwrap_err(),
            GetImageError::NotFound
        );
    }

    #[sqlx::test(fixtures("users", "projects", "images"))]
    async fn get_image_not_an_image(pool: Pool) {
        let core = make_core(pool, fake_now);
        assert_eq!(
            core.get_image(Project(42), "bogus").await.unwrap_err(),
            GetImageError::NotFound
        );
    }

    #[test]
    fn test_split_title_sort_key() {
        assert_eq!(split_title_sort_key(""), ("", None));
        assert_eq!(split_title_sort_key("a game"), ("game", Some("a")));
        assert_eq!(split_title_sort_key("an apron"), ("apron", Some("an")));
        assert_eq!(split_title_sort_key("the game"), ("game", Some("the")));
        assert_eq!(split_title_sort_key("some game"), ("some game", None));
        assert_eq!(split_title_sort_key("a la jeu"), ("a la jeu", None));
        assert_eq!(split_title_sort_key("a las una"), ("a las una", None));
        assert_eq!(split_title_sort_key("a last"), ("last", Some("a")));
    }

    #[test]
    fn test_normalize_title_sort_key() {
        assert_eq!(normalize_title_sort_key("no accents"), "no accents");
        assert_eq!(normalize_title_sort_key("Fureur à l'Est"), "fureur a l'est");
        assert_eq!(normalize_title_sort_key("'!(34!"), "34!");
    }

    #[test]
    fn test_title_sort_key() {
        assert_eq!(title_sort_key(""), "");
        assert_eq!(title_sort_key("A Game"), "game, a");
        assert_eq!(title_sort_key("An Apron"), "apron, an");
        assert_eq!(title_sort_key("The Game"), "game, the");
        assert_eq!(title_sort_key("Some Game"), "some game");
        assert_eq!(title_sort_key("A la Jeu"), "a la jeu");
        assert_eq!(title_sort_key("A las Una"), "a las una");
        assert_eq!(title_sort_key("A Last"), "last, a");
    }

    #[test]
    fn test_is_valid_project_name() {
        assert!(!is_valid_project_name(""));
        assert!(!is_valid_project_name("abcd"));
        assert!(is_valid_project_name("abcde"));
        assert!(is_valid_project_name("ab de"));
        assert!(!is_valid_project_name(&"x".repeat(65)));
        assert!(!is_valid_project_name(" x "));
        assert!(!is_valid_project_name("x  x"));
        assert!(!is_valid_project_name("x💩x"));
    }
}
