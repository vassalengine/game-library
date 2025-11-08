use async_trait::async_trait;
use axum::body::Bytes;
use futures::Stream;
use glc::model::{Flags, Projects, ProjectData, Publishers, Tags, Users};
use mime::Mime;
use std::{
    io,
    sync::Arc
};
use thiserror::Error;

use crate::{
    db,
    input::{FlagPost, GalleryPatch, PackageDataPatch, PackageDataPost, ProjectDataPatch, ProjectDataPost},
    model::{Admin, Flag, Owner, Package, Project, Release, User},
    module,
    params::ProjectsParams,
    time,
    upload,
    version::{self, Version}
};

#[derive(Debug, Error, PartialEq)]
pub enum GetIdError {
    #[error("Not found")]
    NotFound,
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error, PartialEq)]
pub enum UserIsOwnerError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error, PartialEq)]
pub enum GetOwnersError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error, PartialEq)]
pub enum AddOwnersError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveOwnersError {
    #[error("Cannot remove last owner")]
    CannotRemoveLastOwner,
    #[error("{0}")]
    DatabaseError(db::DatabaseError)
}

impl From<db::DatabaseError> for RemoveOwnersError {
    fn from(err: db::DatabaseError) -> Self {
        match err {
            db::DatabaseError::CannotRemoveLastOwner => RemoveOwnersError::CannotRemoveLastOwner,
            e => RemoveOwnersError::DatabaseError(e)
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GetProjectsError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("Malformed query")]
    MalformedQuery,
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

#[derive(Debug, Error, PartialEq)]
pub enum GetProjectError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("Not found")]
    NotFound,
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateProjectError {
    #[error("Already exists")]
    AlreadyExists,
    #[error("{0}")]
    DatabaseError(db::DatabaseError),
    #[error("Invalid project name")]
    InvalidProjectName,
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

impl From<db::DatabaseError> for CreateProjectError {
    fn from(err: db::DatabaseError) -> Self {
        match err {
            db::DatabaseError::AlreadyExists => CreateProjectError::AlreadyExists,
            e => CreateProjectError::DatabaseError(e)
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateProjectError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

#[derive(Debug, Error, PartialEq)]
pub enum CreatePackageError {
    #[error("Already exists")]
    AlreadyExists,
    #[error("{0}")]
    DatabaseError(db::DatabaseError),
    #[error("Invalid package name")]
    InvalidPackageName,
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

impl From<db::DatabaseError> for CreatePackageError {
    fn from(err: db::DatabaseError) -> Self {
        match err {
            db::DatabaseError::AlreadyExists => CreatePackageError::AlreadyExists,
            e => CreatePackageError::DatabaseError(e)
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdatePackageError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("Invalid package name")]
    InvalidPackageName,
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

#[derive(Debug, Error, PartialEq)]
pub enum DeletePackageError {
    #[error("{0}")]
    DatabaseError(db::DatabaseError),
    #[error("Not empty")]
    NotEmpty,
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

impl From<db::DatabaseError> for DeletePackageError {
    fn from(err: db::DatabaseError) -> Self {
        match err {
            db::DatabaseError::NotEmpty => DeletePackageError::NotEmpty,
            e => DeletePackageError::DatabaseError(e)
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateReleaseError {
    #[error("Already exists")]
    AlreadyExists,
    #[error("{0}")]
    DatabaseError(db::DatabaseError),
    #[error("{} is not a valid version", .0.0)]
    InvalidVersion(#[from] version::MalformedVersion),
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

impl From<db::DatabaseError> for CreateReleaseError {
    fn from(err: db::DatabaseError) -> Self {
        match err {
            db::DatabaseError::AlreadyExists => CreateReleaseError::AlreadyExists,
            e => CreateReleaseError::DatabaseError(e)
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteReleaseError {
    #[error("{0}")]
    DatabaseError(db::DatabaseError),
    #[error("Not empty")]
    NotEmpty,
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

impl From<db::DatabaseError> for DeleteReleaseError {
    fn from(err: db::DatabaseError) -> Self {
        match err {
            db::DatabaseError::NotEmpty => DeleteReleaseError::NotEmpty,
            e => DeleteReleaseError::DatabaseError(e)
        }
    }
}

#[derive(Debug, Error)]
pub enum AddFileError {
    #[error("Bad MIME type")]
    BadMimeType,
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("Invalid filename")]
    InvalidFilename,
    #[error("{0}")]
    IOError(#[from] io::Error),
    #[error("{0}")]
    ModuleError(#[from] module::Error),
    #[error("{0}")]
    MalformedVersion(#[from] version::MalformedVersion),
    #[error("Module version {0} != release version {1}")]
    ReleaseVersionMismatch(Version, Version),
    #[error("{0}")]
    TimeError(#[from] time::Error),
    #[error("File too large")]
    TooLarge,
    #[error("{0}")]
    UploadError(#[from] upload::UploadError)
}

impl PartialEq for AddFileError {
    fn eq(&self, other: &Self) -> bool {
        // io::Error is not PartialEq, so we must exclude it
        match (self, other) {
            (Self::DatabaseError(l), Self::DatabaseError(r)) => l == r,
            (Self::MalformedVersion(l), Self::MalformedVersion(r)) => l == r,
            (Self::ReleaseVersionMismatch(l0, l1), Self::ReleaseVersionMismatch(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::ModuleError(l), Self::ModuleError(r)) => l == r,
            (Self::TimeError(l), Self::TimeError(r)) => l == r,
            (Self::UploadError(l), Self::UploadError(r)) => l == r,
            (Self::BadMimeType, Self::BadMimeType) |
            (Self::InvalidFilename, Self::InvalidFilename) |
            (Self::TooLarge, Self::TooLarge) => true,
            (_, _) => false
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GetPlayersError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error, PartialEq)]
pub enum AddPlayerError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error, PartialEq)]
pub enum RemovePlayerError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateGalleryError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("{0}")]
    TimeError(#[from] time::Error),
}

#[derive(Debug, Error, PartialEq)]
pub enum GetImageError {
    #[error("Not found")]
    NotFound,
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error)]
pub enum AddImageError {
    #[error("Unsupported media type")]
    BadMimeType,
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("Invalid filename")]
    InvalidFilename,
    #[error("{0}")]
    IOError(#[from] io::Error),
    #[error("{0}")]
    TimeError(#[from] time::Error),
    #[error("File too large")]
    TooLarge,
    #[error("{0}")]
    UploadError(#[from] upload::UploadError)
}

impl PartialEq for AddImageError {
    fn eq(&self, other: &Self) -> bool {
        // io::Error is not PartialEq, so we must exclude it
        match (self, other) {
            (Self::DatabaseError(l), Self::DatabaseError(r)) => l == r,
            (Self::TimeError(l), Self::TimeError(r)) => l == r,
            (Self::UploadError(l), Self::UploadError(r)) => l == r,
            (Self::BadMimeType, Self::BadMimeType) |
            (Self::InvalidFilename, Self::InvalidFilename) |
            (Self::TooLarge, Self::TooLarge) => true,
            (_, _) => false
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GetPublishersError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error, PartialEq)]
pub enum GetTagsError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError)
}

#[derive(Debug, Error, PartialEq)]
pub enum AddFlagError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

#[derive(Debug, Error, PartialEq)]
pub enum CloseFlagError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

#[derive(Debug, Error, PartialEq)]
pub enum GetFlagsError {
    #[error("{0}")]
    DatabaseError(#[from] db::DatabaseError),
    #[error("{0}")]
    TimeError(#[from] time::Error)
}

#[async_trait]
pub trait Core {
    fn max_file_size(&self) -> usize {
        unimplemented!();
    }

    fn max_image_size(&self) -> usize {
        unimplemented!();
    }

    async fn get_project_id(
         &self,
        _proj: &str
    ) -> Result<Project, GetIdError>
    {
        unimplemented!();
    }

    async fn get_package_id(
         &self,
        _proj: Project,
        _pkg: &str
    ) -> Result<Package, GetIdError>
    {
        unimplemented!();
    }

    async fn get_project_package_ids(
         &self,
        _proj: &str,
        _pkg: &str
    ) -> Result<(Project, Package), GetIdError>
    {
        unimplemented!();
    }

    async fn get_release_id(
         &self,
        _proj: Project,
        _pkg: Package,
        _release: &str
    ) -> Result<Release, GetIdError>
    {
        unimplemented!();
    }

    async fn get_project_package_release_ids(
        &self,
        _proj: &str,
        _pkg: &str,
        _release: &str
    ) -> Result<(Project, Package, Release), GetIdError>
    {
        unimplemented!();
    }

    async fn get_user_id(
         &self,
        _username: &str
    ) -> Result<User, GetIdError>
    {
        unimplemented!();
    }

    async fn get_owners(
        &self,
        _proj: Project
    ) -> Result<Users, GetOwnersError>
    {
        unimplemented!();
    }

    async fn add_owners(
        &self,
        _owners: &Users,
        _proj: Project
    ) -> Result<(), AddOwnersError>
    {
        unimplemented!();
    }

    async fn remove_owners(
        &self,
        _owners: &Users,
        _proj: Project
    ) -> Result<(), RemoveOwnersError>
    {
        unimplemented!();
    }

    async fn user_is_owner(
        &self,
        _user: User,
        _proj: Project
    ) -> Result<bool, UserIsOwnerError>
    {
        unimplemented!();
    }

    async fn get_projects(
        &self,
        _params: ProjectsParams
    ) -> Result<Projects, GetProjectsError>
    {
        unimplemented!();
    }

    async fn get_project(
        &self,
        _proj: Project
    ) -> Result<ProjectData, GetProjectError>
    {
        unimplemented!();
    }

    async fn create_project(
        &self,
        _user: User,
        _proj: &str,
        _proj_data: &ProjectDataPost
    ) -> Result<(), CreateProjectError>
    {
        unimplemented!();
    }

    async fn update_project(
        &self,
        _owner: Owner,
        _proj: Project,
        _proj_data: &ProjectDataPatch
    ) -> Result<(), UpdateProjectError>
    {
        unimplemented!();
    }

    async fn get_project_revision(
        &self,
        _proj: Project,
        _revision: i64
    ) -> Result<ProjectData, GetProjectError>
    {
        unimplemented!();
    }

    async fn create_package(
        &self,
        _owner: Owner,
        _proj: Project,
        _pkg: &str,
        _pkg_data: &PackageDataPost
    ) -> Result<(), CreatePackageError>
    {
        unimplemented!();
    }

    async fn update_package(
        &self,
        _owner: Owner,
        _proj: Project,
        _pkg: Package,
        _pkg_data: &PackageDataPatch
    ) -> Result<(), UpdatePackageError>
    {
        unimplemented!();
    }

    async fn delete_package(
        &self,
        _owner: Owner,
        _proj: Project,
        _pkg: Package,
    ) -> Result<(), DeletePackageError>
    {
        unimplemented!();
    }

    async fn create_release(
        &self,
        _owner: Owner,
        _proj: Project,
        _pkg: Package,
        _version: &str
    ) -> Result<(), CreateReleaseError>
    {
        unimplemented!();
    }

    async fn delete_release(
        &self,
        _owner: Owner,
        _proj: Project,
        _rel: Release
    ) -> Result<(), DeleteReleaseError>
    {
        unimplemented!();
    }

    async fn add_file(
        &self,
        _owner: Owner,
        _proj: Project,
        _release: Release,
        _filename: &str,
        _content_type: Option<&Mime>,
        _content_length: Option<u64>,
        _stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
    ) -> Result<(), AddFileError>
    {
        unimplemented!();
    }

    async fn get_players(
        &self,
        _proj: Project
    ) -> Result<Users, GetPlayersError>
    {
        unimplemented!();
    }

    async fn add_player(
        &self,
        _player: User,
        _proj: Project
    ) -> Result<(), AddPlayerError>
    {
        unimplemented!();
    }

    async fn remove_player(
        &self,
        _player: User,
        _proj: Project
    ) -> Result<(), RemovePlayerError>
    {
        unimplemented!();
    }

    async fn add_gallery_image(
        &self,
        _owner: Owner,
        _proj: Project,
        _img_name: &str,
        _content_type: &Mime,
        _content_length: Option<u64>,
        _stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
    ) -> Result<(), AddImageError>
    {
        unimplemented!();
    }

    async fn update_gallery(
        &self,
        _owner: Owner,
        _proj: Project,
        _gallery_patch: &GalleryPatch
    ) -> Result<(), UpdateGalleryError>
    {
        unimplemented!();
    }

    async fn get_image(
        &self,
        _proj: Project,
        _img_name: &str
    ) -> Result<String, GetImageError>
    {
        unimplemented!();
    }

    async fn get_image_revision(
        &self,
        _proj: Project,
        _revision: i64,
        _img_name: &str
    ) -> Result<String, GetImageError>
    {
        unimplemented!();
    }

    async fn add_image(
        &self,
        _owner: Owner,
        _proj: Project,
        _img_name: &str,
        _content_type: &Mime,
        _content_length: Option<u64>,
        _stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin>
    ) -> Result<(), AddImageError>
    {
        unimplemented!();
    }

    async fn get_publishers(
        &self
    ) -> Result<Publishers, GetPublishersError>
    {
        unimplemented!();
    }

    async fn get_tags(
        &self
    ) -> Result<Tags, GetTagsError>
    {
        unimplemented!();
    }

    async fn get_flag_id(
         &self,
        _flag: i64
    ) -> Result<Flag, GetIdError>
    {
        unimplemented!();
    }

    async fn add_flag(
        &self,
        _reporter: User,
        _proj: Project,
        _flag: &FlagPost
    ) -> Result<(), AddFlagError>
    {
        unimplemented!();
    }

    async fn close_flag(
        &self,
        _admin: Admin,
        _flag: Flag
    ) -> Result<(), CloseFlagError>
    {
        unimplemented!();
    }

    async fn get_flags(
        &self
    ) -> Result<Flags, GetFlagsError>
    {
        unimplemented!();
    }
}

pub type CoreArc = Arc<dyn Core + Send + Sync>;
