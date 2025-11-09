use glc::{
    discourse::UserUpdateParams,
    model::{FlagTag, GalleryImage, Users},
    pagination::{Direction, Facet, SortBy}
};
use serde::Deserialize;
use std::{
    future::Future,
    mem
};
use sqlx::FromRow;
use thiserror::Error;

use crate::{
    input::{FlagPost, GalleryPatch, PackageDataPatch, PackageDataPost, ProjectDataPatch, ProjectDataPost},
    model::{Admin, Flag, Owner, Package, Project, Release, User},
    version::Version
};

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("Cannot remove last owner")]
    CannotRemoveLastOwner,
    #[error("Already exists")]
    AlreadyExists,
    #[error("Not found")]
    NotFound,
    #[error("Not empty")]
    NotEmpty,
    #[error("Invalid sort key")]
    InvalidSortKey,
    #[error("{0}")]
    SqlxError(#[from] sqlx::Error)
}

impl PartialEq for DatabaseError {
    fn eq(&self, other: &Self) -> bool {
        // sqlx::Error is not PartialEq, so we must exclude it
        match (self, other) {
            (Self::SqlxError(_), _) |
            (_, Self::SqlxError(_)) => false,
            (_, _) => mem::discriminant(self) == mem::discriminant(other)
        }
    }
}

pub fn map_unique(e: sqlx::Error) -> DatabaseError {
    match e {
        sqlx::Error::Database(e) if e.is_unique_violation() => DatabaseError::AlreadyExists,
        e => DatabaseError::SqlxError(e)
    }
}

#[derive(Debug, Deserialize, FromRow, PartialEq)]
pub struct ProjectSummaryRow {
    pub rank: f64,
    pub project_id: i64,
    pub name: String,
    pub slug: String,
    pub description: String,
    pub revision: i64,
    pub created_at: i64,
    pub modified_at: i64,
    pub game_title: String,
    pub game_title_sort: String,
    pub game_publisher: String,
    pub game_year: String,
    pub game_players_min: Option<i64>,
    pub game_players_max: Option<i64>,
    pub game_length_min: Option<i64>,
    pub game_length_max: Option<i64>,
    pub image: Option<String>
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ProjectRow {
    pub project_id: i64,
    pub name: String,
    pub slug: String,
    pub description: String,
    pub revision: i64,
    pub created_at: i64,
    pub modified_at: i64,
    pub modified_by: i64,
    pub game_title: String,
    pub game_title_sort: String,
    pub game_publisher: String,
    pub game_year: String,
    pub game_players_min: Option<i64>,
    pub game_players_max: Option<i64>,
    pub game_length_min: Option<i64>,
    pub game_length_max: Option<i64>,
    pub image: Option<String>,
    pub readme: String
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PackageRow {
    pub package_id: i64,
    pub name: String,
    pub slug: String,
    pub sort_key: i64,
    pub created_at: i64
//    description: String
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ReleaseRow {
    pub release_id: i64,
    pub version: String,
    pub version_major: i64,
    pub version_minor: i64,
    pub version_patch: i64,
    pub version_pre: String,
    pub version_build: String,
    pub published_at: i64,
    pub published_by: String
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct FileRow {
    pub id: i64,
    pub filename: String,
    pub url: String,
    pub size: i64,
    pub sha256: String,
    pub requires: Option<String>,
    pub published_at: i64,
    pub published_by: String
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct FlagRow {
    pub flag_id: i64,
    pub project: String,
    pub slug: String,
    pub flag: FlagTag,
    pub flagged_at: i64,
    pub flagged_by: String,
    pub message: Option<String>
}

pub enum MidField<'a> {
    Timestamp(i64),
    Weight(f64),
    Text(&'a str)
}

pub trait DatabaseClient {
    fn get_project_id(
        &self,
        _projname: &str
    ) -> impl Future<Output = Result<Option<Project>, DatabaseError>> + Send;

    fn get_projects_count(
        &self,
        _facets: &[Facet]
    ) -> impl Future<Output = Result<i64, DatabaseError>> + Send;

   fn get_user_id(
        &self,
        _username: &str
    ) -> impl Future<Output = Result<Option<User>, DatabaseError>> + Send;

    fn update_user(
        &self,
        _params: &UserUpdateParams
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn get_owners(
        &self,
        _proj: Project
    ) -> impl Future<Output = Result<Users, DatabaseError>> + Send;

    fn user_is_owner(
        &self,
        _user: User,
        _proj: Project
    ) -> impl Future<Output = Result<bool, DatabaseError>> + Send;

    fn add_owner(
        &self,
        _user: User,
        _proj: Project
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn add_owners(
        &self,
        _owners: &Users,
        _proj: Project
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn remove_owner(
        &self,
        _user: User,
        _proj: Project
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn remove_owners(
        &self,
        _owners: &Users,
        _proj: Project
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn has_owner(
        &self,
        _proj: Project
    ) -> impl Future<Output = Result<bool, DatabaseError>> + Send;

    fn get_projects_end_window(
        &self,
        _facets: &[Facet],
        _sort_by: SortBy,
        _dir: Direction,
        _limit: u32
    ) -> impl Future<Output = Result<Vec<ProjectSummaryRow>, DatabaseError>> + Send;

    fn get_projects_mid_window(
        &self,
        _facets: &[Facet],
        _sort_by: SortBy,
        _dir: Direction,
        _field: MidField<'_>,
        _id: u32,
        _limit: u32
    ) -> impl Future<Output = Result<Vec<ProjectSummaryRow>, DatabaseError>> + Send;

    fn create_project(
        &self,
        _user: User,
        _proj: &str,
        _proj_data: &ProjectDataPost,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn update_project(
        &self,
        _owner: Owner,
        _proj: Project,
        _proj_data: &ProjectDataPatch,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn get_project_row(
        &self,
        proj: Project
    ) -> impl Future<Output = Result<ProjectRow, DatabaseError>> + Send;

    fn get_project_row_revision(
        &self,
        _proj: Project,
        _revision: i64
    ) -> impl Future<Output = Result<ProjectRow, DatabaseError>> + Send;

    fn get_packages(
        &self,
        _proj: Project
    ) -> impl Future<Output = Result<Vec<PackageRow>, DatabaseError>> + Send;

    fn get_packages_at(
        &self,
        _proj: Project,
        _date: i64,
    ) -> impl Future<Output = Result<Vec<PackageRow>, DatabaseError>> + Send;

    fn get_package_id(
        &self,
        _proj: Project,
        _pkg: &str
    ) -> impl Future<Output = Result<Option<Package>, DatabaseError>> + Send;

    fn get_project_package_ids(
         &self,
        _proj: &str,
        _pkg: &str
    ) -> impl Future<Output = Result<Option<(Project, Package)>, DatabaseError>> + Send;

    fn create_package(
        &self,
        _owner: Owner,
        _proj: Project,
        _pkg: &str,
        _pkg_data: &PackageDataPost,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn delete_package(
        &self,
        _owner: Owner,
        _proj: Project,
        _pkg: Package,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn update_package(
        &self,
        _owner: Owner,
        _proj: Project,
        _pkg: Package,
        _pkg_data: &PackageDataPatch,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn get_releases(
        &self,
        _pkg: Package
    ) -> impl Future<Output = Result<Vec<ReleaseRow>, DatabaseError>> + Send;

    fn get_releases_at(
        &self,
        _pkg: Package,
        _date: i64
    ) -> impl Future<Output = Result<Vec<ReleaseRow>, DatabaseError>> + Send;

    fn get_release_id(
        &self,
        _proj: Project,
        _pkg: Package,
        _release: &str
    ) -> impl Future<Output = Result<Option<Release>, DatabaseError>> + Send;

    fn get_project_package_release_ids(
         &self,
        _proj: &str,
        _pkg: &str,
        _release: &str
    ) -> impl Future<
        Output = Result<Option<(Project, Package, Release)>, DatabaseError>
    > + Send;

    fn create_release(
        &self,
        _owner: Owner,
        _proj: Project,
        _pkg: Package,
        _version: &Version,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn delete_release(
        &self,
        _owner: Owner,
        _proj: Project,
        _rel: Release,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn get_release_version(
        &self,
        _release: Release
    ) -> impl Future<Output = Result<Version, DatabaseError>> + Send;

    fn get_files(
        &self,
        _release: Release
    ) -> impl Future<Output = Result<Vec<FileRow>, DatabaseError>> + Send;

    fn get_files_at(
        &self,
        _release: Release,
        _date: i64
    ) -> impl Future<Output = Result<Vec<FileRow>, DatabaseError>> + Send;

    #[allow(clippy::too_many_arguments)]
    fn add_file_url(
        &self,
        _owner: Owner,
        _proj: Project,
        _release: Release,
        _filename: &str,
        _size: i64,
        _sha256: &str,
        _content_type: &str,
        _requires: Option<&str>,
        _url: &str,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn get_players(
        &self,
        _proj: Project
    ) -> impl Future<Output = Result<Users, DatabaseError>> + Send;

    fn add_player(
        &self,
        _player: User,
        _proj: Project
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn remove_player(
        &self,
        _player: User,
        _proj: Project
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn get_image_url(
        &self,
        _proj: Project,
        _img_name: &str
    ) -> impl Future<Output = Result<Option<String>, DatabaseError>> + Send;

    fn get_image_url_at(
        &self,
        _proj: Project,
        _img_name: &str,
        _date: i64
    ) -> impl Future<Output = Result<Option<String>, DatabaseError>> + Send;

    fn add_image_url(
        &self,
        _owner: Owner,
        _proj: Project,
        _img_name: &str,
        _url: &str,
        _content_type: &str,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn get_project_tags(
        &self,
        _proj: Project
    ) -> impl Future<Output = Result<Vec<String>, DatabaseError>> + Send;

    fn get_project_tags_at(
        &self,
        _proj: Project,
        _date: i64
    ) -> impl Future<Output = Result<Vec<String>, DatabaseError>> + Send;

    fn get_gallery(
        &self,
        _proj: Project
    ) -> impl Future<Output = Result<Vec<GalleryImage>, DatabaseError>> + Send;

    fn get_gallery_at(
        &self,
        _proj: Project,
        _date: i64
    ) -> impl Future<Output = Result<Vec<GalleryImage>, DatabaseError>> + Send;

    fn add_gallery_image(
        &self,
        _owner: Owner,
        _proj: Project,
        _img_name: &str,
        _url: &str,
        _content_type: &str,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn update_gallery(
        &self,
        _owner: Owner,
        _proj: Project,
        _gallery_patch: &GalleryPatch,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn get_publishers(
        &self
    ) -> impl Future<Output = Result<Vec<String>, DatabaseError>> + Send;

    fn get_tags(
        &self
    ) -> impl Future<Output = Result<Vec<String>, DatabaseError>> + Send;

    fn get_flag_id(
        &self,
        _flag: i64
    ) -> impl Future<Output = Result<Option<Flag>, DatabaseError>> + Send;

    fn add_flag(
        &self,
        _reporter: User,
        _proj: Project,
        _flag: &FlagPost,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn close_flag(
        &self,
        _admin: Admin,
        _flag: Flag,
        _now: i64
    ) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn get_flags(
        &self
    ) -> impl Future<Output = Result<Vec<FlagRow>, DatabaseError>> + Send;
}
