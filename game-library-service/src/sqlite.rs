use glc::{
    discourse::UserUpdateParams,
    model::{GalleryImage, Users},
    pagination::{Direction, Facet, SortBy}
};
use sqlx::{
    Database,
    sqlite::{Sqlite, SqliteQueryResult}
};

mod flag;
mod images;
mod packages;
mod players;
mod project;
mod projects;
mod publishers;
mod releases;
mod tags;
mod users;

use crate::{
    db::{DatabaseClient, DatabaseError, FileRow, FlagRow, MidField, PackageRow, ProjectRow, ProjectSummaryRow, ReleaseRow},
    input::{FlagPost, GalleryPatch, PackageDataPatch, PackageDataPost, ProjectDataPatch, ProjectDataPost},
    model::{Admin, Flag, Owner, Package, Project, Release, User},
    version::Version
};

pub type Pool = sqlx::Pool<Sqlite>;

#[derive(Clone)]
pub struct SqlxDatabaseClient<DB: Database>(pub sqlx::Pool<DB>);

impl DatabaseClient for SqlxDatabaseClient<Sqlite> {
    async fn get_project_id(
        &self,
        projname: &str
    ) -> Result<Option<Project>, DatabaseError>
    {
        project::get_project_id(&self.0, projname).await
    }

    async fn get_projects_count(
        &self,
        facets: &[Facet]
    ) -> Result<i64, DatabaseError>
    {
        projects::get_projects_count(&self.0, facets).await
    }

    async fn get_user_id(
        &self,
        username: &str
    ) -> Result<Option<User>, DatabaseError>
    {
        users::get_user_id(&self.0, username).await
    }

    async fn update_user(
        &self,
        params: &UserUpdateParams
    ) -> Result<(), DatabaseError> {
        users::update_user(&self.0, params).await
    }

    async fn get_owners(
        &self,
        proj: Project
    ) -> Result<Users, DatabaseError>
    {
        users::get_owners(&self.0, proj).await
    }

    async fn user_is_owner(
        &self,
        user: User,
        proj: Project
    ) -> Result<bool, DatabaseError>
    {
        users::user_is_owner(&self.0, user, proj).await
    }

    async fn add_owner(
        &self,
        user: User,
        proj: Project
    ) -> Result<(), DatabaseError>
    {
        users::add_owner(&self.0, user, proj).await
    }

    async fn add_owners(
        &self,
        owners: &Users,
        proj: Project
    ) -> Result<(), DatabaseError>
    {
        users::add_owners(&self.0, owners, proj).await
    }

    async fn remove_owner(
        &self,
        user: User,
        proj: Project
    ) -> Result<(), DatabaseError>
    {
        users::remove_owner(&self.0, user, proj).await
    }

    async fn remove_owners(
        &self,
        owners: &Users,
        proj: Project
    ) -> Result<(), DatabaseError>
    {
        users::remove_owners(&self.0, owners, proj).await
    }

    async fn has_owner(
        &self,
        proj: Project
    ) -> Result<bool, DatabaseError>
    {
        users::has_owner(&self.0, proj).await
    }

    async fn get_projects_end_window(
        &self,
        facets: &[Facet],
        sort_by: SortBy,
        dir: Direction,
        limit: u32
    ) -> Result<Vec<ProjectSummaryRow>, DatabaseError>
    {
        projects::get_projects_end_window(&self.0, facets, sort_by, dir, limit).await
    }

    async fn get_projects_mid_window(
        &self,
        facets: &[Facet],
        sort_by: SortBy,
        dir: Direction,
        field: MidField<'_>,
        id: u32,
        limit: u32
    ) -> Result<Vec<ProjectSummaryRow>, DatabaseError>
    {
        match field {
            MidField::Timestamp(f) => projects::get_projects_mid_window(
                &self.0,
                facets,
                sort_by,
                dir,
                &f,
                id,
                limit
            ).await,
            MidField::Weight(f) => projects::get_projects_mid_window(
                &self.0,
                facets,
                sort_by,
                dir,
                &f,
                id,
                limit
            ).await,
            MidField::Text(f) => projects::get_projects_mid_window(
                &self.0,
                facets,
                sort_by,
                dir,
                &f,
                id,
                limit
            ).await
        }
    }

    async fn create_project(
        &self,
        user: User,
        proj: &str,
        proj_data: &ProjectDataPost,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        project::create_project(&self.0, user, proj, proj_data, now).await
    }

    async fn update_project(
        &self,
        owner: Owner,
        proj: Project,
        proj_data: &ProjectDataPatch,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        project::update_project(&self.0, owner, proj, proj_data, now).await
    }

    async fn get_project_row(
        &self,
        proj: Project
    ) -> Result<ProjectRow, DatabaseError>
    {
        project::get_project_row(&self.0, proj).await
    }

    async fn get_project_row_revision(
        &self,
        proj: Project,
        revision: i64
    ) -> Result<ProjectRow, DatabaseError>
    {
        project::get_project_row_revision(&self.0, proj, revision).await
    }

    async fn get_packages(
        &self,
        proj: Project
    ) -> Result<Vec<PackageRow>, DatabaseError>
    {
        packages::get_packages(&self.0, proj).await
    }

    async fn get_packages_at(
        &self,
        proj: Project,
        date: i64,
    ) -> Result<Vec<PackageRow>, DatabaseError>
    {
        packages::get_packages_at(&self.0, proj, date).await
    }

    async fn get_package_id(
        &self,
        proj: Project,
        pkg: &str
    ) -> Result<Option<Package>, DatabaseError>
    {
        packages::get_package_id(&self.0, proj, pkg).await
    }

    async fn get_project_package_ids(
        &self,
        proj: &str,
        pkg: &str
    ) -> Result<Option<(Project, Package)>, DatabaseError>
    {
        packages::get_project_package_ids(&self.0, proj, pkg).await
    }

    async fn create_package(
        &self,
        owner: Owner,
        proj: Project,
        pkg: &str,
        pkg_data: &PackageDataPost,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        packages::create_package(&self.0, owner, proj, pkg, pkg_data, now).await
    }

    async fn update_package(
        &self,
        owner: Owner,
        proj: Project,
        pkg: Package,
        pkg_data: &PackageDataPatch,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        packages::update_package(&self.0, owner, proj, pkg, pkg_data, now).await
    }

    async fn delete_package(
        &self,
        owner: Owner,
        proj: Project,
        pkg: Package,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        packages::delete_package(&self.0, owner, proj, pkg, now).await
    }

    async fn get_releases(
        &self,
        pkg: Package
    ) -> Result<Vec<ReleaseRow>, DatabaseError>
    {
        releases::get_releases(&self.0, pkg).await
    }

    async fn get_releases_at(
        &self,
        pkg: Package,
        date: i64
    ) -> Result<Vec<ReleaseRow>, DatabaseError>
    {
        releases::get_releases_at(&self.0, pkg, date).await
    }

    async fn get_release_id(
        &self,
        proj: Project,
        pkg: Package,
        release: &str
    ) -> Result<Option<Release>, DatabaseError>
    {
        releases::get_release_id(&self.0, proj, pkg, release).await
    }

    async fn get_project_package_release_ids(
        &self,
        projname: &str,
        pkgname: &str,
        release: &str
    ) -> Result<Option<(Project, Package, Release)>, DatabaseError> {
        releases::get_project_package_release_ids(
            &self.0,
            projname,
            pkgname,
            release
        ).await
    }

    async fn create_release(
        &self,
        owner: Owner,
        proj: Project,
        pkg: Package,
        version: &Version,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        releases::create_release(&self.0, owner, proj, pkg, version, now).await
    }

    async fn delete_release(
        &self,
        owner: Owner,
        proj: Project,
        rel: Release,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        releases::delete_release(&self.0, owner, proj, rel, now).await
    }

    async fn get_release_version(
        &self,
        rel: Release
    ) -> Result<Version, DatabaseError> {
        releases::get_release_version(&self.0, rel).await
    }

    async fn get_files(
        &self,
        rel: Release
    ) -> Result<Vec<FileRow>, DatabaseError>
    {
        releases::get_files(&self.0, rel).await
    }

    async fn get_files_at(
        &self,
        rel: Release,
        date: i64
    ) -> Result<Vec<FileRow>, DatabaseError>
    {
        releases::get_files_at(&self.0, rel, date).await
    }

    async fn add_file_url(
        &self,
        owner: Owner,
        proj: Project,
        release: Release,
        filename: &str,
        size: i64,
        sha256: &str,
        content_type: &str,
        requires: Option<&str>,
        url: &str,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        releases::add_file_url(
            &self.0,
            owner,
            proj,
            release,
            filename,
            size,
            sha256,
            content_type,
            requires,
            url,
            now
        ).await
    }

    async fn get_players(
        &self,
        proj: Project
    ) -> Result<Users, DatabaseError>
    {
        players::get_players(&self.0, proj).await
    }

    async fn add_player(
        &self,
        player: User,
        proj: Project
    ) -> Result<(), DatabaseError>
    {
        players::add_player(&self.0, player, proj).await
    }

    async fn remove_player(
        &self,
        player: User,
        proj: Project
    ) -> Result<(), DatabaseError>
    {
        players::remove_player(&self.0, player, proj).await
    }

    async fn get_image_url(
        &self,
        proj: Project,
        img_name: &str
    ) -> Result<Option<String>, DatabaseError>
    {
        images::get_image_url(&self.0, proj, img_name).await
    }

    async fn get_image_url_at(
        &self,
        proj: Project,
        img_name: &str,
        date: i64
    ) -> Result<Option<String>, DatabaseError>
    {
        images::get_image_url_at(&self.0, proj, img_name, date).await
    }

    async fn add_image_url(
        &self,
        owner: Owner,
        proj: Project,
        img_name: &str,
        url: &str,
        content_type: &str,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        images::add_image_url(
            &self.0,
            owner,
            proj,
            img_name,
            url,
            content_type,
            now
        ).await
    }

    async fn get_project_tags(
        &self,
        proj: Project
    ) -> Result<Vec<String>, DatabaseError>
    {
        tags::get_project_tags(&self.0, proj).await
    }

    async fn get_project_tags_at(
        &self,
        proj: Project,
        date: i64
    ) -> Result<Vec<String>, DatabaseError>
    {
        tags::get_project_tags_at(&self.0, proj, date).await
    }

    async fn get_gallery(
        &self,
        proj: Project
    ) -> Result<Vec<GalleryImage>, DatabaseError> {
        images::get_gallery(&self.0, proj).await
    }

    async fn get_gallery_at(
        &self,
        proj: Project,
        date: i64
    ) -> Result<Vec<GalleryImage>, DatabaseError> {
        images::get_gallery_at(&self.0, proj, date).await
    }

    async fn add_gallery_image(
        &self,
        owner: Owner,
        proj: Project,
        img_name: &str,
        url: &str,
        content_type: &str,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        images::add_gallery_image(
            &self.0,
            owner,
            proj,
            img_name,
            url,
            content_type,
            now
        ).await
    }

    async fn update_gallery(
        &self,
        owner: Owner,
        proj: Project,
        gallery_patch: &GalleryPatch,
        now: i64
    ) -> Result<(), DatabaseError>
    {
        images::update_gallery(&self.0, owner, proj, gallery_patch, now).await
    }

    async fn get_publishers(
        &self
    ) -> Result<Vec<String>, DatabaseError>
    {
        publishers::get_publishers(&self.0).await
    }

    async fn get_tags(
        &self
    ) -> Result<Vec<String>, DatabaseError>
    {
        tags::get_tags(&self.0).await
    }

    async fn get_tags_active(
        &self
    ) -> Result<Vec<String>, DatabaseError>
    {
        tags::get_tags_active(&self.0).await
    }

    async fn get_flag_id(
        &self,
        flag: i64
    ) -> Result<Option<Flag>, DatabaseError>
    {
        flag::get_flag_id(&self.0, flag).await
    }

    async fn add_flag(
        &self,
        reporter: User,
        proj: Project,
        flag: &FlagPost,
        now: i64
    ) -> Result<(), DatabaseError> {
        flag::add_flag(&self.0, reporter, proj, flag, now).await
    }

    async fn close_flag(
        &self,
        admin: Admin,
        flag: Flag,
        now: i64
    ) -> Result<(), DatabaseError> {
        flag::close_flag(&self.0, admin, flag, now).await
    }

    async fn get_flags(
        &self
    ) -> Result<Vec<FlagRow>, DatabaseError> {
        flag::get_flags(&self.0).await
    }
}

pub fn require_one_modified(r: SqliteQueryResult) -> Result<(), DatabaseError> {
    if r.rows_affected() == 1 {
        Ok(())
    }
    else {
        Err(DatabaseError::NotFound)
    }
}
