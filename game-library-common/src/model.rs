use serde::{Deserialize, Serialize};

use crate::pagination::Pagination;

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Users {
    pub users: Vec<String>
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Range {
    pub min: Option<i64>,
    pub max: Option<i64>
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GameData {
    pub title: String,
    pub title_sort_key: String,
    pub publisher: String,
    pub year: String,
    pub players: Range,
    pub length: Range
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileData {
    pub filename: String,
    pub url: String,
    pub size: i64,
    pub sha256: String,
    pub published_at: String,
    pub published_by: String,
    pub requires: Option<String>
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReleaseData {
    pub version: String,
    pub files: Vec<FileData>
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageData {
    pub name: String,
    pub slug: String,
    pub sort_key: i64,
    pub description: String,
    pub releases: Vec<ReleaseData>
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GalleryImage {
    pub id: i64,
    pub filename: String,
    pub description: String
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectData {
    pub name: String,
    pub slug: String,
    pub description: String,
    pub revision: i64,
    pub created_at: String,
    pub modified_at: String,
    pub tags: Vec<String>,
    pub game: GameData,
    pub readme: String,
    pub image: Option<String>,
    pub owners: Vec<String>,
    pub packages: Vec<PackageData>,
    pub gallery: Vec<GalleryImage>
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectSummary {
    pub name: String,
    pub slug: String,
    pub description: String,
    pub revision: i64,
    pub created_at: String,
    pub modified_at: String,
    pub tags: Vec<String>,
    pub game: GameData
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Projects {
    pub projects: Vec<ProjectSummary>,
    pub meta: Pagination
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Publishers {
    pub publishers: Vec<String>
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Tags {
    pub tags: Vec<String>
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("flag tag {0} unknown")]
pub struct FlagTagError(pub String);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "&str", rename_all = "lowercase")]
pub enum FlagTag {
    Inappropriate,
    Spam,
    Illegal,
    Other
}

impl TryFrom<&str> for FlagTag {
    type Error = FlagTagError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "inappropriate" => Ok(FlagTag::Inappropriate),
            "spam" => Ok(FlagTag::Spam),
            "illegal" => Ok(FlagTag::Illegal),
            "other" => Ok(FlagTag::Other),
            _ => Err(FlagTagError(value.into()))
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FlagData {
    pub flag_id: i64,
    pub project: String,
    pub slug: String,
    pub flag: FlagTag,
    pub flagged_at: String,
    pub flagged_by: String,
    pub message: Option<String>
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Flags {
    pub flags: Vec<FlagData>
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn try_from_str_flag_tag() {
        assert_eq!(FlagTag::try_from("inappropriate").unwrap(), FlagTag::Inappropriate);
        assert_eq!(FlagTag::try_from("spam").unwrap(), FlagTag::Spam);
        assert_eq!(FlagTag::try_from("illegal").unwrap(), FlagTag::Illegal);
        assert_eq!(FlagTag::try_from("other").unwrap(), FlagTag::Other);
        assert_eq!(FlagTag::try_from("bogus").unwrap_err(), FlagTagError("bogus".into()));
    }
}
