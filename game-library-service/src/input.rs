use glc::model::FlagTag;
use std::sync::LazyLock;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};

pub trait ConsecutiveWhitespace {
    fn has_consecutive_whitespace(&self) -> bool;
}

impl<T: AsRef<str>> ConsecutiveWhitespace for T {
    fn has_consecutive_whitespace(&self) -> bool {
        self.as_ref().chars()
            .zip(self.as_ref().chars().skip(1))
            .any(|(a, b)| a.is_whitespace() && b.is_whitespace())
    }
}

#[derive(Clone, Debug, Deserialize, Default, Eq, PartialEq, Serialize)]
pub struct MaybePackageDataPost {
    pub name: String,
    pub sort_key: i64,
    pub description: String
}

impl MaybePackageDataPost {
    fn is_valid(&self) -> bool {
        is_valid_package_name(&self.name) &&
        self.description.len() <= PACKAGE_DESCRIPTION_MAX_LENGTH &&
        self.description == self.description.trim()
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MaybePackageDataPost")]
pub struct PackageDataPost {
    pub name: String,
    pub sort_key: i64,
    pub description: String
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("invalid data {0:?}")]
pub struct PackageDataPostError(MaybePackageDataPost);

impl TryFrom<MaybePackageDataPost> for PackageDataPost {
    type Error = PackageDataPostError;

    fn try_from(m: MaybePackageDataPost) -> Result<Self, Self::Error> {
        match m.is_valid() {
            true => Ok(
                PackageDataPost {
                    name: m.name,
                    sort_key: m.sort_key,
                    description: m.description
                }
            ),
            false => Err(PackageDataPostError(m))
        }
    }
}

const PACKAGE_NAME_MAX_LENGTH: usize = 128;
const PACKAGE_DESCRIPTION_MAX_LENGTH: usize = 256;

#[derive(Debug, PartialEq, Eq)]
pub struct InvalidPackageName;

pub fn is_valid_package_name(name: &str) -> bool {
    // reject empty package names
    // reject overlong package names
    // reject package names with leading or trailing whitespace
    // reject package names with consecutive whitespace
    !name.is_empty() &&
    name.len() <= PACKAGE_NAME_MAX_LENGTH &&
    name == name.trim() &&
    !name.has_consecutive_whitespace()
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MaybePackageDataPatch {
    pub name: Option<String>,
    pub sort_key: Option<i64>,
    pub description: Option<String>
}

impl MaybePackageDataPatch {
    fn is_empty(&self) -> bool {
        matches!(
            self,
            MaybePackageDataPatch {
                name: None,
                sort_key: None,
                ..
            }
        )
    }

    fn is_valid(&self) -> bool {
        !self.is_empty() &&
         match &self.name {
            Some(n) => is_valid_package_name(n),
            None => true
        }
        &&
        match &self.description {
            Some(d) => !d.is_empty() &&
                d.len() <= PACKAGE_DESCRIPTION_MAX_LENGTH &&
                d == d.trim(),
            None => true
        }
    }
}

pub fn slug_for(s: &str) -> String {
    static HYPHENS: LazyLock<Regex> = LazyLock::new(||
        Regex::new("-+").expect("bad regex")
    );

    static SPECIAL: LazyLock<Regex> = LazyLock::new(||
        Regex::new(r#"[:/?#\[\]@!$&'()*+,;=%"<>\\^`{}|]"#).expect("bad regex")
    );

    // replace whitespace with hyphens
    let s = s.replace(char::is_whitespace, "-");
    // remove all special characters
    let s = SPECIAL.replace_all(&s, "");
    // coalesce consecutive hyphens
    let s = HYPHENS.replace_all(&s, "-");
    // remove leading and trailing hyphens
    s.trim_start_matches('-')
        .trim_end_matches('-')
        .into()
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MaybePackageDataPatch")]
pub struct PackageDataPatch {
    pub name: Option<String>,
    pub sort_key: Option<i64>,
    pub description: Option<String>
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("invalid data {0:?}")]
pub struct PackageDataPatchError(MaybePackageDataPatch);

impl TryFrom<MaybePackageDataPatch> for PackageDataPatch {
    type Error = PackageDataPatchError;

    fn try_from(m: MaybePackageDataPatch) -> Result<Self, Self::Error> {
        // at least one element must be present to be a valid request
        // and field lengths must be within bounds
        match m.is_valid() {
            true => Ok(
                PackageDataPatch {
                    name: m.name,
                    sort_key: m.sort_key,
                    description: m.description
                }
            ),
            false => Err(PackageDataPatchError(m))
        }
    }
}

fn double_option<'de, T, D>(de: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>
{
    // Ensure that explicit null in the JSON is mapped to Some(None)
    Deserialize::deserialize(de).map(Some)
}

// maximum field lengths
const PROJECT_DESCRIPTION_MAX_LENGTH: usize = 1024;
const GAME_TITLE_MAX_LENGTH: usize = 256;
const GAME_PUBLISHER_MAX_LENGTH: usize = 256;
const GAME_YEAR_MAX_LENGTH: usize = 32;
const README_MAX_LENGTH: usize = 65536;
const IMAGE_MAX_LENGTH: usize = 256;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangePatch {
    #[serde(default, deserialize_with = "double_option")]
    pub min: Option<Option<u32>>,
    #[serde(default, deserialize_with = "double_option")]
    pub max: Option<Option<u32>>
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MaybeGameDataPatch {
    pub title: Option<String>,
    pub publisher: Option<String>,
    pub year: Option<String>,
    pub players: Option<RangePatch>,
    pub length: Option<RangePatch>
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(from = "MaybeGameDataPatch")]
pub struct GameDataPatch {
    pub title: Option<String>,
    pub title_sort_key: Option<String>,
    pub publisher: Option<String>,
    pub year: Option<String>,
    pub players: RangePatch,
    pub length: RangePatch
}

impl From<MaybeGameDataPatch> for GameDataPatch {
    fn from(m: MaybeGameDataPatch) -> Self {
        GameDataPatch {
            title: m.title,
            title_sort_key: None,
            publisher: m.publisher,
            year: m.year,
            players: m.players.unwrap_or_default(),
            length: m.length.unwrap_or_default()
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MaybeProjectDataPatch {
    pub description: Option<String>,
    pub tags_add: Option<Vec<String>>,
    pub tags_remove: Option<Vec<String>>,
    pub game: Option<MaybeGameDataPatch>,
    pub readme: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    pub image: Option<Option<String>>
}

impl MaybeProjectDataPatch {
    fn is_empty(&self) -> bool {
        matches!(
            self,
            MaybeProjectDataPatch {
                description: None,
                tags_add: None,
                tags_remove: None,
                game: None | Some(MaybeGameDataPatch {
                    title: None,
                    publisher: None,
                    year: None,
                    players: None | Some(RangePatch {
                        min: None,
                        max: None
                    }),
                    length: None | Some(RangePatch {
                        min: None,
                        max: None
                    }),
                }),
                readme: None,
                image: None
            }
        )
    }

    fn is_valid(&self) -> bool {
        !self.is_empty() &&
        // check description
        match &self.description {
            Some(d) => !d.is_empty() &&
                d.len() <= PROJECT_DESCRIPTION_MAX_LENGTH &&
                d == d.trim(),
            None => true,
        }
        &&
        // check tags add
        match &self.tags_add {
            Some(tags_add) => !tags_add.is_empty(),
            None => true
        }
        &&
        // check tags remove
        match &self.tags_remove {
            Some(tags_remove) => !tags_remove.is_empty(),
            None => true
        }
        &&
        // check readme
        match &self.readme {
            Some(r) => r.len() <= README_MAX_LENGTH,
            None => true
        }
        &&
        // check image
        match &self.image {
            Some(Some(i)) => i.len() <= IMAGE_MAX_LENGTH,
            _ => true
        }
        &&
        // check game
        match &self.game {
            Some(game) => (
                // check title
                match &game.title {
                    Some(t) => !t.is_empty() &&
                        t.len() <= GAME_TITLE_MAX_LENGTH &&
                        t == t.trim() &&
                        !t.has_consecutive_whitespace(),
                    None => true
                }
                &&
                // check title
                match &game.publisher {
                    Some(p) => p.len() <= GAME_PUBLISHER_MAX_LENGTH &&
                        p == p.trim() &&
                        !p.has_consecutive_whitespace(),
                    None => true
                }
                &&
                // check year
                match &game.year {
                    Some(y) => y.len() <= GAME_YEAR_MAX_LENGTH &&
                        y == y.trim() &&
                        !y.has_consecutive_whitespace(),
                    None => true
                }
            ),
            None => true
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MaybeProjectDataPatch")]
pub struct ProjectDataPatch {
    pub description: Option<String>,
    pub tags_add: Option<Vec<String>>,
    pub tags_remove: Option<Vec<String>>,
    #[serde(default)]
    pub game: GameDataPatch,
    pub readme: Option<String>,
    pub image: Option<Option<String>>
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("invalid data {0:?}")]
pub struct ProjectDataPatchError(MaybeProjectDataPatch);

impl TryFrom<MaybeProjectDataPatch> for ProjectDataPatch {
    type Error = ProjectDataPatchError;

    fn try_from(m: MaybeProjectDataPatch) -> Result<Self, Self::Error> {
        // at least one element must be present to be a valid request
        // and field lengths must be within bounds
        match m.is_valid() {
            true => Ok(
                ProjectDataPatch {
                    description: m.description,
                    tags_add: m.tags_add,
                    tags_remove: m.tags_remove,
                    game: m.game.unwrap_or_default().into(),
                    readme: m.readme,
                    image: m.image
                }
            ),
            false => Err(ProjectDataPatchError(m))
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MaybeRangePost {
    pub min: Option<u32>,
    pub max: Option<u32>
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MaybeRangePost")]
pub struct RangePost {
    pub min: Option<u32>,
    pub max: Option<u32>
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("min > max: {0:?}")]
pub struct RangePostError(MaybeRangePost);

impl TryFrom<MaybeRangePost> for RangePost {
    type Error = RangePostError;

    fn try_from(m: MaybeRangePost) -> Result<Self, Self::Error> {
        match (m.min, m.max) {
            (None, _) | (_, None) => Ok(RangePost { min: m.min, max: m.max }),
            (Some(min), Some(max)) if min <= max => Ok(RangePost { min: m.min, max: m.max }),
            _ => Err(RangePostError(m))
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GameDataPost {
    pub title: String,
    #[serde(skip)]
    pub title_sort_key: String,
    pub publisher: String,
    pub year: String,
    pub players: RangePost,
    pub length: RangePost
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MaybeProjectDataPost {
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub game: GameDataPost,
    pub readme: String,
    pub image: Option<String>
}

impl MaybeProjectDataPost {
    fn is_valid(&self) -> bool {
        // check description
        self.description.len() <= PROJECT_DESCRIPTION_MAX_LENGTH &&
        self.description == self.description.trim() &&
        // check title
        self.game.title.len() <= GAME_TITLE_MAX_LENGTH &&
        self.game.title == self.game.title.trim() &&
        !self.game.title.has_consecutive_whitespace() &&
        // check publisher
        self.game.publisher.len() <= GAME_PUBLISHER_MAX_LENGTH &&
        self.game.publisher == self.game.publisher.trim() &&
        !self.game.publisher.has_consecutive_whitespace() &&
        // check year
        self.game.year.len() <= GAME_YEAR_MAX_LENGTH &&
        self.game.year == self.game.year.trim() &&
        !self.game.year.has_consecutive_whitespace() &&
        // check readme
        self.readme.len() <= README_MAX_LENGTH &&
        // check image
        self.image.as_ref().is_none_or(|i| i.len() <= IMAGE_MAX_LENGTH)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MaybeProjectDataPost")]
pub struct ProjectDataPost {
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub game: GameDataPost,
    pub readme: String,
    pub image: Option<String>
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("invalid data {0:?}")]
pub struct ProjectDataPostError(MaybeProjectDataPost);

impl TryFrom<MaybeProjectDataPost> for ProjectDataPost {
    type Error = ProjectDataPostError;

    fn try_from(m: MaybeProjectDataPost) -> Result<Self, Self::Error> {
        // field lengths must be within bounds
        if m.is_valid() {
            Ok(
                ProjectDataPost{
                    name: m.name,
                    description: m.description,
                    tags: m.tags,
                    game: m.game,
                    readme: m.readme,
                    image: m.image
                }
            )
        }
        else {
            Err(ProjectDataPostError(m))
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum MaybeGalleryOp {
    Update {
        id: i64,
        description: String
    },
    Delete {
        id: i64
    },
    Move {
        id: i64,
        next: Option<i64>
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MaybeGalleryOp")]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum GalleryOp {
    Update {
        id: i64,
        description: String
    },
    Delete {
        id: i64
    },
    Move {
        id: i64,
        next: Option<i64>
    }
}

const GALLERY_ITEM_DESCRIPTION_MAX_LENGTH: usize = 128;

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("invalid data {0:?}")]
pub struct GalleryOpError(MaybeGalleryOp);

impl TryFrom<MaybeGalleryOp> for GalleryOp {
    type Error = GalleryOpError;

    fn try_from(m: MaybeGalleryOp) -> Result<Self, Self::Error> {
        match m {
            MaybeGalleryOp::Update { id, description } => {
                // field lengths must be within bounds
                if description.len() <= GALLERY_ITEM_DESCRIPTION_MAX_LENGTH &&
                    description == description.trim()
                {
                    Ok(GalleryOp::Update { id, description })
                }
                else {
                    Err(GalleryOpError(
                        MaybeGalleryOp::Update { id, description }
                    ))
                }
            },
            MaybeGalleryOp::Delete { id } =>
                Ok(GalleryOp::Delete { id }),
            MaybeGalleryOp::Move { id, next } =>
                Ok(GalleryOp::Move { id, next })
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct MaybeGalleryPatch {
    pub ops: Vec<GalleryOp>
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MaybeGalleryPatch")]
pub struct GalleryPatch {
    pub ops: Vec<GalleryOp>
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("invalid data {0:?}")]
pub struct GalleryPatchError(MaybeGalleryPatch);

impl TryFrom<MaybeGalleryPatch> for GalleryPatch {
    type Error = GalleryPatchError;

    fn try_from(m: MaybeGalleryPatch) -> Result<Self, Self::Error> {
        if m.ops.is_empty() {
            Err(GalleryPatchError(m))
        }
        else {
            Ok(GalleryPatch { ops: m.ops })
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MaybeFlagPost {
    pub flag: FlagTag,
    pub message: Option<String>
}

impl From<FlagPost> for MaybeFlagPost {
    fn from(fp: FlagPost) -> MaybeFlagPost {
        match fp {
            FlagPost::Inappropriate => MaybeFlagPost {
                flag: FlagTag::Inappropriate,
                message: None
            },
            FlagPost::Spam => MaybeFlagPost {
                flag: FlagTag::Spam,
                message: None
            },
            FlagPost::Illegal(msg) => MaybeFlagPost {
                flag: FlagTag::Illegal,
                message: Some(msg)
            },
            FlagPost::Other(msg) => MaybeFlagPost {
                flag: FlagTag::Other,
                message: Some(msg)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MaybeFlagPost", into = "MaybeFlagPost")]
pub enum FlagPost {
    Inappropriate,
    Spam,
    Illegal(String),
    Other(String)
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("flag {0:?} invalid")]
pub struct FlagPostError(MaybeFlagPost);

impl TryFrom<MaybeFlagPost> for FlagPost {
    type Error = FlagPostError;

    fn try_from(fd: MaybeFlagPost) -> Result<Self, Self::Error> {
        match fd {
            MaybeFlagPost {
                flag: FlagTag::Inappropriate,
                message: None
            } => Ok(FlagPost::Inappropriate),
            MaybeFlagPost {
                flag: FlagTag::Spam,
                message: None
            } => Ok(FlagPost::Spam),
            MaybeFlagPost {
                flag: FlagTag::Illegal,
                message: Some(msg)
            } => Ok(FlagPost::Illegal(msg)),
            MaybeFlagPost {
                flag: FlagTag::Other,
                message: Some(msg)
            } => Ok(FlagPost::Other(msg)),
            _ => Err(FlagPostError(fd))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_valid_package_name_ok() {
        let name = "acceptable_name";
        assert!(is_valid_package_name(name));
    }

    #[test]
    fn is_valid_package_name_untrimmed() {
        assert!(!is_valid_package_name("  bad  "));
    }

    #[test]
    fn is_valid_package_name_consecutive_whitespace() {
        assert!(!is_valid_package_name("x  x"));
    }

    #[test]
    fn is_valid_package_name_overlong() {
        assert!(!is_valid_package_name(&"x".repeat(PACKAGE_NAME_MAX_LENGTH + 1)));
    }

    #[test]
    fn slug_for_no_change() {
        assert_eq!(slug_for("abcd"), "abcd");
    }

    #[test]
    fn slug_for_whitespace() {
        assert_eq!(slug_for("x      x"), "x-x");
    }

    #[test]
    fn slug_for_consecutive_hyphens() {
        assert_eq!(slug_for("x----x---x"), "x-x-x");
    }

    #[test]
    fn slug_for_trim_hyphens() {
        assert_eq!(slug_for("-x-"), "x");
    }

    #[test]
    fn slug_for_special() {
        assert_eq!(slug_for("x/#?*x"), "xx");
    }

    #[test]
    fn slug_for_nonascii() {
        assert_eq!(slug_for("x💩x"), "x💩x");
    }

    #[test]
    fn try_from_package_data_post_ok() {
        assert_eq!(
            PackageDataPost::try_from(
                MaybePackageDataPost {
                    name: "pkg".into(),
                    sort_key: 3,
                    description: "desc".into()
                }
            ).unwrap(),
            PackageDataPost {
                name: "pkg".into(),
                sort_key: 3,
                description: "desc".into()
            }
        );
    }

    #[test]
    fn try_from_package_data_post_overlong_description() {
        let mpdp = MaybePackageDataPost {
            description: "x".repeat(PROJECT_DESCRIPTION_MAX_LENGTH + 1),
            ..Default::default()
        };

        assert_eq!(
            PackageDataPost::try_from(mpdp.clone()).unwrap_err(),
            PackageDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_package_data_post_untrimmed_description() {
        let mpdp = MaybePackageDataPost {
            description: " x ".into(),
            ..Default::default()
        };

        assert_eq!(
            PackageDataPost::try_from(mpdp.clone()).unwrap_err(),
            PackageDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_package_data_patch_ok() {
        assert_eq!(
            PackageDataPatch::try_from(
                MaybePackageDataPatch {
                    name: Some("foo".into()),
                    sort_key: Some(3),
                    description: Some("desc".into())
                }
            ).unwrap(),
            PackageDataPatch {
                name: Some("foo".into()),
                sort_key: Some(3),
                description: Some("desc".into())
            }
        );
    }

    #[test]
    fn try_from_package_data_patch_overlong_description() {
        let mpdp = MaybePackageDataPatch {
            description: Some("x".repeat(PACKAGE_DESCRIPTION_MAX_LENGTH + 1)),
            ..Default::default()
        };

        assert_eq!(
            PackageDataPatch::try_from(mpdp.clone()).unwrap_err(),
            PackageDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_package_data_patch_untrimmed_description() {
        let mpdp = MaybePackageDataPatch {
            description: Some(" x ".into()),
            ..Default::default()
        };

        assert_eq!(
            PackageDataPatch::try_from(mpdp.clone()).unwrap_err(),
            PackageDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_package_data_patch_empty_name() {
        let mpdp = MaybePackageDataPatch {
            name: Some("".into()),
            ..Default::default()
        };

        assert_eq!(
            PackageDataPatch::try_from(mpdp.clone()).unwrap_err(),
            PackageDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_package_data_patch_untrimmed_name() {
        let mpdp = MaybePackageDataPatch {
            name: Some(" x ".into()),
            ..Default::default()
        };

        assert_eq!(
            PackageDataPatch::try_from(mpdp.clone()).unwrap_err(),
            PackageDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_package_data_patch_consecutive_whitespace_name() {
        let mpdp = MaybePackageDataPatch {
            name: Some("x  x".into()),
            ..Default::default()
        };

        assert_eq!(
            PackageDataPatch::try_from(mpdp.clone()).unwrap_err(),
            PackageDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_package_data_patch_overlong_name() {
        let mpdp = MaybePackageDataPatch {
            name: Some("x".repeat(PACKAGE_DESCRIPTION_MAX_LENGTH + 1)),
            ..Default::default()
        };

        assert_eq!(
            PackageDataPatch::try_from(mpdp.clone()).unwrap_err(),
            PackageDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_package_data_patch_empty() {
        let mpdp = MaybePackageDataPatch::default();

        assert_eq!(
            PackageDataPatch::try_from(mpdp.clone()).unwrap_err(),
            PackageDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_post_ok() {
        assert_eq!(
            ProjectDataPost::try_from(
                MaybeProjectDataPost {
                    name: "name".into(),
                    description: "description".into(),
                    tags: vec![],
                    game: GameDataPost {
                        title: "the title".into(),
                        title_sort_key: "title, the".into(),
                        publisher: "publisher".into(),
                        year: "1979".into(),
                        players: RangePost::default(),
                        length: RangePost::default()
                    },
                    readme: "readme".into(),
                    image: None
                }
            ).unwrap(),
            ProjectDataPost {
                name: "name".into(),
                description: "description".into(),
                tags: vec![],
                game: GameDataPost {
                    title: "the title".into(),
                    title_sort_key: "title, the".into(),
                    publisher: "publisher".into(),
                    year: "1979".into(),
                    players: RangePost::default(),
                    length: RangePost::default()
                },
                readme: "readme".into(),
                image: None
            }
        );
    }

    #[test]
    fn try_from_project_data_post_overlong_description() {
        let mpdp = MaybeProjectDataPost {
            description: "x".repeat(PROJECT_DESCRIPTION_MAX_LENGTH + 1),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPost::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_post_overlong_title() {
        let mpdp = MaybeProjectDataPost {
            game: GameDataPost {
                title: "x".repeat(GAME_TITLE_MAX_LENGTH + 1),
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPost::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_post_consecutive_whitespace_title() {
        let mpdp = MaybeProjectDataPost {
            game: GameDataPost {
                title: "x  x".into(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPost::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_post_overlong_publisher() {
        let mpdp = MaybeProjectDataPost {
            game: GameDataPost {
                publisher: "x".repeat(GAME_PUBLISHER_MAX_LENGTH + 1),
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPost::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_post_consecutive_whitespace_publisher() {
        let mpdp = MaybeProjectDataPost {
            game: GameDataPost {
                publisher: "x  x".into(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPost::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_post_overlong_year() {
        let mpdp = MaybeProjectDataPost {
            game: GameDataPost {
                year: "x".repeat(GAME_YEAR_MAX_LENGTH + 1),
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPost::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_post_consecutive_whitespace_year() {
        let mpdp = MaybeProjectDataPost {
            game: GameDataPost {
                year: "x  x".into(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPost::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_post_overlong_readme() {
        let mpdp = MaybeProjectDataPost {
            readme: "x".repeat(README_MAX_LENGTH + 1),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPost::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPostError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_post_overlong_image() {
        let mpdp = MaybeProjectDataPost {
            image: Some("x".repeat(IMAGE_MAX_LENGTH + 1)),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPost::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPostError(mpdp)
        );
    }

    #[test]
    fn maybe_project_data_post_from_json_minimal() {
    let json = r#"
        {
            "game": {
                "title": "foo",
                "publisher": "",
                "year": "",
                "players": { "min": null, "max": null },
                "length": { "min": null, "max": null }
            },
            "name": "",
            "description": "",
            "tags": [],
            "readme": ""
        }"#;

        assert_eq!(
            serde_json::from_str::<MaybeProjectDataPost>(json).unwrap(),
            MaybeProjectDataPost {
                name: "".into(),
                description: "".into(),
                tags: vec![],
                game: GameDataPost {
                    title: "foo".into(),
                    title_sort_key: "".into(),
                    publisher: "".into(),
                    year: "".into(),
                    ..Default::default()
                },
                readme: "".into(),
                ..Default::default()
            }
        );
    }

    #[test]
    fn maybe_project_data_patch_from_json_game_title() {
        let json = r#"{"game":{"title":"foo"}}"#;
        assert_eq!(
            serde_json::from_str::<MaybeProjectDataPatch>(json).unwrap(),
            MaybeProjectDataPatch {
                game: Some(MaybeGameDataPatch {
                    title: Some("foo".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }
        );
    }

    #[test]
    fn maybe_project_data_patch_from_json_image() {
        let json = r#"{"image": "foo.png"}"#;
        assert_eq!(
            serde_json::from_str::<MaybeProjectDataPatch>(json).unwrap(),
            MaybeProjectDataPatch {
                image: Some(Some("foo.png".into())),
                ..Default::default()
            }
        );
    }

    #[test]
    fn maybe_project_data_patch_from_json_image_clear() {
        let json = r#"{"image": null}"#;
        assert_eq!(
            serde_json::from_str::<MaybeProjectDataPatch>(json).unwrap(),
            MaybeProjectDataPatch {
                image: Some(None),
                ..Default::default()
            }
        );
    }

    #[test]
    fn maybe_project_data_patch_default_empty() {
        assert!(MaybeProjectDataPatch::default().is_empty());
    }

    #[test]
    fn maybe_project_data_patch_default_and_game_empty() {
        assert!(
            MaybeProjectDataPatch {
                game: Some(MaybeGameDataPatch::default()),
                ..Default::default()
            }.is_empty()
        );
    }

    #[test]
    fn maybe_project_data_patch_description_not_empty() {
        assert!(
            !MaybeProjectDataPatch {
                description: Some("description".into()),
                ..Default::default()
            }.is_empty()
        );
    }

    #[test]
    fn maybe_project_data_patch_readme_not_empty() {
        assert!(
            !MaybeProjectDataPatch {
                readme: Some("readme".into()),
                ..Default::default()
            }.is_empty()
        );
    }

    #[test]
    fn maybe_project_data_patch_image_not_empty() {
        assert!(
            !MaybeProjectDataPatch {
                image: Some(None),
                ..Default::default()
            }.is_empty()
        );
    }

    #[test]
    fn maybe_project_data_patch_image_not_empty_none() {
        assert!(
            !MaybeProjectDataPatch {
                image: Some(Some("image".into())),
                ..Default::default()
            }.is_empty()
        );
    }

    #[test]
    fn maybe_project_data_patch_title_not_empty() {
        assert!(
            !MaybeProjectDataPatch {
                game: Some(MaybeGameDataPatch {
                    title: Some("title".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }.is_empty()
        );
    }

    #[test]
    fn maybe_project_data_patch_publisher_not_empty() {
        assert!(
            !MaybeProjectDataPatch {
                game: Some(MaybeGameDataPatch {
                    publisher: Some("publisher".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }.is_empty()
        );
    }

    #[test]
    fn maybe_project_data_patch_year_not_empty() {
        assert!(
            !MaybeProjectDataPatch {
                game: Some(MaybeGameDataPatch {
                    year: Some("1979".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }.is_empty()
        );
    }

    #[test]
    fn maybe_project_data_patch_plyers_not_empty() {
        assert!(
            !MaybeProjectDataPatch {
                game: Some(MaybeGameDataPatch {
                    players: Some(RangePatch {
                        min: Some(Some(1)),
                        max: Some(Some(2))
                     }),
                    ..Default::default()
                }),
                ..Default::default()
            }.is_empty()
        );
    }

    #[test]
    fn try_from_project_data_patch_description() {
        assert_eq!(
            ProjectDataPatch::try_from(
                MaybeProjectDataPatch {
                    description: Some("description".into()),
                    ..Default::default()
                }
            ).unwrap(),
            ProjectDataPatch {
                description: Some("description".into()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn try_from_project_data_patch_image_clear() {
        assert_eq!(
            ProjectDataPatch::try_from(
                MaybeProjectDataPatch {
                    image: Some(None),
                    ..Default::default()
                }
            ).unwrap(),
            ProjectDataPatch {
                image: Some(None),
                ..Default::default()
            }
        );
    }

    #[test]
    fn try_from_project_data_patch_empty() {
        assert_eq!(
            ProjectDataPatch::try_from(MaybeProjectDataPatch::default())
                .unwrap_err(),
            ProjectDataPatchError(MaybeProjectDataPatch::default())
        );
    }

    #[test]
    fn try_from_project_data_patch_overlong_description() {
        let mpdp = MaybeProjectDataPatch {
            description: Some("x".repeat(PROJECT_DESCRIPTION_MAX_LENGTH + 1)),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_untrimmed_description() {
        let mpdp = MaybeProjectDataPatch {
            description: Some(" x ".into()),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_overlong_title() {
        let mpdp = MaybeProjectDataPatch {
            game: Some(MaybeGameDataPatch {
                title: Some("x".repeat(GAME_TITLE_MAX_LENGTH + 1)),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_untrimmed_title() {
        let mpdp = MaybeProjectDataPatch {
            game: Some(MaybeGameDataPatch {
                title: Some(" x ".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_consecutive_whitespace_title() {
        let mpdp = MaybeProjectDataPatch {
            game: Some(MaybeGameDataPatch {
                title: Some("x  x".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_overlong_publisher() {
        let mpdp = MaybeProjectDataPatch {
            game: Some(MaybeGameDataPatch {
                publisher: Some("x".repeat(GAME_PUBLISHER_MAX_LENGTH + 1)),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_untrimmed_publisher() {
        let mpdp = MaybeProjectDataPatch {
            game: Some(MaybeGameDataPatch {
                publisher: Some(" x ".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_consecutive_whitespace_publisher() {
        let mpdp = MaybeProjectDataPatch {
            game: Some(MaybeGameDataPatch {
                publisher: Some("x  x".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_overlong_year() {
        let mpdp = MaybeProjectDataPatch {
            game: Some(MaybeGameDataPatch {
                year: Some("x".repeat(GAME_YEAR_MAX_LENGTH + 1)),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_untrimmed_year() {
        let mpdp = MaybeProjectDataPatch {
            game: Some(MaybeGameDataPatch {
                year: Some(" x ".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_consecutive_whitespace_year() {
        let mpdp = MaybeProjectDataPatch {
            game: Some(MaybeGameDataPatch {
                year: Some("x  x".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_overlong_readme() {
        let mpdp = MaybeProjectDataPatch {
            readme: Some("x".repeat(README_MAX_LENGTH + 1)),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_overlong_image() {
        let mpdp = MaybeProjectDataPatch {
            image: Some(Some("x".repeat(IMAGE_MAX_LENGTH + 1))),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_empty_tags_add() {
        let mpdp = MaybeProjectDataPatch {
            tags_add: Some(vec![]),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_project_data_patch_empty_tags_remove() {
        let mpdp = MaybeProjectDataPatch {
            tags_remove: Some(vec![]),
            ..Default::default()
        };

        assert_eq!(
            ProjectDataPatch::try_from(mpdp.clone()).unwrap_err(),
            ProjectDataPatchError(mpdp)
        );
    }

    #[test]
    fn try_from_maybe_flag_post_flag_post_inappropriate() {
        assert_eq!(
            FlagPost::try_from(MaybeFlagPost {
                flag: FlagTag::Inappropriate,
                message: None
            }),
            Ok(FlagPost::Inappropriate)
        );
    }

    #[test]
    fn try_from_maybe_flag_post_flag_post_inappropriate_msg() {
        let fd = MaybeFlagPost {
            flag: FlagTag::Inappropriate,
            message: Some("bad".into())
        };

        assert_eq!(
            FlagPost::try_from(fd.clone()),
            Err(FlagPostError(fd))
        );
    }

    #[test]
    fn try_from_maybe_flag_post_flag_post_spam() {
        assert_eq!(
            FlagPost::try_from(MaybeFlagPost {
                flag: FlagTag::Spam,
                message: None
            }),
            Ok(FlagPost::Spam)
        );
    }

    #[test]
    fn try_from_maybe_flag_post_flag_post_spam_msg() {
        let fd = MaybeFlagPost {
            flag: FlagTag::Spam,
            message: Some("bad".into())
        };

        assert_eq!(
            FlagPost::try_from(fd.clone()),
            Err(FlagPostError(fd))
        );
    }

    #[test]
    fn try_from_maybe_flag_post_flag_post_illegal() {
        let fd = MaybeFlagPost {
            flag: FlagTag::Illegal,
            message: None
        };

        assert_eq!(
            FlagPost::try_from(fd.clone()),
            Err(FlagPostError(fd))
        );
    }

    #[test]
    fn try_from_maybe_flag_post_flag_post_illegal_msg() {
        assert_eq!(
            FlagPost::try_from(MaybeFlagPost {
                flag: FlagTag::Illegal,
                message: Some("ok".into())
            }),
            Ok(FlagPost::Illegal("ok".into()))
        );
    }

    #[test]
    fn try_from_maybe_flag_post_flag_post_other() {
        let fd = MaybeFlagPost {
            flag: FlagTag::Other,
            message: None
        };

        assert_eq!(
            FlagPost::try_from(fd.clone()),
            Err(FlagPostError(fd))
        );
    }

    #[test]
    fn try_from_maybe_flag_post_flag_pos_other_msg() {
        assert_eq!(
            FlagPost::try_from(MaybeFlagPost {
                flag: FlagTag::Other,
                message: Some("ok".into())
            }),
            Ok(FlagPost::Other("ok".into()))
        );
    }

    #[test]
    fn try_from_maybe_range_post_none_none() {
        assert_eq!(
            RangePost::try_from(MaybeRangePost { min: None, max: None }),
            Ok(RangePost { min: None, max: None })
        );
    }

    #[test]
    fn try_from_maybe_range_post_some_none() {
        assert_eq!(
            RangePost::try_from(MaybeRangePost { min: Some(1), max: None }),
            Ok(RangePost { min: Some(1), max: None })
        );
    }

    #[test]
    fn try_from_maybe_range_post_none_some() {
        assert_eq!(
            RangePost::try_from(MaybeRangePost { min: None, max: Some(1) }),
            Ok(RangePost { min: None, max: Some(1) })
        );
    }

    #[test]
    fn try_from_maybe_range_post_some_some_eq() {
        assert_eq!(
            RangePost::try_from(MaybeRangePost { min: Some(1), max: Some(1) }),
            Ok(RangePost { min: Some(1), max: Some(1) })
        );
    }

    #[test]
    fn try_from_maybe_range_post_some_some_less() {
        assert_eq!(
            RangePost::try_from(MaybeRangePost { min: Some(0), max: Some(1) }),
            Ok(RangePost { min: Some(0), max: Some(1) })
        );
    }

    #[test]
    fn try_from_maybe_range_post_some_some_more() {
        let mrp = MaybeRangePost { min: Some(1), max: Some(0) };
        assert_eq!(
            RangePost::try_from(mrp.clone()),
            Err(RangePostError(mrp))
        );
    }

    #[test]
    fn maybe_gallery_op_delete_from_json() {
        let json = r#"{ "op": "delete", "id": 3 }"#;
        assert_eq!(
            serde_json::from_str::<MaybeGalleryOp>(json).unwrap(),
            MaybeGalleryOp::Delete { id: 3 }
        );
    }

    #[test]
    fn maybe_gallery_op_update_from_json() {
        let json = r#"{ "op": "update", "id": 3, "description": "x" }"#;
        assert_eq!(
            serde_json::from_str::<MaybeGalleryOp>(json).unwrap(),
            MaybeGalleryOp::Update { id: 3, description: "x".into() }
        );
    }

    #[test]
    fn maybe_gallery_op_move_from_json() {
        let json = r#"{ "op": "move", "id": 3, "next": 5 }"#;
        assert_eq!(
            serde_json::from_str::<MaybeGalleryOp>(json).unwrap(),
            MaybeGalleryOp::Move { id: 3, next: Some(5) }
        );
    }

    #[test]
    fn maybe_gallery_op_move_end_from_json() {
        let json = r#"{ "op": "move", "id": 3, "next": null }"#;
        assert_eq!(
            serde_json::from_str::<MaybeGalleryOp>(json).unwrap(),
            MaybeGalleryOp::Move { id: 3, next: None }
        );
    }

    #[test]
    fn try_from_maybe_gallery_op_delete() {
        let mgp = MaybeGalleryOp::Delete { id: 3 };
        assert_eq!(
            GalleryOp::try_from(mgp),
            Ok(GalleryOp::Delete { id: 3 })
        );
    }

    #[test]
    fn try_from_maybe_gallery_op_update() {
        let mgp = MaybeGalleryOp::Update { id: 3, description: "x".into() };
        assert_eq!(
            GalleryOp::try_from(mgp),
            Ok(GalleryOp::Update { id: 3, description: "x".into() })
        );
    }

    #[test]
    fn try_from_maybe_gallery_op_update_too_long() {
        let mgp = MaybeGalleryOp::Update {
            id: 3,
            description: "x".repeat(GALLERY_ITEM_DESCRIPTION_MAX_LENGTH + 1)
        };
        assert_eq!(
            GalleryOp::try_from(mgp.clone()),
            Err(GalleryOpError(mgp))
        );
    }

    #[test]
    fn try_from_maybe_gallery_op_update_untrimmed() {
        let mgp = MaybeGalleryOp::Update {
            id: 3,
            description: " x ".into()
        };
        assert_eq!(
            GalleryOp::try_from(mgp.clone()),
            Err(GalleryOpError(mgp))
        );
    }

    #[test]
    fn try_from_maybe_gallery_op_move() {
        let mgp = MaybeGalleryOp::Move { id: 3, next: Some(5) };
        assert_eq!(
            GalleryOp::try_from(mgp),
            Ok(GalleryOp::Move { id: 3, next: Some(5) })
        );
    }

    #[test]
    fn try_from_maybe_gallery_op_move_end() {
        let mgp = MaybeGalleryOp::Move { id: 3, next: None };
        assert_eq!(
            GalleryOp::try_from(mgp),
            Ok(GalleryOp::Move { id: 3, next: None })
        );
    }


}
