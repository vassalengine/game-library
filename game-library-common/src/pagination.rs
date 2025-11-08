use serde::{Deserialize, Serialize};
use std::{
    fmt,
    num::NonZeroU8
};
use urlencoding::Encoded;

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum LimitError {
    #[error("limit {0} out of range")]
    OutOfRange(u8),
    #[error("limit {0} malformed")]
    Malformed(String)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[repr(transparent)]
#[serde(try_from = "&str")]
pub struct Limit(NonZeroU8);

impl Limit {
    pub const fn new(limit: u8) -> Option<Limit> {
        match limit {
            limit if limit > 100 => None,
            limit => match NonZeroU8::new(limit) {
                Some(n) => Some(Limit(n)),
                None => None
            }
        }
    }

    pub const fn get(self) -> u8 {
        self.0.get()
    }
}

impl Default for Limit {
    fn default() -> Self {
        Limit::new(10).expect("0 < 10 <= 100")
    }
}

impl TryFrom<&str> for Limit {
    type Error = LimitError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.parse::<u8>() {
            Ok(n) => Limit::new(n).ok_or(LimitError::OutOfRange(n)),
            Err(_) => Err(LimitError::Malformed(s.into()))
        }
    }
}

impl fmt::Display for Limit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
// Must be String instead of &str due to percent-decoding;
// the incoming type is actually Cow<'a, str>
#[serde(try_from = "String")]
pub enum Anchor {
    Start,
    Before(String, u32),
    After(String, u32)
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("anchor {0} invalid")]
pub struct AnchorError(String);

impl TryFrom<String> for Anchor {
    type Error = AnchorError;

    fn try_from(v: String) -> Result<Self, Self::Error> {
        let mut s = v.split('\t');

        let tag = s.next()
            .ok_or(AnchorError(v.clone()))?;

        match tag {
            "s" if s.next().is_none() => Ok(Anchor::Start),
            "b" => match (s.next(), s.next(), s.next()) {
                (Some(f), Some(i), None) => {
                    let i = i.parse().map_err(|_| AnchorError(v.clone()))?;
                    Ok(Anchor::Before(f.into(), i))
                },
                _ => Err(AnchorError(v.clone()))
            },
            "a" => match (s.next(), s.next(), s.next()) {
                (Some(f), Some(i), None) => {
                    let i = i.parse().map_err(|_| AnchorError(v.clone()))?;
                    Ok(Anchor::After(f.into(), i))
                },
                _ => Err(AnchorError(v.clone()))
            },
            _ => Err(AnchorError(v.clone()))
        }
    }
}

impl From<Anchor> for String {
    fn from(a: Anchor) -> String {
        a.to_string()
    }
}

impl fmt::Display for Anchor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Anchor::Start => write!(f, "s"),
            Anchor::Before(field, id) => write!(
                f,
                "b%09{}%09{id}",
                Encoded(field)
            ),
            Anchor::After(field, id) => write!(
                f,
                "a%09{}%09{id}",
                Encoded(field)
            )
        }
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("direction {0} invalid")]
pub struct DirectionError(String);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "&str", into = "String")]
pub enum Direction {
    Ascending,
    Descending
}

impl Direction {
    pub fn rev(&self) -> Direction {
        match self {
            Direction::Ascending => Direction::Descending,
            Direction::Descending => Direction::Ascending
        }
    }
}

impl From<Direction> for String {
    fn from(d: Direction) -> Self {
        d.to_string()
    }
}

impl TryFrom<&str> for Direction {
    type Error = DirectionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "a" => Ok(Direction::Ascending),
            "d" => Ok(Direction::Descending),
            _ => Err(DirectionError(value.into()))
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = match self {
            Direction::Ascending => "a",
            Direction::Descending => "d"
        };
        write!(f, "{}", d)
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("sort {0} invalid")]
pub struct SortByError(String);

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "&str", into = "String")]
pub enum SortBy {
    ProjectName,
    #[default]
    GameTitle,
    ModificationTime,
    CreationTime,
    Relevance
}

impl From<SortBy> for String {
    fn from(s: SortBy) -> Self {
        s.to_string()
    }
}

impl TryFrom<&str> for SortBy {
    type Error = SortByError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "p" => Ok(SortBy::ProjectName),
            "t" => Ok(SortBy::GameTitle),
            "m" => Ok(SortBy::ModificationTime),
            "c" => Ok(SortBy::CreationTime),
            "r" => Ok(SortBy::Relevance),
            _ => Err(SortByError(value.into()))
        }
    }
}

impl fmt::Display for SortBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
         let s = match self {
            SortBy::ProjectName => "p",
            SortBy::GameTitle => "t",
            SortBy::ModificationTime => "m",
            SortBy::CreationTime => "c",
            SortBy::Relevance => "r"
        };
        write!(f, "{}", s)
    }
}

impl SortBy {
    pub fn default_direction(&self) -> Direction {
        match self {
            SortBy::ProjectName => Direction::Ascending,
            SortBy::GameTitle => Direction::Ascending,
            SortBy::ModificationTime => Direction::Descending,
            SortBy::CreationTime => Direction::Descending,
            SortBy::Relevance => Direction::Ascending
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Facet {
    Query(String),
    Publisher(String),
    Year(String),
    PlayersMin(u32),
    PlayersMax(u32),
    PlayersInc(u32),
    LengthMin(u32),
    LengthMax(u32),
    Tag(String),
    Owner(String),
    Player(String)
}

impl fmt::Display for Facet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Facet::Query(q) => write!(f, "q={}", Encoded(q)),
            Facet::Publisher(p) => write!(f, "publisher={}", Encoded(p)),
            Facet::Year(y) => write!(f, "year={}", Encoded(y)),
            Facet::PlayersMin(m) => write!(f, "players_min={m}"),
            Facet::PlayersMax(m) => write!(f, "players_max={m}"),
            Facet::PlayersInc(m) => write!(f, "players_inc={m}"),
            Facet::LengthMin(m) => write!(f, "length_min={m}"),
            Facet::LengthMax(m) => write!(f, "length_max={m}"),
            Facet::Tag(t) => write!(f, "tag={}", Encoded(t)),
            Facet::Owner(o) => write!(f, "owner={}", Encoded(o)),
            Facet::Player(p) => write!(f, "player={}", Encoded(p))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Seek {
    pub sort_by: SortBy,
    pub dir: Direction,
    pub anchor: Anchor,
    pub facets: Vec<Facet>
}

impl Default for Seek {
    fn default() -> Self {
        Seek {
            anchor: Anchor::Start,
            sort_by: SortBy::ProjectName,
            dir: Direction::Ascending,
            facets: vec![]
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SeekLink(String);

impl SeekLink {
    pub fn new(
        seek: &Seek,
        limit: Option<Limit>
    ) -> SeekLink
    {
        let Seek { sort_by, dir, anchor, facets } = seek;

        let mut qv = vec![
            format!("?sort_by={sort_by}&dir={dir}&anchor={anchor}")
        ];

        for f in facets {
            qv.push(f.to_string());
        }

        if let Some(l) = limit {
            qv.push(format!("limit={l}"));
        }

        SeekLink(qv.join("&"))
    }
}

impl fmt::Display for SeekLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Pagination {
    pub prev_page: Option<SeekLink>,
    pub next_page: Option<SeekLink>,
    pub total: i64
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn size_of_limit() {
        assert_eq!(
            std::mem::size_of::<Limit>(),
            std::mem::size_of::<u8>()
        );
    }

    #[test]
    fn string_to_limit_zero_err() {
        assert!(Limit::try_from("0").is_err());
    }

    #[test]
    fn string_to_limit_one_ok() {
        assert_eq!(
            Limit::try_from("1").unwrap(),
            Limit::new(1).unwrap()
        );
    }

    #[test]
    fn string_to_limit_one_hundred_ok() {
        assert_eq!(
            Limit::try_from("100").unwrap(),
            Limit::new(100).unwrap()
        );
    }

    #[test]
    fn string_to_limit_one_hundred_one_err() {
        assert!(Limit::try_from("101").is_err());
    }

    #[track_caller]
    fn assert_anchor_round_trip(a: Anchor) {
        assert_eq!(
            Anchor::try_from(
                urlencoding::decode(&a.to_string())
                    .unwrap()
                    .into_owned()
            ).unwrap(),
            a
        );
    }

    #[test]
    fn anchor_round_trip() {
        assert_anchor_round_trip(Anchor::Start);
        assert_anchor_round_trip(Anchor::Before("a".into(), 1));
        assert_anchor_round_trip(Anchor::After("a".into(), 1));
    }

    #[track_caller]
    fn assert_direction_round_trip(d: Direction) {
        assert_eq!(
            Direction::try_from(String::from(d).as_str()).unwrap(),
            d
        );
    }

    #[test]
    fn direction_round_trip() {
        assert_direction_round_trip(Direction::Ascending);
        assert_direction_round_trip(Direction::Descending);
    }

    #[test]
    fn direction_bad() {
        assert!(
            matches!(
                Direction::try_from("q").unwrap_err(),
                DirectionError(_)
            )
        );
    }

    #[test]
    fn direction_reverse() {
        assert_eq!(Direction::Ascending.rev(), Direction::Descending);
        assert_eq!(Direction::Descending.rev(), Direction::Ascending);
    }

    #[track_caller]
    fn assert_sort_by_round_trip(s: SortBy) {
        assert_eq!(
            SortBy::try_from(String::from(s).as_str()).unwrap(),
            s
        );
    }

    #[test]
    fn sort_by_round_trip() {
        assert_sort_by_round_trip(SortBy::ProjectName);
        assert_sort_by_round_trip(SortBy::GameTitle);
        assert_sort_by_round_trip(SortBy::ModificationTime);
        assert_sort_by_round_trip(SortBy::CreationTime);
        assert_sort_by_round_trip(SortBy::Relevance);
    }

    #[test]
    fn sort_by_bad() {
        assert!(
            matches!(
                SortBy::try_from("z").unwrap_err(),
                SortByError(_)
            )
        );
    }

    #[test]
    fn sort_by_default_direction() {
        assert_eq!(
            SortBy::ProjectName.default_direction(),
            Direction::Ascending
        );
        assert_eq!(
            SortBy::GameTitle.default_direction(),
            Direction::Ascending
        );
        assert_eq!(
            SortBy::ModificationTime.default_direction(),
            Direction::Descending
        );
        assert_eq!(
            SortBy::CreationTime.default_direction(),
            Direction::Descending
        );
        assert_eq!(
            SortBy::Relevance.default_direction(),
            Direction::Ascending
        );
    }
}
