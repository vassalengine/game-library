use glc::pagination::{Anchor, Facet, Limit, Direction, SortBy, Seek};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize, Eq, PartialEq)]
pub struct MaybeProjectsParams {
    pub q: Option<String>,
    pub from: Option<String>,
    pub sort_by: Option<SortBy>,
    pub dir: Option<Direction>,
    pub anchor: Option<Anchor>,
    pub limit: Option<Limit>,
    // facets
    pub publisher: Option<String>,
    pub year: Option<String>,
    pub players_min: Option<u32>,
    pub players_max: Option<u32>,
    #[serde(default)]
    pub players_inc: Vec<u32>,
    pub length_min: Option<u32>,
    pub length_max: Option<u32>,
    #[serde(default)]
    pub tag: Vec<String>,
    #[serde(default)]
    pub owner: Vec<String>,
    #[serde(default)]
    pub player: Vec<String>
}

#[derive(Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(try_from = "MaybeProjectsParams")]
pub struct ProjectsParams {
/*
    pub sort_by: Option<SortBy>,
    pub dir: Option<Direction>,
    pub anchor: Option<Anchor>,
*/
    pub seek: Seek,
    pub limit: Option<Limit>
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("invalid combination")]
    InvalidCombination,
}

impl TryFrom<MaybeProjectsParams> for ProjectsParams {
    type Error = Error;

    fn try_from(m: MaybeProjectsParams) -> Result<Self, Self::Error> {
        let MaybeProjectsParams {
            q,
            from,
            sort_by,
            dir,
            anchor,
            limit,
            publisher,
            year,
            players_min,
            players_max,
            players_inc,
            length_min,
            length_max,
            tag,
            owner,
            player
        } = m;

        if sort_by == Some(SortBy::Relevance) && q.is_none() {
            // Relevance requires a query
            return Err(Error::InvalidCombination);
        }

        // collect the facets
        let mut facets = Vec::with_capacity(
            (q.is_some() as usize) +
            (publisher.is_some() as usize) +
            (year.is_some() as usize) +
            (players_min.is_some() as usize) +
            (players_max.is_some() as usize) +
            (length_min.is_some() as usize) +
            (length_max.is_some() as usize) +
            players_inc.len() +
            tag.len() +
            owner.len() +
            player.len()
        );

        let has_query = match q {
            Some(q) => {
                facets.push(Facet::Query(q));
                true
            },
            None => false
        };

        if let Some(publisher) = publisher {
            facets.push(Facet::Publisher(publisher));
        }

        if let Some(year) = year {
            facets.push(Facet::Year(year));
        }

        if let Some(players_min) = players_min {
            facets.push(Facet::PlayersMin(players_min));
        }

        if let Some(players_max) = players_max {
            facets.push(Facet::PlayersMax(players_max));
        }

        if let Some(length_min) = length_min {
            facets.push(Facet::LengthMin(length_min));
        }

        if let Some(length_max) = length_max {
            facets.push(Facet::LengthMax(length_max));
        }

        facets.extend(players_inc.into_iter().map(Facet::PlayersInc));
        facets.extend(tag.into_iter().map(Facet::Tag));
        facets.extend(owner.into_iter().map(Facet::Owner));
        facets.extend(player.into_iter().map(Facet::Player));

        // assemble the Seek
        let seek = match (has_query, from, sort_by, dir, anchor) {
            // sort_by, dir, anchor
            (_, None, Some(sort_by), Some(dir), Some(anchor)) => Seek {
                sort_by,
                dir,
                anchor,
                facets
            },
            // query with optional sort_by, dir
            (true, None, sort_by, dir, None) => {
                let sort_by = sort_by.unwrap_or(SortBy::Relevance);
                let dir = dir.unwrap_or_else(|| sort_by.default_direction());
                Seek {
                    sort_by,
                    dir,
                    anchor: Anchor::Start,
                    facets
                }
            },
            // optional from, sort_by, dir
            (false, from, sort_by, dir, None) => {
                let sort_by = sort_by.unwrap_or_default();
                let dir = dir.unwrap_or_else(|| sort_by.default_direction());
                Seek {
                    sort_by,
                    dir,
                    anchor: match from {
                        // id 0 is unused and sorts before all other
                        // instances of the from string
                        Some(from) => Anchor::After(from, 0),
                        None => Anchor::Start
                    },
                    facets
                }
            },
            _ => return Err(Error::InvalidCombination)
        };

        Ok(ProjectsParams { seek, limit })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use axum_extra::extract::Query;
    use http::uri::Uri;

    #[test]
    fn maybe_projects_params_default_ok() {
        let mpp = MaybeProjectsParams::default();

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::default(),
                dir: SortBy::default().default_direction(),
                anchor: Anchor::Start,
                facets: vec![]
            },
            limit: None
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_from_and_q_invalid() {
        let mpp = MaybeProjectsParams {
            from: Some("whatever".into()),
            q: Some("whatever".into()),
            ..Default::default()
        };

        assert_eq!(
            ProjectsParams::try_from(mpp).unwrap_err(),
            Error::InvalidCombination
        );
    }

    #[test]
    fn maybe_projects_params_seek_ok() {
        let mpp = MaybeProjectsParams {
            sort_by: Some(SortBy::ProjectName),
            dir: Some(Direction::Ascending),
            anchor: Some(Anchor::Start),
            ..Default::default()
        };

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                anchor: Anchor::Start,
                facets: vec![]
            },
            limit: None
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_seek_incomplete() {
        let mpp = MaybeProjectsParams {
            sort_by: Some(SortBy::ProjectName),
            anchor: Some(Anchor::Start),
            ..Default::default()
        };

        assert_eq!(
            ProjectsParams::try_from(mpp).unwrap_err(),
            Error::InvalidCombination
        );
    }

    #[test]
    fn maybe_projects_params_mixed_invalid() {
        let mpp = MaybeProjectsParams {
            sort_by: Some(SortBy::ProjectName),
            anchor: Some(Anchor::Start),
            dir: Some(Direction::Ascending),
            from: Some("whatever".into()),
            ..Default::default()
        };

        assert_eq!(
            ProjectsParams::try_from(mpp).unwrap_err(),
            Error::InvalidCombination
        );
    }

    #[test]
    fn maybe_projects_params_sort_by_dir_ok() {
        let mpp = MaybeProjectsParams {
            sort_by: Some(SortBy::ProjectName),
            dir: Some(Direction::Ascending),
            ..Default::default()
        };

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::ProjectName,
                dir: Direction::Ascending,
                anchor: Anchor::Start,
                facets: vec![]
            },
            limit: None
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_from_ok() {
        let mpp = MaybeProjectsParams {
            from: Some("whatever".into()),
            ..Default::default()
        };

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::default(),
                dir: SortBy::default().default_direction(),
                anchor: Anchor::After("whatever".into(), 0),
                facets: vec![]
            },
            limit: None
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_limit_ok() {
        let mpp = MaybeProjectsParams {
            limit: Limit::new(50),
            ..Default::default()
        };

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::default(),
                dir: SortBy::default().default_direction(),
                anchor: Anchor::Start,
                facets: vec![]
            },
            limit: Limit::new(50)
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_publisher_ok() {
        let mpp = MaybeProjectsParams {
            publisher: Some("abc".into()),
            ..Default::default()
        };

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::default(),
                dir: SortBy::default().default_direction(),
                anchor: Anchor::Start,
                facets: vec![ Facet::Publisher("abc".into()) ]
            },
            limit: None
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_year_ok() {
        let mpp = MaybeProjectsParams {
            year: Some("1979".into()),
            ..Default::default()
        };

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::default(),
                dir: SortBy::default().default_direction(),
                anchor: Anchor::Start,
                facets: vec![ Facet::Year("1979".into()) ]
            },
            limit: None
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_tag_ok() {
        let mpp = MaybeProjectsParams {
            tag: vec![ "x".into(), "y".into() ],
            ..Default::default()
        };

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::default(),
                dir: SortBy::default().default_direction(),
                anchor: Anchor::Start,
                facets: vec![
                    Facet::Tag("x".into()),
                    Facet::Tag("y".into())
                ]
            },
            limit: None
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_owner_ok() {
        let mpp = MaybeProjectsParams {
            owner: vec![ "x".into(), "y".into() ],
            ..Default::default()
        };

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::default(),
                dir: SortBy::default().default_direction(),
                anchor: Anchor::Start,
                facets: vec![
                    Facet::Owner("x".into()),
                    Facet::Owner("y".into())
                ]
            },
            limit: None
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_player_ok() {
        let mpp = MaybeProjectsParams {
            player: vec![ "x".into(), "y".into() ],
            ..Default::default()
        };

        let pp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::default(),
                dir: SortBy::default().default_direction(),
                anchor: Anchor::Start,
                facets: vec![
                    Facet::Player("x".into()),
                    Facet::Player("y".into())
                ]
            },
            limit: None
        };

        assert_eq!(ProjectsParams::try_from(mpp).unwrap(), pp);
    }

    #[test]
    fn maybe_projects_params_sort_by_dir_anchor_query_ok() {
        let uri: Uri = "http://example.com?sort_by=t&dir=a&anchor=a%09battle+for+fallujah%3A+april+2004%09446&q=Battle&limit=50".parse().unwrap();

        let exp = ProjectsParams {
            seek: Seek {
                sort_by: SortBy::GameTitle,
                dir: Direction::Ascending,
                anchor: Anchor::After("battle for fallujah: april 2004".into(), 446),
                facets: vec![Facet::Query("Battle".into())]
            },
            limit: Limit::new(50)
        };

        let Query(act): Query<ProjectsParams> = Query::try_from_uri(&uri)
            .unwrap();

        assert_eq!(act, exp);
    }
}
