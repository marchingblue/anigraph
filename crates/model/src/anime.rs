use serde::{Deserialize, Serialize};

use crate::shared::*;

/// A single anime entry in the anigraph dataset.
///
/// All fields map 1:1 to `schemas/schema-anime.json`. Optional fields are
/// filled phase-by-phase during the pipeline — consumers see only the final
/// enriched entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnimeEntry {
    // ── Identity ──────────────────────────────────────────────────────────
    pub id: i32,
    pub r#type: EntryType,
    pub sources: Vec<String>,

    // ── Graph ────────────────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ids: Option<CrossIds>,

    // ── Titles ───────────────────────────────────────────────────────────
    pub titles: Title,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synonyms: Option<Vec<String>>,

    // ── Content ──────────────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub format: AnimeFormat,
    pub episodes_count: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<i32>,
    pub status: MediaStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<MediaSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_rating: Option<AgeRating>,

    // ── Temporal ─────────────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub season: Option<SeasonInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dates: Option<DateRange>,

    // ── Classification ───────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub genres: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,

    // ── Credits ──────────────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub studios: Option<Vec<Studio>>,
    /// Creative authors. For anime, this captures "Original Creator" and
    /// "Original Story" credits (e.g., Hajime Yatate for Cowboy Bebop,
    /// Kazuma Kamachi for Railgun). Intentionally included — these are the
    /// people who *created* the work being adapted, not the anime staff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authors: Option<Vec<Author>>,

    // ── Scores ───────────────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<Score>,

    // ── Visuals ──────────────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artwork: Option<Vec<Artwork>>,

    // ── Graph ────────────────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relations: Option<Vec<Relation>>,

    // ── Episodes ─────────────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episodes: Option<Vec<Episode>>,
}

/// Per-episode metadata, sourced from TVDB API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Episode {
    pub number: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub absolute: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub season_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub titles: Option<EpisodeTitle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub air_date: Option<FuzzyDate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<i32>,
    /// Episode synopsis / description from TVDB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ids: Option<EpisodeIds>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeTitle {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub english: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub romaji: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeIds {
    pub tvdb: i32,
}
