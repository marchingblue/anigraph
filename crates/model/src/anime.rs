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

/// Per-episode metadata.
///
/// Source is TMDB (`/tv/{id}/season/{n}`).  All fields except `number` are
/// optional and omitted when absent — never serialized as empty strings or
/// nulls.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// Runtime in minutes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<i32>,
    /// Episode synopsis / description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overview: Option<String>,
    /// Community rating for this episode (e.g. TMDB vote average).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rating: Option<EpisodeRating>,
    /// Episode still/thumbnail image URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ids: Option<EpisodeIds>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeTitle {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub english: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub romaji: Option<String>,
}

/// Per-episode community rating.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeRating {
    /// Average score (TMDB vote_average / AniDB rating), 0–10 scale.
    pub average: f64,
    /// Number of votes contributing to the average, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeIds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tvdb: Option<i32>,
    /// TMDB episode id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmdb: Option<i32>,
    /// AniDB episode id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anidb: Option<i32>,
}
