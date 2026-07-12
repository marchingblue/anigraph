use serde::{Deserialize, Serialize};

use crate::shared::*;

/// A single manga entry in the anigraph dataset.
///
/// Manga is a strict subset of the anime schema:
/// - No `episodes`, `duration`, `season`, or `studios`
/// - Has `chapters_count` and `volumes_count` instead of `episodes_count`
/// - `authors` captures primary creators (Story & Art, Story, Art, Original Creator)
/// - `age_rating` is present (from AniList `isAdult`, same mapping as anime)
/// - `artwork` is AniList-only (POSTER from anilist provider)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MangaEntry {
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
    pub format: MangaFormat,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chapters_count: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes_count: Option<i32>,
    pub status: MediaStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<MediaSource>,
    /// Present for manga — from AniList `isAdult`, same mapping as anime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_rating: Option<AgeRating>,

    // ── Temporal ─────────────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dates: Option<DateRange>,

    // ── Classification ───────────────────────────────────────────────────
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub genres: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,

    // ── Credits ──────────────────────────────────────────────────────────
    /// Primary creators. For manga this captures Story & Art, Story, Art,
    /// and Original Creator (e.g., Gege Akutami for Jujutsu Kaisen).
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
}
