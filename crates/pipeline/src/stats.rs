//! Internal per-phase counters used for progress logging.
//!
//! These are **not** persisted anywhere — each phase logs its counters when
//! it finishes and that is the only consumption.  (The old `stats.json`
//! output was removed: its numbers were unreliable and nobody used them.)
use serde::{Deserialize, Serialize};

// ── TMDB enrichment ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TmdbPhaseStats {
    pub total_input: u64,
    pub with_tv_ids: u64,
    pub with_movie_ids: u64,
    pub artwork_found: u64,
    pub no_artwork: u64,
    pub failures: u64,
    pub total_poster_count: u64,
    pub total_backdrop_count: u64,
    pub total_logo_count: u64,
}

// ── TVDB enrichment ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvdbPhaseStats {
    pub total_input: u64,
    pub with_ids: u64,
    pub failures: u64,
}
