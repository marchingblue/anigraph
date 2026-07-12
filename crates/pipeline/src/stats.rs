/// Pipeline statistics — per-phase metrics aggregated into a final report.
///
/// These stats are produced by each phase during `run()` and merged into a
/// single [`PipelineStats`] at the end of the pipeline. The stats are
/// serialized to `stats.json` alongside the dataset release — they are
/// separate from both the checkpoint (resume state) and the JSONL schema
/// (per-entry data).
///
/// # Adding new metrics
///
/// Add a field to the relevant `XxxPhaseStats` struct, populate it in the
/// phase's `run()` method, and merge it into [`PipelineStats::merge()`].
use serde::{Deserialize, Serialize};

/// Aggregate stats for the entire pipeline run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineStats {
    pub generated_at: String,

    pub anilist_anime: AnilistPhaseStats,
    pub anilist_manga: AnilistPhaseStats,
    pub fribb_crossref: CrossrefPhaseStats,
    pub tmdb_enrich: TmdbPhaseStats,
    pub tvdb_enrich: TvdbPhaseStats,
}

impl PipelineStats {
    pub fn new() -> Self {
        Self {
            generated_at: chrono::Utc::now().to_rfc3339(),
            ..Default::default()
        }
    }
}

// ── AniList enumeration ───────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnilistPhaseStats {
    pub total_entries: u64,
}

// ── Fribb cross-reference ─────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossrefPhaseStats {
    pub total_input: u64,
    pub matched: u64,
    pub unmatched: u64,
    pub unmatched_percent: f64,
}

// ── TMDB enrichment ───────────────────────────────────────────────────────

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

impl TmdbPhaseStats {
    pub fn merge(&mut self, other: &Self) {
        self.total_input += other.total_input;
        self.with_tv_ids += other.with_tv_ids;
        self.with_movie_ids += other.with_movie_ids;
        self.artwork_found += other.artwork_found;
        self.no_artwork += other.no_artwork;
        self.failures += other.failures;
        self.total_poster_count += other.total_poster_count;
        self.total_backdrop_count += other.total_backdrop_count;
        self.total_logo_count += other.total_logo_count;
    }
}

// ── TVDB enrichment ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TvdbPhaseStats {
    pub total_input: u64,
    pub with_ids: u64,
    pub episodes_found: u64,
    pub total_episodes: u64,
    pub failures: u64,
}
