use std::collections::HashMap;
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identifies a pipeline phase.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseId {
    AnilistAnime,
    AnilistManga,
    FribbCrossref,
    TvdbEnrich,
    TmdbEnrich,
    Output,
}

impl std::fmt::Display for PhaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AnilistAnime => write!(f, "anilist_anime"),
            Self::AnilistManga => write!(f, "anilist_manga"),
            Self::FribbCrossref => write!(f, "fribb_crossref"),
            Self::TvdbEnrich => write!(f, "tvdb_enrich"),
            Self::TmdbEnrich => write!(f, "tmdb_enrich"),
            Self::Output => write!(f, "output"),
        }
    }
}

/// Overall status of a phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    /// Phase has not been started yet.
    Pending,
    /// Phase is currently running.
    InProgress,
    /// Phase finished successfully.
    Completed,
    /// Phase failed permanently (won't retry automatically).
    Failed(String),
}

/// Per-phase progress state for **paginated** enumeration phases.
///
/// # Resume correctness
///
/// Checkpoints are written after **every completed page** (`complete_page`),
/// so the maximum re-fetch on a crash is a single page. The resume cursor is
/// tracked with two fields:
///
/// - `next_window_start`: the start ID of the AniList window that contains the
///   next page to fetch. Enumeration walks the ID space in fixed `id_in`
///   windows to dodge AniList's ~5,000-entry `Page` depth limit, so a page
///   number alone is not enough — we also need to know *which window* it lives
///   in.
/// - `current_inner_page`: the 1-based page number *within* that window to
///   resume at.
///
/// On resume the output file is truncated to `last_byte_offset` (the exact
/// byte position after the last fully written page) and fetching continues at
/// `(next_window_start, current_inner_page)`.
///
/// This is safe because:
/// - AniList `sort: ID` pagination is stable — page N always yields the same
///   entries in the same order, even if new entries are added with higher IDs.
/// - Truncating to a clean page boundary means refetched pages never
///   duplicate entries already in the file.
/// - The writer flushes *every* page, so `last_byte_offset` is always an
///   accurate on-disk position even if a crash happens between a flush and the
///   checkpoint save.
///
/// **Worst case on crash:** At most one page (~50 items) of duplicated work
/// (refetched, idempotent), never data loss or silent truncation.
///
/// **If a phase is ever parallelized**, this page-based approach becomes
/// unsound because pages may complete out of order. Parallel phases should
/// use a `completed_ids: HashSet<u64>` set and avoid shared file I/O.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedState {
    pub status: PhaseStatus,

    /// Total pages to fetch (estimated when phase begins).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_pages: Option<u64>,

    /// The last page number that was successfully written to the output file.
    /// On resume, the output is truncated to `last_completed_page × per_page`
    /// lines, and fetching resumes at `last_completed_page + 1`.
    #[serde(default)]
    pub last_completed_page: u64,

    /// Items per page (typically 50 for AniList).
    #[serde(default = "default_per_page")]
    pub per_page: u32,

    /// Total items successfully processed (informational).
    #[serde(default)]
    pub items_written: u64,

    /// Exact byte offset in the output file after completing this page.
    /// Used by the JSONL writer to truncate to a precise position on resume.
    #[serde(default)]
    pub last_byte_offset: u64,

    /// IDs of entries that failed permanently (logged, won't block resume).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_item_ids: Vec<u64>,

    /// Path to the intermediate output file for this phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<PathBuf>,

    /// Per-second request limit for phases that make HTTP calls.
    /// Configurable so it can be tuned without recompiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<f32>,

    /// For enumeration phases: the start AniList ID of the window that
    /// contains the next page to fetch. Enumeration walks the ID space in
    /// fixed-size `id_in` windows to work around AniList's ~5,000-entry
    /// `Page` depth limit, so the resume cursor pairs this with
    /// `current_inner_page` (the page *within* the window) rather than a bare
    /// page number.
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "id_greater_than")]
    pub next_window_start: Option<i32>,

    /// For enumeration phases: the 1-based page number *within*
    /// `next_window_start`'s window to resume at. `None` means "start from the
    /// first page of `next_window_start`".
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "inner_page")]
    pub current_inner_page: Option<u32>,
}

const fn default_per_page() -> u32 {
    50
}

impl PaginatedState {
    pub fn new(total_pages: u64, per_page: u32, rate_limit: Option<f32>) -> Self {
        Self {
            status: PhaseStatus::Pending,
            total_pages: Some(total_pages),
            last_completed_page: 0,
            per_page,
            items_written: 0,
            last_byte_offset: 0,
            failed_item_ids: Vec::new(),
            output: None,
            rate_limit,
            next_window_start: None,
            current_inner_page: None,
        }
    }

    /// The number of lines the output file should contain for a clean resume.
    /// Uses `items_written` rather than `last_completed_page * per_page` to
    /// correctly handle the last page when it has fewer than `per_page` items.
    pub fn expected_line_count(&self) -> u64 {
        self.items_written
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            PhaseStatus::Completed | PhaseStatus::Failed(_)
        )
    }
}

/// Per-phase progress state for **non-paginated** phases (cross-ref, output).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleState {
    pub status: PhaseStatus,
    #[serde(default)]
    pub completed: u64,
    #[serde(default)]
    pub total: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<PathBuf>,
}

impl SimpleState {
    pub fn new(total: u64) -> Self {
        Self {
            status: PhaseStatus::Pending,
            completed: 0,
            total,
            output: None,
        }
    }
}

/// Union of phase state types.
///
/// Uses explicit internal tagging so each variant is routed correctly
/// on deserialization. The `#[serde(untagged)]` approach was tried but
/// failed because `PaginatedState` has defaults on all optional fields,
/// causing it to greedily match ANY JSON object — `SimpleState` could
/// never deserialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase_type")]
pub enum PhaseState {
    #[serde(rename = "paginated")]
    Paginated(PaginatedState),
    #[serde(rename = "simple")]
    Simple(SimpleState),
}

/// The checkpoint file: a JSON document recording pipeline progress.
///
/// Written atomically (write to `.tmp`, rename) so a crash mid-write
/// never corrupts the last good checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub schema_version: u32,
    pub session_id: String,
    pub started_at: String,
    pub updated_at: String,
    pub phases: HashMap<PhaseId, PhaseState>,
}

impl Checkpoint {
    /// Create a fresh checkpoint for a new pipeline run.
    pub fn new() -> Self {
        let now = Utc::now().to_rfc3339();
        Self {
            schema_version: 1,
            session_id: Uuid::new_v4().to_string(),
            started_at: now.clone(),
            updated_at: now,
            phases: HashMap::new(),
        }
    }

    /// Load an existing checkpoint from disk.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let ckpt: Self = serde_json::from_str(&raw)?;
        Ok(ckpt)
    }

    /// Save checkpoint atomically.
    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let tmp = path.with_extension("json.tmp");
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, path)?; // atomic on same filesystem
        Ok(())
    }

    /// Get a mutable reference to a paginated phase state.
    /// Panics if the phase doesn't exist or isn't paginated.
    pub fn paginated_mut(&mut self, id: &PhaseId) -> &mut PaginatedState {
        match self.phases.get_mut(id) {
            Some(PhaseState::Paginated(s)) => s,
            _ => panic!("phase {id} is not paginated or not initialized"),
        }
    }

    /// Get a reference to a paginated phase state.
    pub fn paginated(&self, id: &PhaseId) -> Option<&PaginatedState> {
        match self.phases.get(id) {
            Some(PhaseState::Paginated(s)) => Some(s),
            _ => None,
        }
    }

    /// Get a mutable reference to a simple phase state.
    pub fn simple_mut(&mut self, id: &PhaseId) -> &mut SimpleState {
        match self.phases.get_mut(id) {
            Some(PhaseState::Simple(s)) => s,
            _ => panic!("phase {id} is not simple or not initialized"),
        }
    }

    /// Mark a paginated phase as in-progress (for enumeration phases).
    pub fn begin_paginated(
        &mut self,
        id: &PhaseId,
        total_pages: u64,
        per_page: u32,
        rate_limit: Option<f32>,
    ) {
        let state = self
            .phases
            .entry(id.clone())
            .or_insert_with(|| PhaseState::Paginated(PaginatedState::new(total_pages, per_page, rate_limit)));
        if let PhaseState::Paginated(s) = state {
            s.status = PhaseStatus::InProgress;
            s.total_pages = Some(total_pages);
            s.rate_limit = rate_limit;
        }
        self.updated_at = Utc::now().to_rfc3339();
    }

    /// Record that a page completed successfully.
    /// `byte_offset` is the exact position in the output file after writing
    /// this page's entries — used for precise truncation on resume.
    pub fn complete_page(&mut self, id: &PhaseId, items_in_page: u64, byte_offset: u64) {
        let state = self.paginated_mut(id);
        state.last_completed_page += 1;
        state.items_written += items_in_page;
        state.last_byte_offset = byte_offset;
        self.updated_at = Utc::now().to_rfc3339();
    }

    /// Set the resume cursor for enumeration phases.
    ///
    /// `window_start` is the start AniList ID of the window containing the next
    /// page to fetch, and `inner_page` is the 1-based page number *within*
    /// that window. This pairs `(next_window_start, current_inner_page)` so a
    /// resume knows both *which window* and *which page in it* to continue
    /// from — a bare page number is ambiguous because enumeration walks the ID
    /// space in fixed windows.
    pub fn set_window_cursor(&mut self, id: &PhaseId, window_start: i32, inner_page: u32) {
        if let Some(PhaseState::Paginated(s)) = self.phases.get_mut(id) {
            s.next_window_start = Some(window_start);
            s.current_inner_page = Some(inner_page);
        }
        self.updated_at = Utc::now().to_rfc3339();
    }

    /// Record a permanently failed entry ID (for audit, not resume).
    pub fn record_failure(&mut self, id: &PhaseId, entry_id: u64) {
        let state = self.paginated_mut(id);
        state.failed_item_ids.push(entry_id);
        self.updated_at = Utc::now().to_rfc3339();
    }

    /// Mark a paginated phase as completed.
    pub fn complete_paginated(&mut self, id: &PhaseId) {
        if let Some(state) = self.phases.get_mut(id)
            && let PhaseState::Paginated(s) = state {
                s.status = PhaseStatus::Completed;
            }
        self.updated_at = Utc::now().to_rfc3339();
    }

    /// Mark a simple phase as completed.
    pub fn complete_simple(&mut self, id: &PhaseId) {
        if let Some(state) = self.phases.get_mut(id)
            && let PhaseState::Simple(s) = state {
                s.status = PhaseStatus::Completed;
            }
        self.updated_at = Utc::now().to_rfc3339();
    }

    /// Mark any phase as failed.
    pub fn fail_phase(&mut self, id: &PhaseId, error: &str) {
        if let Some(state) = self.phases.get_mut(id) {
            match state {
                PhaseState::Paginated(s) => s.status = PhaseStatus::Failed(error.to_string()),
                PhaseState::Simple(s) => s.status = PhaseStatus::Failed(error.to_string()),
            }
        }
        self.updated_at = Utc::now().to_rfc3339();
    }

    /// Check if a phase is completed (should be skipped on resume).
    pub fn is_completed(&self, id: &PhaseId) -> bool {
        self.phases.get(id).is_some_and(|s| match s {
            PhaseState::Paginated(s) => matches!(s.status, PhaseStatus::Completed),
            PhaseState::Simple(s) => matches!(s.status, PhaseStatus::Completed),
        })
    }

    /// Get the next page number to fetch for a paginated phase.
    /// Pages are 1-indexed. Returns 1 if nothing was completed yet.
    pub fn next_page(&self, id: &PhaseId) -> u64 {
        self.paginated(id)
            .map(|s| s.last_completed_page + 1)
            .unwrap_or(1)
    }

    /// Get the byte offset for truncation on resume.
    /// Returns the exact byte position of the last completed page boundary.
    pub fn resume_byte_offset(&self, id: &PhaseId) -> u64 {
        self.paginated(id).map(|s| s.last_byte_offset).unwrap_or(0)
    }

    /// Number of items written so far for a paginated phase.
    pub fn items_written(&self, id: &PhaseId) -> u64 {
        self.paginated(id).map(|s| s.items_written).unwrap_or(0)
    }

    /// Get the expected line count for truncation on resume (informational).
    pub fn expected_line_count(&self, id: &PhaseId) -> u64 {
        self.paginated(id)
            .map(|s| s.expected_line_count())
            .unwrap_or(0)
    }

    /// Get the per-page size.
    pub fn per_page(&self, id: &PhaseId) -> u32 {
        self.paginated(id).map(|s| s.per_page).unwrap_or(50)
    }
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paginated_roundtrip() {
        let mut ckpt = Checkpoint::new();
        let phase = PhaseId::AnilistAnime;

        ckpt.begin_paginated(&phase, 300, 50, Some(25.0));

        // Simulate completing 10 pages
        for i in 0..10u64 {
            let byte_off = (i + 1) * 5000; // simulated byte offset after each page
            ckpt.complete_page(&phase, 50, byte_off);
        }
        ckpt.record_failure(&phase, 42);

        assert_eq!(ckpt.next_page(&phase), 11);
        assert_eq!(ckpt.expected_line_count(&phase), 500);
        assert_eq!(ckpt.resume_byte_offset(&phase), 50000);
        assert!(!ckpt.is_completed(&phase));

        ckpt.complete_paginated(&phase);
        assert!(ckpt.is_completed(&phase));

        // Round-trip through serialization
        let json = serde_json::to_string_pretty(&ckpt).unwrap();
        let restored: Checkpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.next_page(&phase), 11);
        assert!(restored.is_completed(&phase));
    }

    #[test]
    fn test_simple_roundtrip() {
        let mut ckpt = Checkpoint::new();
        let phase = PhaseId::FribbCrossref;

        ckpt.phases.insert(
            phase.clone(),
            PhaseState::Simple(SimpleState::new(7170)),
        );

        let json = serde_json::to_string_pretty(&ckpt).unwrap();
        let restored: Checkpoint = serde_json::from_str(&json).unwrap();
        match restored.phases.get(&phase).unwrap() {
            PhaseState::Simple(s) => assert_eq!(s.total, 7170),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_atomic_save_load() {
        let dir = std::env::temp_dir().join("anigraph-ckpt-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("checkpoint.json");

        let mut ckpt = Checkpoint::new();
        ckpt.begin_paginated(&PhaseId::AnilistAnime, 300, 50, None);
        ckpt.complete_page(&PhaseId::AnilistAnime, 50, 5000);
        ckpt.complete_page(&PhaseId::AnilistAnime, 50, 10000);
        ckpt.complete_paginated(&PhaseId::AnilistAnime);
        ckpt.save(&path).unwrap();

        let loaded = Checkpoint::load(&path).unwrap();
        assert!(loaded.is_completed(&PhaseId::AnilistAnime));
        assert_eq!(loaded.next_page(&PhaseId::AnilistAnime), 3);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_resume_page_boundary_safe() {
        // Simulate: completed pages 1-10 (500 items), crashed mid-page-11
        // On resume: next_page = 11, line_count = 500
        // Output truncated to 500 lines, page 11 refetched cleanly
        let mut ckpt = Checkpoint::new();
        ckpt.begin_paginated(&PhaseId::AnilistAnime, 300, 50, None);
        for i in 0..10u64 {
            let byte_off = (i + 1) * 5000;
            ckpt.complete_page(&PhaseId::AnilistAnime, 50, byte_off);
        }

        assert_eq!(ckpt.next_page(&PhaseId::AnilistAnime), 11);
        assert_eq!(ckpt.expected_line_count(&PhaseId::AnilistAnime), 500);
        assert_eq!(ckpt.resume_byte_offset(&PhaseId::AnilistAnime), 50000);

        // Resume: truncate output to byte offset 50000, start from page 11
        // Page 11 returns items 501-550 (idempotent with sort:ID)
        // No duplicates, no gaps
    }
}
