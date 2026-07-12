use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;

use crate::checkpoint::{Checkpoint, PhaseId};

/// Configuration for a single pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Root working directory for intermediate files.
    pub work_dir: PathBuf,
    /// Final output directory.
    pub output_dir: PathBuf,
    /// Path to the checkpoint file.
    pub checkpoint_path: PathBuf,
    /// Path to the Fribb/anime-lists JSON file.
    /// If set, skips the automatic download and uses this local file instead.
    pub fribb_path: Option<PathBuf>,
    /// Whether to resume from existing checkpoint.
    pub resume: bool,
    /// Skip enumeration phases (assumes anime-base.jsonl and manga-base.jsonl
    /// already exist) and start directly from cross-reference + enrichment.
    pub skip_enumeration: bool,
    /// Skip TMDB enrichment phase.  Useful for testing TVDB independently.
    pub skip_tmdb: bool,
    /// TVDB API key (optional, skips TVDB phase if absent).
    pub tvdb_api_key: Option<String>,
    /// TMDB API key (optional, skips TMDB phase if absent).
    pub tmdb_api_key: Option<String>,
    /// TVDB requests-per-second limit.  Defaults to 2 (empirically tested).
    pub tvdb_rate_limit: f64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            work_dir: PathBuf::from("tmp"),
            output_dir: PathBuf::from("."),
            checkpoint_path: PathBuf::from("tmp/anigraph-checkpoint.json"),
            fribb_path: None, // Auto-download from GitHub
            resume: false,
            skip_enumeration: false,
            skip_tmdb: false,
            tvdb_api_key: None,
            tmdb_api_key: None,
            tvdb_rate_limit: 15.0,
        }
    }
}

/// A single pipeline phase.
///
/// Each phase is an independently runnable unit with its own input/output,
/// checkpoint state, and progress reporting.
#[async_trait]
pub trait Phase {
    /// The unique identifier for this phase (matches checkpoint keys).
    fn id(&self) -> PhaseId;
    /// A human-readable name for progress bars and logging.
    fn name(&self) -> &'static str;
    /// Run this phase. Returns the total number of items processed.
    async fn run(&self, config: &PipelineConfig, checkpoint: &mut Checkpoint) -> Result<u64>;
}
