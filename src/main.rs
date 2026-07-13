use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use anigraph_pipeline::phase::PipelineConfig;
use anigraph_pipeline::runner::run_pipeline;

/// anigraph — open-source anime/manga metadata dataset generator.
///
/// Runs the full generation pipeline: AniList enumeration, Fribb
/// cross-reference, TMDB enrichment, and TVDB enrichment.
///
/// All intermediate files are written to --work-dir.  The final dataset
/// is written to --output-dir once the output phase is implemented.
#[derive(Parser, Debug)]
#[command(name = "anigraph", version, about)]
struct Cli {
    /// Working directory for intermediate files.
    #[arg(long, default_value = "data")]
    work_dir: PathBuf,

    /// Output directory for the final dataset.
    #[arg(long, default_value = "data")]
    output_dir: PathBuf,

    /// Resume from the last checkpoint (requires checkpoint file).
    #[arg(long)]
    resume: bool,

    /// Skip animation/manga enumeration.  Assumes anime-base.jsonl and
    /// manga-base.jsonl exist in --work-dir and starts from cross-ref.
    #[arg(long)]
    skip_enumeration: bool,

    /// Skip TMDB enrichment phase.  Useful for testing TVDB independently.
    #[arg(long)]
    skip_tmdb: bool,

    /// Path to a local Fribb/anime-lists JSON file.
    /// If not set, the file is downloaded automatically.
    #[arg(long)]
    fribb_path: Option<PathBuf>,

    /// TVDB requests-per-second limit.
    /// Default 15 req/s (50% of tested 30 req/s ceiling).
    #[arg(long, default_value = "15.0")]
    tvdb_rate: f64,
}

fn main() -> Result<()> {
    // ── Init tracing ───────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    // ── Load .env file (silently ignore if not present) ────────────────
    if let Err(e) = dotenvy::dotenv() {
        tracing::debug!("No .env file: {e}");
    }

    // ── Parse CLI ─────────────────────────────────────────────────────
    let cli = Cli::parse();

    // ── Build config ──────────────────────────────────────────────────
    let work_dir = cli.work_dir.clone();
    let config = PipelineConfig {
        work_dir: cli.work_dir,
        output_dir: cli.output_dir,
        checkpoint_path: work_dir.join("anigraph-checkpoint.json"),
        fribb_path: cli.fribb_path,
        resume: cli.resume,
        skip_enumeration: cli.skip_enumeration,
        tmdb_api_key: std::env::var("TMDB_READ_KEY")
            .ok()
            .or_else(|| std::env::var("TMDB_API_KEY").ok())
            .map(|k| k.trim().to_string()),
        tvdb_api_key: std::env::var("TVDB_API_KEY")
            .ok()
            .or_else(|| std::env::var("THETVDB_KEY").ok())
            .map(|k| k.trim().to_string()),
        skip_tmdb: cli.skip_tmdb,
        tvdb_rate_limit: cli.tvdb_rate,
    };

    // Validate required config
    if config.tmdb_api_key.is_none() {
        tracing::warn!("TMDB_API_KEY not set in environment — TMDB enrichment will be skipped");
    }
    if config.tvdb_api_key.is_none() {
        tracing::warn!("TVDB_API_KEY not set in environment — TVDB enrichment will be skipped");
    }

    // ── Run pipeline ──────────────────────────────────────────────────
    tracing::info!("anigraph v{}", env!("CARGO_PKG_VERSION"));
    tracing::info!("Work dir: {}", config.work_dir.display());
    tracing::info!("Resume:   {}", config.resume);
    tracing::info!("Skip enum: {}", config.skip_enumeration);

    let runtime = tokio::runtime::Runtime::new()?;
    let stats = runtime.block_on(run_pipeline(&config))?;

    tracing::info!("Pipeline complete. Session: {}", stats.generated_at);

    Ok(())
}
