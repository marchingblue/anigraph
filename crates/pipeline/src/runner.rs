use anyhow::{Context, Result};

use crate::checkpoint::{Checkpoint, PhaseId};
use crate::crossref::CrossRefPhase;
use crate::enumerate::{EnumeratePhase, MediaType};
use crate::output::OutputPhase;
use crate::phase::{Phase, PipelineConfig};
use crate::stats::{AnilistPhaseStats, CrossrefPhaseStats, PipelineStats, TmdbPhaseStats, TvdbPhaseStats};
use crate::tmdb::TmdbEnrichPhase;
use crate::tvdb::TvdbEnrichPhase;

/// Run the full pipeline: enumerate → cross-ref → TMDB → TVDB.
///
/// Each phase runs to completion (or is skipped if already completed).
/// The checkpoint is saved after every phase so a crash mid-pipeline
/// only loses the in-flight phase's progress.
///
/// Returns aggregate stats for the entire run.
pub async fn run_pipeline(config: &PipelineConfig) -> Result<PipelineStats> {
    let mut stats = PipelineStats::new();

    // ── Single-instance lock ────────────────────────────────────────
    // Prevent two runs (e.g. an orphaned child from a killed `cargo run`
    // racing a fresh `--resume`) from writing the same work dir.
    #[cfg(unix)]
    let _lock = crate::lock::acquire(&config.work_dir)?;

    // ── Load or create checkpoint ──────────────────────────────────────
    std::fs::create_dir_all(&config.work_dir)
        .context("creating work directory")?;

    let mut checkpoint = if config.resume && config.checkpoint_path.exists() {
        tracing::info!("Loading checkpoint from {}", config.checkpoint_path.display());
        Checkpoint::load(&config.checkpoint_path)
            .context("loading checkpoint")?
    } else if config.resume {
        anyhow::bail!(
            "resume requested but checkpoint not found at {}",
            config.checkpoint_path.display()
        );
    } else {
        Checkpoint::new()
    };

    // ── Phase 1: Anime enumeration (skippable) ─────────────────────────
    if config.skip_enumeration {
        let anime_path = config.work_dir.join("anime-base.jsonl");
        let manga_path = config.work_dir.join("manga-base.jsonl");

        if !anime_path.exists() {
            anyhow::bail!(
                "--skip-enumeration but anime-base.jsonl not found at {}",
                anime_path.display()
            );
        }
        if !manga_path.exists() {
            anyhow::bail!(
                "--skip-enumeration but manga-base.jsonl not found at {}",
                manga_path.display()
            );
        }

        // Mark enumeration as completed so the phases are skipped
        for (phase_id, path) in [
            (PhaseId::AnilistAnime, &anime_path),
            (PhaseId::AnilistManga, &manga_path),
        ] {
            let count = count_jsonl_lines(path)
                .context(format!("counting lines in {}", path.display()))?;
            checkpoint.phases.insert(
                phase_id.clone(),
                crate::checkpoint::PhaseState::Simple(
                    crate::checkpoint::SimpleState::new(count),
                ),
            );
            // Set completed count so stats reflect the actual line count
            if let crate::checkpoint::PhaseState::Simple(s) =
                checkpoint.phases.get_mut(&phase_id).unwrap()
            {
                s.completed = count;
            }
            checkpoint.complete_simple(&phase_id);
            tracing::info!("Enumeration skipped: {} ({count} entries)", path.display());
        }

        checkpoint.save(&config.checkpoint_path)
            .context("saving checkpoint after marking enumeration skipped")?;
    }

    // ── Anime enumeration ──────────────────────────────────────────────
    run_phase(
        &EnumeratePhase {
            media_type: MediaType::Anime,
        },
        config,
        &mut checkpoint,
    )
    .await?;

    // ── Manga enumeration ──────────────────────────────────────────────
    run_phase(
        &EnumeratePhase {
            media_type: MediaType::Manga,
        },
        config,
        &mut checkpoint,
    )
    .await?;

    // ── Phase 3: Fribb cross-reference ─────────────────────────────────
    run_phase(&CrossRefPhase, config, &mut checkpoint).await?;

    // ── Phase 4: TMDB enrichment (only if API key is set and not skipped) ─
    if config.skip_tmdb {
        tracing::info!("--skip-tmdb set — skipping TMDB enrichment");
    } else if config.tmdb_api_key.is_some() {
        run_phase(&TmdbEnrichPhase, config, &mut checkpoint).await?;
    } else {
        tracing::info!("TMDB_API_KEY not set — skipping TMDB enrichment");
    }

    // ── Phase 5: TVDB enrichment (only if API key is set) ──────────────
    if config.tvdb_api_key.is_some() {
        run_phase(&TvdbEnrichPhase, config, &mut checkpoint).await?;
    } else {
        tracing::info!("TVDB_API_KEY not set — skipping TVDB enrichment");
    }

    // ── Phase 6: Output (compress + checksum) ─────────────────────────
    run_phase(&OutputPhase, config, &mut checkpoint).await?;

    // ── Build stats from checkpoint ───────────────────────────────────
    build_stats(&checkpoint, &mut stats);

    // ── Write stats.json ───────────────────────────────────────────────
    let stats_path = config.output_dir.join("stats.json");
    let stats_json = serde_json::to_string_pretty(&stats)
        .context("serializing pipeline stats")?;
    std::fs::write(&stats_path, &stats_json)
        .context(format!("writing stats to {}", stats_path.display()))?;
    tracing::info!("Stats written to {}", stats_path.display());

    // ── Final summary ─────────────────────────────────────────────────
    summarize(&checkpoint);

    Ok(stats)
}

/// Run a single phase with progress logging and checkpoint saving.
async fn run_phase(
    phase: &dyn Phase,
    config: &PipelineConfig,
    checkpoint: &mut Checkpoint,
) -> Result<u64> {
    let name = phase.name();
    let phase_id = phase.id();

    // Check if already completed
    if checkpoint.is_completed(&phase_id) {
        let prev = match checkpoint.phases.get(&phase_id) {
            Some(crate::checkpoint::PhaseState::Paginated(s)) => s.items_written,
            Some(crate::checkpoint::PhaseState::Simple(s)) => s.completed,
            None => 0,
        };
        tracing::info!("{name}: already completed ({prev} items), skipping");
        return Ok(prev);
    }

    tracing::info!("─── Starting: {name} ───");

    let start = std::time::Instant::now();
    let count = phase
        .run(config, checkpoint)
        .await
        .with_context(|| format!("{name} failed"))?;
    let elapsed = start.elapsed();
    let rate = if elapsed.as_secs() > 0 {
        count as f64 / elapsed.as_secs_f64()
    } else {
        count as f64
    };

    tracing::info!(
        "─── {name}: {count} items in {elapsed:.1?} ({rate:.1}/s) ───"
    );

    Ok(count)
}

/// Build `PipelineStats` from checkpoint data.
///
/// Each phase tracks item counts in the checkpoint; this extracts them into
/// the typed stats structs.  Detailed per-phase stats (image counts, episode
/// counts) are left at default until the phases export them explicitly.
fn count_jsonl_lines(path: &std::path::Path) -> Result<u64> {
    let reader = std::fs::File::open(path)?;
    let buf = std::io::BufReader::new(reader);
    Ok(std::io::BufRead::lines(buf).map_while(|l| l.ok()).count() as u64)
}

fn build_stats(checkpoint: &Checkpoint, stats: &mut PipelineStats) {
    stats.generated_at = chrono::Utc::now().to_rfc3339();

    fn get_count(ckpt: &Checkpoint, id: &PhaseId) -> u64 {
        match ckpt.phases.get(id) {
            Some(crate::checkpoint::PhaseState::Paginated(s)) => s.items_written,
            Some(crate::checkpoint::PhaseState::Simple(s)) => s.completed,
            None => 0,
        }
    }

    stats.anilist_anime = AnilistPhaseStats {
        total_entries: get_count(checkpoint, &PhaseId::AnilistAnime),
    };
    stats.anilist_manga = AnilistPhaseStats {
        total_entries: get_count(checkpoint, &PhaseId::AnilistManga),
    };
    stats.fribb_crossref = CrossrefPhaseStats {
        total_input: get_count(checkpoint, &PhaseId::FribbCrossref),
        matched: 0,   // TODO: extract from crossref phase when it exports stats
        unmatched: 0,
        unmatched_percent: 0.0,
    };
    stats.tmdb_enrich = TmdbPhaseStats {
        total_input: get_count(checkpoint, &PhaseId::TmdbEnrich),
        ..Default::default()
    };
    stats.tvdb_enrich = TvdbPhaseStats {
        total_input: get_count(checkpoint, &PhaseId::TvdbEnrich),
        ..Default::default()
    };
}

/// Log a summary of all phases and their status.
fn summarize(checkpoint: &Checkpoint) {
    tracing::info!("");
    tracing::info!("═══════════════════════════════════════════");
    tracing::info!("  Pipeline Summary");
    tracing::info!("═══════════════════════════════════════════");
    tracing::info!("  Session:     {}", checkpoint.session_id);
    tracing::info!("  Started:     {}", checkpoint.started_at);

    for phase_id in [
        PhaseId::AnilistAnime,
        PhaseId::AnilistManga,
        PhaseId::FribbCrossref,
        PhaseId::TmdbEnrich,
        PhaseId::TvdbEnrich,
    ] {
        let (status, count) = match checkpoint.phases.get(&phase_id) {
            Some(crate::checkpoint::PhaseState::Paginated(s)) => {
                (format!("{:?}", s.status), s.items_written)
            }
            Some(crate::checkpoint::PhaseState::Simple(s)) => {
                (format!("{:?}", s.status), s.completed)
            }
            None => ("NotStarted".to_string(), 0),
        };
        tracing::info!("  {:<15}  {:>8}  {}", phase_id, count, status);
    }

    tracing::info!("═══════════════════════════════════════════");
}
