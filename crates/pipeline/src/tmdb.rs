use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;

use anigraph_model::{Artwork, ArtworkProvider, ArtworkType};

use crate::checkpoint::{Checkpoint, PhaseId};
use crate::jsonl_writer::JsonlWriter;
use crate::phase::{Phase, PipelineConfig};
use crate::stats::TmdbPhaseStats;

// ── Constants ──────────────────────────────────────────────────────────────

const TMDB_BASE_URL: &str = "https://api.themoviedb.org/3";
const TMDB_IMAGE_URL: &str = "https://image.tmdb.org/t/p/original";
const TMDB_CONCURRENCY: usize = 20;
const TMDB_RATE_LIMIT: f64 = 30.0;
const TMDB_MAX_RETRIES: u32 = 3;

/// Max artwork per type — TMDB can return hundreds for popular shows.
const MAX_POSTERS: usize = 10;
const MAX_BACKDROPS: usize = 5;
const MAX_LOGOS: usize = 5;

const ANIME_CROSSREF: &str = "anime-crossref.jsonl";
const ANIME_TMDB: &str = "anime-tmdb.jsonl";

// ── Token-bucket rate limiter ──────────────────────────────────────────────

struct TokenBucket {
    tokens: f64,
    capacity: f64,
    rate: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate: f64) -> Self {
        Self { tokens: rate, capacity: rate, rate, last_refill: Instant::now() }
    }

    fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let elapsed = self.last_refill.elapsed().as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last_refill = Instant::now();
    }
}

// ── TMDB API response types ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TmdbTvResponse {
    #[serde(default)]
    images: TmdbImages,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TmdbMovieResponse {
    #[serde(default)]
    images: TmdbImages,
}

#[derive(Debug, Default, Deserialize)]
struct TmdbImages {
    #[serde(default)]
    backdrops: Vec<TmdbImage>,
    #[serde(default)]
    posters: Vec<TmdbImage>,
    #[serde(default)]
    logos: Vec<TmdbImage>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct TmdbImage {
    file_path: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    #[serde(default)]
    iso_639_1: Option<String>,
    /// TMDB rates images by vote; used to pick the best ones.
    #[serde(default)]
    vote_average: Option<f64>,
}

// ── Artwork conversion (with capping) ──────────────────────────────────────

/// Pick the top `max` images by vote_average (highest first).
/// Images without a vote (`None`) are always sorted to the end.
fn pick_top_images(images: &[TmdbImage], max: usize) -> Vec<TmdbImage> {
    if images.len() <= max {
        return images.to_vec();
    }
    let mut sorted: Vec<_> = images.to_vec();
    sorted.sort_by(|a, b| {
        let bv = b.vote_average.unwrap_or(f64::NEG_INFINITY);
        let av = a.vote_average.unwrap_or(f64::NEG_INFINITY);
        bv.partial_cmp(&av)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    sorted.truncate(max);
    sorted
}

fn tmdb_images_to_artwork(images: &TmdbImages) -> (Vec<Artwork>, usize, usize, usize) {
    let posters = pick_top_images(&images.posters, MAX_POSTERS);
    let backdrops = pick_top_images(&images.backdrops, MAX_BACKDROPS);
    let logos = pick_top_images(&images.logos, MAX_LOGOS);

    let poster_count = posters.len();
    let backdrop_count = backdrops.len();
    let logo_count = logos.len();

    let mut artworks = Vec::with_capacity(poster_count + backdrop_count + logo_count);

    for img in &posters {
        if let Some(ref path) = img.file_path {
            artworks.push(Artwork {
                r#type: ArtworkType::Poster,
                provider: ArtworkProvider::Tmdb,
                url: format!("{TMDB_IMAGE_URL}{path}"),
                width: img.width,
                height: img.height,
                language: img.iso_639_1.clone(),
            });
        }
    }

    for img in &backdrops {
        if let Some(ref path) = img.file_path {
            artworks.push(Artwork {
                r#type: ArtworkType::Backdrop,
                provider: ArtworkProvider::Tmdb,
                url: format!("{TMDB_IMAGE_URL}{path}"),
                width: img.width,
                height: img.height,
                language: img.iso_639_1.clone(),
            });
        }
    }

    for img in &logos {
        if let Some(ref path) = img.file_path {
            artworks.push(Artwork {
                r#type: ArtworkType::Clearlogo,
                provider: ArtworkProvider::Tmdb,
                url: format!("{TMDB_IMAGE_URL}{path}"),
                width: img.width,
                height: img.height,
                language: img.iso_639_1.clone(),
            });
        }
    }

    (artworks, poster_count, backdrop_count, logo_count)
}

// ── API fetch helpers (with retry) ─────────────────────────────────────────

/// Acquire a rate-limit token, then make an HTTP GET to `url`.
///
/// Retries on network errors, 5xx, and 429 up to [`TMDB_MAX_RETRIES`] times
/// with exponential backoff (1s, 2s, 4s).  4xx errors other than 429 are
/// returned immediately — they won't succeed on retry.
async fn fetch_with_retry(
    http: &reqwest::Client,
    api_key: &str,
    bucket: &Mutex<TokenBucket>,
    url: &str,
    label: &str,
) -> Result<reqwest::Response> {
    for attempt in 0..=TMDB_MAX_RETRIES {
        // Acquire rate-limit token
        {
            let mut b = bucket.lock().await;
            while !b.try_consume() {
                let wait = Duration::from_secs_f64(1.0 / TMDB_RATE_LIMIT);
                drop(b);
                tokio::time::sleep(wait).await;
                b = bucket.lock().await;
            }
        }

        match http.get(url).header("Authorization", format!("Bearer {api_key}")).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    return Ok(resp);
                }
                // Retry server errors and rate limits
                if (status.is_server_error() || status.as_u16() == 429)
                    && attempt < TMDB_MAX_RETRIES {
                        let delay = Duration::from_secs(2u64.pow(attempt));
                        tracing::warn!("{label}: HTTP {status}, retry {}/{}", attempt + 1, TMDB_MAX_RETRIES);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                // 4xx errors other than 429 — not worth retrying
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("{label}: HTTP {status}: {body}");
            }
            Err(e) => {
                if attempt < TMDB_MAX_RETRIES {
                    let delay = Duration::from_secs(2u64.pow(attempt));
                    tracing::warn!("{label}: {e}, retry {}/{}", attempt + 1, TMDB_MAX_RETRIES);
                    tokio::time::sleep(delay).await;
                } else {
                    anyhow::bail!("{label}: failed after {} retries: {e}", TMDB_MAX_RETRIES);
                }
            }
        }
    }
    unreachable!()
}

async fn fetch_tv(
    http: &reqwest::Client,
    api_key: &str,
    bucket: &Mutex<TokenBucket>,
    id: i32,
) -> Result<Option<TmdbTvResponse>> {
    let url = format!("{TMDB_BASE_URL}/tv/{id}?append_to_response=images,external_ids");
    let label = format!("TMDB TV {id}");
    let resp = fetch_with_retry(http, api_key, bucket, &url, &label).await?;

    if resp.status().as_u16() == 404 {
        return Ok(None);
    }
    Ok(Some(resp.json().await.with_context(|| format!("TMDB TV {id} parse failed"))?))
}

async fn fetch_movie(
    http: &reqwest::Client,
    api_key: &str,
    bucket: &Mutex<TokenBucket>,
    id: i32,
) -> Result<Option<TmdbMovieResponse>> {
    let url = format!("{TMDB_BASE_URL}/movie/{id}?append_to_response=images,external_ids");
    let label = format!("TMDB movie {id}");
    let resp = fetch_with_retry(http, api_key, bucket, &url, &label).await?;

    if resp.status().as_u16() == 404 {
        return Ok(None);
    }
    Ok(Some(resp.json().await.with_context(|| format!("TMDB movie {id} parse failed"))?))
}

// ── Enrichment function ────────────────────────────────────────────────────

/// Per-entry TMDB enrichment result.
struct EnrichResult {
    entry: Value,
    /// Did we find any TMDB artwork to add?
    artwork_added: bool,
    /// Per-type artwork counts (after capping).
    poster_count: usize,
    backdrop_count: usize,
    logo_count: usize,
    /// Did any API call fail (network, 5xx) after exhausting retries?
    failure_occurred: bool,
}

/// Fetch TMDB data for a single entry and merge it in.
///
/// If the entry has no TMDB IDs, or if all API calls fail, the entry is
/// returned **unchanged** (pass-through).  The caller writes every entry.
async fn enrich_entry(
    http: &reqwest::Client,
    api_key: &str,
    bucket: &Mutex<TokenBucket>,
    mut entry: Value,
) -> EnrichResult {
    let anilist_id = entry["id"].as_i64().unwrap_or(0) as i32;
    let ids = &entry["ids"];

    let tmdb_tv = ids["tmdbTv"].as_i64().map(|v| v as i32);
    let tmdb_movies: Vec<i32> = ids["tmdbMovie"]
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_i64().map(|v| v as i32)).collect())
        .unwrap_or_default();

    if tmdb_tv.is_none() && tmdb_movies.is_empty() {
        return EnrichResult {
            entry,
            artwork_added: false,
            poster_count: 0,
            backdrop_count: 0,
            logo_count: 0,
            failure_occurred: false,
        };
    }

    let mut failure_occurred = false;
    let mut poster_count = 0usize;
    let mut backdrop_count = 0usize;
    let mut logo_count = 0usize;

    // Fetch TV series artwork
    if let Some(tv_id) = tmdb_tv {
        match fetch_tv(http, api_key, bucket, tv_id).await {
            Ok(Some(resp)) => {
                let (artworks, pc, bc, lc) = tmdb_images_to_artwork(&resp.images);
                poster_count += pc;
                backdrop_count += bc;
                logo_count += lc;
                if !artworks.is_empty() {
                    merge_artwork(&mut entry, artworks);
                }
            }
            Ok(None) => { /* 404 — stale cross-ref */ }
            Err(e) => {
                tracing::warn!("TMDB: entry {anilist_id} (TV {tv_id}) fetch failed: {e:#}");
                failure_occurred = true;
            }
        }
    }

    // Fetch movie artwork (may have multiple movie IDs)
    for &movie_id in &tmdb_movies {
        match fetch_movie(http, api_key, bucket, movie_id).await {
            Ok(Some(resp)) => {
                let (artworks, pc, bc, lc) = tmdb_images_to_artwork(&resp.images);
                poster_count += pc;
                backdrop_count += bc;
                logo_count += lc;
                if !artworks.is_empty() {
                    merge_artwork(&mut entry, artworks);
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("TMDB: entry {anilist_id} (movie {movie_id}) fetch failed: {e:#}");
                failure_occurred = true;
            }
        }
    }

    let artwork_added = poster_count + backdrop_count + logo_count > 0;

    if artwork_added
        && let Some(sources) = entry["sources"].as_array_mut()
            && !sources.iter().any(|s| s == "tmdb") {
                sources.push(Value::String("tmdb".to_string()));
            }

    EnrichResult {
        entry,
        artwork_added,
        poster_count,
        backdrop_count,
        logo_count,
        failure_occurred,
    }
}

/// Append `new_artworks` to the entry's existing `artwork` array.
fn merge_artwork(entry: &mut Value, new_artworks: Vec<Artwork>) {
    let existing = entry["artwork"].take();
    let mut combined: Vec<Artwork> = if existing.is_null() {
        Vec::new()
    } else {
        serde_json::from_value(existing).unwrap_or_default()
    };
    combined.extend(new_artworks);
    entry["artwork"] = serde_json::to_value(&combined).unwrap_or(Value::Null);
}

// ── Phase ──────────────────────────────────────────────────────────────────

pub struct TmdbEnrichPhase;

#[async_trait]
impl Phase for TmdbEnrichPhase {
    fn id(&self) -> PhaseId { PhaseId::TmdbEnrich }
    fn name(&self) -> &'static str { "TMDB Enrichment" }

    async fn run(&self, config: &PipelineConfig, checkpoint: &mut Checkpoint) -> Result<u64> {
        let phase_id = self.id();
        let api_key = config.tmdb_api_key.as_ref()
            .context("TMDB_API_KEY not set")?;

        let input_path = config.work_dir.join(ANIME_CROSSREF);
        let output_path = config.work_dir.join(ANIME_TMDB);

        if !input_path.exists() {
            anyhow::bail!("input not found at {}", input_path.display());
        }

        // ── Determine start state ───────────────────────────────────────
        let total_input = count_jsonl_lines(&input_path)?;
        let start_from = if config.resume {
            if checkpoint.is_completed(&phase_id) {
                tracing::info!("TMDB enrichment already completed, skipping");
                let total = match checkpoint.phases.get(&phase_id) {
                    Some(crate::checkpoint::PhaseState::Simple(s)) => s.completed,
                    _ => checkpoint.items_written(&phase_id),
                };
                return Ok(total);
            }
            if output_path.exists() { count_jsonl_lines(&output_path)? } else { 0 }
        } else {
            checkpoint.phases.insert(
                phase_id.clone(),
                crate::checkpoint::PhaseState::Simple(
                    crate::checkpoint::SimpleState::new(total_input),
                ),
            );
            0
        };

        // ── Read entries ────────────────────────────────────────────────
        let reader = std::fs::File::open(&input_path)?;
        let buf = std::io::BufReader::new(reader);
        let lines: Vec<String> = std::io::BufRead::lines(buf)
            .map_while(|l| l.ok())
            .skip(start_from as usize)
            .collect();
        let batch_size = lines.len() as u64;

        if batch_size == 0 {
            if start_from > 0 {
                if let crate::checkpoint::PhaseState::Simple(s) =
                    checkpoint.phases.get_mut(&phase_id).unwrap() { s.completed = start_from; }
                checkpoint.complete_simple(&phase_id);
                checkpoint.save(&config.checkpoint_path)?;
            }
            return Ok(0);
        }

        tracing::info!("TMDB: processing {batch_size} entries (from line {start_from})");

        // Wall-clock start, used for the ETA in the progress indicator.
        let start = Instant::now();

        // ── Shared state ───────────────────────────────────────────────
        let http = reqwest::Client::new();
        let bucket = Arc::new(Mutex::new(TokenBucket::new(TMDB_RATE_LIMIT)));
        let api_key = api_key.clone();

        // ── Ordered buffered processing ─────────────────────────────────
        // Each stream item returns: (out_line, idx, has_tv_id, has_movie_ids, artwork_added, poster, backdrop, logo, failure_occurred)
        let stream = futures::stream::iter(lines.into_iter().enumerate().map(
            |(offset, line)| {
                let http = http.clone();
                let bucket = Arc::clone(&bucket);
                let api_key = api_key.clone();

                async move {
                    let idx = start_from + offset as u64;

                    let entry: Value = match serde_json::from_str(&line) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("TMDB: malformed JSON at line {idx}: {e}");
                            return (line, idx, false, false, 0u64, 0u64, 0u64, 0u64, 0u64);
                        }
                    };

                    let has_tv_id = entry["ids"]["tmdbTv"].is_number();
                    let has_movie_ids = entry["ids"]["tmdbMovie"]
                        .as_array().is_some_and(|a| !a.is_empty());

                    let result = enrich_entry(&http, &api_key, &bucket, entry).await;
                    let out_line = serde_json::to_string(&result.entry)
                        .unwrap_or_else(|_| line.clone());

                    let artwork_added = if result.artwork_added { 1u64 } else { 0u64 };
                    let failure = if result.failure_occurred { 1u64 } else { 0u64 };

                    (out_line, idx, has_tv_id, has_movie_ids,
                     result.poster_count as u64, result.backdrop_count as u64, result.logo_count as u64,
                     failure, artwork_added)
                }
            },
        ))
        .buffered(TMDB_CONCURRENCY);

        // ── Write results ──────────────────────────────────────────────
        let mut writer = if start_from > 0 {
            JsonlWriter::append(&output_path)?
        } else {
            JsonlWriter::new(&output_path)?
        };

        let mut stats = TmdbPhaseStats::default();
        let mut processed = 0u64;

        tokio::pin!(stream);
        while let Some((out_line, idx, has_tv, has_movie, posters, backdrops, logos, failure, added)) = stream.next().await {
            writer.write_raw(&out_line).context(format!("writing line {idx}"))?;
            processed += 1;

            stats.total_input += 1;
            if has_tv { stats.with_tv_ids += 1; }
            if has_movie { stats.with_movie_ids += 1; }
            if added > 0 { stats.artwork_found += 1; } else if has_tv || has_movie { stats.no_artwork += 1; }
            stats.total_poster_count += posters;
            stats.total_backdrop_count += backdrops;
            stats.total_logo_count += logos;
            stats.failures += failure;

            // Periodically report progress + stats
            if processed.is_multiple_of(1000) {
                let written = start_from + processed;
                if let crate::checkpoint::PhaseState::Simple(s) =
                    checkpoint.phases.get_mut(&phase_id).unwrap() { s.completed = written; }
                crate::progress::log_progress(
                    "TMDB",
                    written,
                    stats.total_input,
                    Some(start),
                    &[
                        ("TV", stats.with_tv_ids),
                        ("movie", stats.with_movie_ids),
                        ("artwork", stats.artwork_found),
                        ("failures", stats.failures),
                    ],
                );
            }
        }

        writer.flush()?;
        let total_written = start_from + processed;

        tracing::info!(
            "TMDB complete: {} input, {} written.  TV IDs: {}, Movie IDs: {}, artwork found: {}, \
             total images: {}p+{}b+{}l, failures: {}",
            stats.total_input, total_written,
            stats.with_tv_ids, stats.with_movie_ids, stats.artwork_found,
            stats.total_poster_count, stats.total_backdrop_count, stats.total_logo_count,
            stats.failures,
        );

        if let crate::checkpoint::PhaseState::Simple(s) =
            checkpoint.phases.get_mut(&phase_id).unwrap() { s.completed = total_written; }
        checkpoint.complete_simple(&phase_id);
        checkpoint.save(&config.checkpoint_path)
            .context("saving final checkpoint")?;

        Ok(total_written)
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn count_jsonl_lines(path: &std::path::Path) -> Result<u64> {
    let reader = std::fs::File::open(path)?;
    let buf = std::io::BufReader::new(reader);
    Ok(std::io::BufRead::lines(buf).map_while(|l| l.ok()).count() as u64)
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Token bucket ───────────────────────────────────────────────────

    #[test]
    fn test_token_bucket_initial_full() {
        let bucket = TokenBucket::new(10.0);
        assert!((bucket.tokens - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_token_bucket_can_consume() {
        let mut bucket = TokenBucket::new(10.0);
        assert!(bucket.try_consume());
        assert!(bucket.try_consume());
        assert!((bucket.tokens - 8.0).abs() < 0.1);
    }

    #[test]
    fn test_token_bucket_refill() {
        let mut bucket = TokenBucket::new(10.0);
        bucket.tokens = 0.0;
        bucket.last_refill = Instant::now() - Duration::from_secs_f64(0.5);
        bucket.refill();
        assert!((bucket.tokens - 5.0).abs() < 0.1);
    }

    #[test]
    fn test_token_bucket_cannot_exceed_capacity() {
        let mut bucket = TokenBucket::new(10.0);
        bucket.last_refill = Instant::now() - Duration::from_secs(10);
        bucket.refill();
        assert!((bucket.tokens - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_token_bucket_exhaustion() {
        let mut bucket = TokenBucket::new(1.0);
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume());
    }

    // ── Artwork capping ────────────────────────────────────────────────

    #[test]
    fn test_pick_top_images_under_limit() {
        let images = vec![
            TmdbImage { file_path: Some("/a.jpg".into()), vote_average: Some(5.0), ..Default::default() },
            TmdbImage { file_path: Some("/b.jpg".into()), vote_average: Some(3.0), ..Default::default() },
        ];
        let picked = pick_top_images(&images, 10);
        assert_eq!(picked.len(), 2);
    }

    #[test]
    fn test_pick_top_images_caps_correctly() {
        let images: Vec<TmdbImage> = (0..20)
            .map(|i| TmdbImage {
                file_path: Some(format!("/{i}.jpg")),
                vote_average: Some(i as f64),
                ..Default::default()
            })
            .collect();
        let picked = pick_top_images(&images, 5);
        assert_eq!(picked.len(), 5);
        // Highest vote_average should be at the top
        for i in 0..4 {
            assert!(picked[i].vote_average >= picked[i + 1].vote_average);
        }
    }

    #[test]
    fn test_pick_top_images_handles_none_votes() {
        let images = vec![
            TmdbImage { file_path: Some("/a.jpg".into()), vote_average: None, ..Default::default() },
            TmdbImage { file_path: Some("/b.jpg".into()), vote_average: Some(5.0), ..Default::default() },
            TmdbImage { file_path: Some("/c.jpg".into()), vote_average: None, ..Default::default() },
        ];
        let picked = pick_top_images(&images, 2);
        assert_eq!(picked.len(), 2);
        assert_eq!(picked[0].vote_average, Some(5.0));
    }

    // ── Artwork conversion ─────────────────────────────────────────────

    #[test]
    fn test_tmdb_images_to_artwork_empty() {
        let images = TmdbImages::default();
        let (artworks, pc, bc, lc) = tmdb_images_to_artwork(&images);
        assert!(artworks.is_empty());
        assert_eq!(pc, 0);
        assert_eq!(bc, 0);
        assert_eq!(lc, 0);
    }

    #[test]
    fn test_tmdb_images_to_artwork_posters() {
        let images = TmdbImages {
            posters: vec![TmdbImage {
                file_path: Some("/p.jpg".into()), width: Some(1000), height: Some(1500),
                iso_639_1: Some("en".into()), vote_average: Some(7.5),
            }],
            backdrops: vec![],
            logos: vec![],
        };
        let (artworks, pc, bc, lc) = tmdb_images_to_artwork(&images);
        assert_eq!(pc, 1);
        assert_eq!(bc, 0);
        assert_eq!(lc, 0);
        assert_eq!(artworks.len(), 1);
        assert_eq!(format!("{:?}", artworks[0].r#type), "Poster");
        assert_eq!(format!("{:?}", artworks[0].provider), "Tmdb");
        assert!(artworks[0].url.contains("/p.jpg"));
    }

    #[test]
    fn test_tmdb_images_to_artwork_all_types() {
        let images = TmdbImages {
            posters: vec![TmdbImage { file_path: Some("/p.jpg".into()), ..Default::default() }],
            backdrops: vec![TmdbImage { file_path: Some("/b.jpg".into()), ..Default::default() }],
            logos: vec![TmdbImage { file_path: Some("/l.png".into()), ..Default::default() }],
        };
        let (artworks, pc, bc, lc) = tmdb_images_to_artwork(&images);
        assert_eq!(pc, 1);
        assert_eq!(bc, 1);
        assert_eq!(lc, 1);
        assert_eq!(artworks.len(), 3);
    }

    // ── Phase identity ─────────────────────────────────────────────────

    #[test]
    fn test_tmdb_enrich_phase_id() {
        let phase = TmdbEnrichPhase;
        assert_eq!(phase.id(), PhaseId::TmdbEnrich);
        assert_eq!(phase.name(), "TMDB Enrichment");
    }
}
