use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;

use anigraph_model::{
    Artwork, ArtworkProvider, ArtworkType, Episode, EpisodeIds, EpisodeRating, EpisodeTitle,
    FuzzyDate,
};

use crate::checkpoint::{Checkpoint, PhaseId};
use crate::jsonl_writer::JsonlWriter;
use crate::phase::{Phase, PipelineConfig};
use crate::stats::TmdbPhaseStats;

// ── Constants ──────────────────────────────────────────────────────────────

const TMDB_BASE_URL: &str = "https://api.themoviedb.org/3";
const TMDB_IMAGE_URL: &str = "https://image.tmdb.org/t/p/original";

/// TMDB v3 API keys are exactly 32 hex characters.  v4 read-access tokens are
/// longer opaque strings.  The v3 API authenticates v3 keys via the
/// `?api_key=` query parameter and v4 tokens via the `Authorization: Bearer`
/// header — so we pick the scheme based on the key's shape.
fn is_v3_api_key(key: &str) -> bool {
    key.len() == 32 && key.chars().all(|c| c.is_ascii_hexdigit())
}
const TMDB_CONCURRENCY: usize = 20;
const TMDB_RATE_LIMIT: f64 = 30.0;
const TMDB_MAX_RETRIES: u32 = 3;

/// Max artwork per type — TMDB can return hundreds for popular shows.
const MAX_POSTERS: usize = 10;
const MAX_BACKDROPS: usize = 5;
const MAX_LOGOS: usize = 5;

/// Safety cap on seasons fetched per show when collecting episodes.
const MAX_SEASONS_PER_SHOW: usize = 60;

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

/// NOTE: TMDB v3 responses are **snake_case** (`season_number`,
/// `episode_count`, …).  Do NOT add `rename_all = "camelCase"` here — it
/// silently deserializes every field to `None` and zero episodes ship
/// (this exact bug produced the episode-less 2026-34-fix1 release).
#[derive(Debug, Deserialize)]
struct TmdbTvResponse {
    #[serde(default)]
    images: TmdbImages,
    /// Season list — used to discover which seasons to fetch episodes for.
    #[serde(default)]
    seasons: Vec<TmdbSeasonSummary>,
}

#[derive(Debug, Deserialize)]
struct TmdbMovieResponse {
    #[serde(default)]
    images: TmdbImages,
}

/// Season entry inside the `/tv/{id}` response.
#[derive(Debug, Default, Deserialize)]
struct TmdbSeasonSummary {
    season_number: Option<i32>,
    #[serde(default)]
    episode_count: Option<i32>,
}

/// Response of `/tv/{id}/season/{n}`.
#[derive(Debug, Default, Deserialize)]
struct TmdbSeasonResponse {
    #[serde(default)]
    episodes: Vec<TmdbEpisodeDto>,
}

/// Episode record inside a season response.
#[derive(Debug, Default, Deserialize)]
struct TmdbEpisodeDto {
    id: Option<i64>,
    episode_number: Option<i32>,
    season_number: Option<i32>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    overview: Option<String>,
    #[serde(default)]
    air_date: Option<String>,
    #[serde(default)]
    runtime: Option<i32>,
    #[serde(default)]
    vote_average: Option<f64>,
    #[serde(default)]
    vote_count: Option<i64>,
    #[serde(default)]
    still_path: Option<String>,
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

        let mut req = http.get(url);
        if !is_v3_api_key(api_key) {
            req = req.header("Authorization", format!("Bearer {api_key}"));
        }
        match req.send().await {
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
    let mut url = format!("{TMDB_BASE_URL}/tv/{id}?append_to_response=images,external_ids");
    if is_v3_api_key(api_key) {
        url.push_str(&format!("&api_key={api_key}"));
    }
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
    let mut url = format!("{TMDB_BASE_URL}/movie/{id}?append_to_response=images,external_ids");
    if is_v3_api_key(api_key) {
        url.push_str(&format!("&api_key={api_key}"));
    }
    let label = format!("TMDB movie {id}");
    let resp = fetch_with_retry(http, api_key, bucket, &url, &label).await?;

    if resp.status().as_u16() == 404 {
        return Ok(None);
    }
    Ok(Some(resp.json().await.with_context(|| format!("TMDB movie {id} parse failed"))?))
}

/// Fetch all seasons' episodes for a show, in parallel.
///
/// Season 0 (specials) is included — the caller decides how to order/keep it.
/// Seasons with zero episodes are skipped.  Bounded by `MAX_SEASONS_PER_SHOW`.
async fn fetch_show_episodes(
    http: &reqwest::Client,
    api_key: &str,
    bucket: &Mutex<TokenBucket>,
    tv_id: i32,
    seasons: &[TmdbSeasonSummary],
) -> Result<Vec<Episode>> {
    let season_numbers: Vec<i32> = seasons
        .iter()
        .filter(|s| s.episode_count.unwrap_or(0) > 0)
        .filter_map(|s| s.season_number)
        .take(MAX_SEASONS_PER_SHOW)
        .collect();

    if season_numbers.is_empty() {
        return Ok(Vec::new());
    }

    let fetches = season_numbers.iter().map(|sn| {
        fetch_season(http, api_key, bucket, tv_id, *sn)
    });
    let results = futures::future::join_all(fetches).await;

    let mut episodes = Vec::new();
    for (sn, res) in season_numbers.iter().zip(results) {
        match res {
            Ok(Some(season)) => {
                episodes.extend(map_tmdb_episodes(*sn, &season.episodes));
            }
            Ok(None) => {}
            Err(e) => {
                // One bad season shouldn't kill the whole show.
                tracing::warn!("TMDB TV {tv_id} season {sn}: {e:#}");
            }
        }
    }
    episodes.sort_by_key(|e| (e.season_number.unwrap_or(0), e.number));
    Ok(episodes)
}

async fn fetch_season(
    http: &reqwest::Client,
    api_key: &str,
    bucket: &Mutex<TokenBucket>,
    tv_id: i32,
    season_number: i32,
) -> Result<Option<TmdbSeasonResponse>> {
    let mut url = format!("{TMDB_BASE_URL}/tv/{tv_id}/season/{season_number}");
    if is_v3_api_key(api_key) {
        url.push_str(&format!("?api_key={api_key}"));
    }
    let label = format!("TMDB TV {tv_id} season {season_number}");
    let resp = fetch_with_retry(http, api_key, bucket, &url, &label).await?;

    if resp.status().as_u16() == 404 {
        return Ok(None);
    }
    Ok(Some(
        resp.json()
            .await
            .with_context(|| format!("{label} parse failed"))?,
    ))
}

/// Parse a TMDB air date (`YYYY-MM-DD`) into a [`FuzzyDate`].
fn parse_tmdb_date(s: &str) -> Option<FuzzyDate> {
    let mut parts = s.split('-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: i32 = parts.next()?.parse().ok()?;
    let day: i32 = parts.next()?.parse().ok()?;
    Some(FuzzyDate { year, month: Some(month), day: Some(day) })
}

/// Convert a season's TMDB episode records into model [`Episode`]s.
fn map_tmdb_episodes(season_number: i32, dtos: &[TmdbEpisodeDto]) -> Vec<Episode> {
    dtos
        .iter()
        .filter_map(|dto| {
            let number = dto.episode_number?;
            let name = dto.name.as_deref().map(str::trim).filter(|s| !s.is_empty());
            let overview = dto
                .overview
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty());

            let rating = match (dto.vote_average, dto.vote_count) {
                (Some(avg), Some(count)) if count > 0 => {
                    Some(EpisodeRating { average: avg, count: Some(count) })
                }
                _ => None,
            };

            Some(Episode {
                number,
                absolute: None,
                season_number: dto.season_number.or(Some(season_number)),
                titles: name.map(|n| EpisodeTitle {
                    english: Some(n.to_string()),
                    native: None,
                    romaji: None,
                }),
                air_date: dto.air_date.as_deref().and_then(parse_tmdb_date),
                runtime: dto.runtime,
                overview: overview.map(String::from),
                rating,
                image: dto
                    .still_path
                    .as_deref()
                    .map(|p| format!("{TMDB_IMAGE_URL}{p}")),
                ids: dto.id.map(|id| EpisodeIds { tmdb: Some(id as i32), ..Default::default() }),
            })
        })
        .collect()
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
    /// Episodes fetched from TMDB season endpoints.
    episode_count: usize,
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
            episode_count: 0,
            failure_occurred: false,
        };
    }

    let mut failure_occurred = false;
    let mut poster_count = 0usize;
    let mut backdrop_count = 0usize;
    let mut logo_count = 0usize;
    let mut episode_count = 0usize;

    // Fetch TV series artwork + episodes
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
                // ── Episodes ────────────────────────────────────────────
                match fetch_show_episodes(http, api_key, bucket, tv_id, &resp.seasons).await {
                    Ok(eps) if !eps.is_empty() => {
                        episode_count = eps.len();
                        match serde_json::to_value(&eps) {
                            Ok(v) => entry["episodes"] = v,
                            Err(e) => {
                                tracing::warn!(
                                    "TMDB: entry {anilist_id} (TV {tv_id}) episode serialize failed: {e}"
                                );
                                failure_occurred = true;
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(
                            "TMDB: entry {anilist_id} (TV {tv_id}) episode fetch failed: {e:#}"
                        );
                        failure_occurred = true;
                    }
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

    if (artwork_added || episode_count > 0)
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
        episode_count,
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
        // Each stream item returns: (out_line, idx, has_tv_id, has_movie_ids, artwork_added,
        // poster, backdrop, logo, failure_occurred, episode_count)
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
                            return (line, idx, false, false, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
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
                     failure, artwork_added, result.episode_count as u64)
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
        let mut total_episodes = 0u64;
        let mut entries_with_episodes = 0u64;

        tokio::pin!(stream);
        while let Some((out_line, idx, has_tv, has_movie, posters, backdrops, logos, failure, added, eps)) = stream.next().await {
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
            total_episodes += eps;
            if eps > 0 { entries_with_episodes += 1; }

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
                        ("episodes", total_episodes),
                        ("failures", stats.failures),
                    ],
                );
            }
        }

        writer.flush()?;
        let total_written = start_from + processed;

        tracing::info!(
            "TMDB complete: {} input, {} written.  TV IDs: {}, Movie IDs: {}, artwork found: {}, \
             total images: {}p+{}b+{}l, episodes: {} across {} shows, failures: {}",
            stats.total_input, total_written,
            stats.with_tv_ids, stats.with_movie_ids, stats.artwork_found,
            stats.total_poster_count, stats.total_backdrop_count, stats.total_logo_count,
            total_episodes, entries_with_episodes,
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

    // ── API key scheme detection ──────────────────────────────────────

    #[test]
    fn test_is_v3_api_key() {
        // 32-char hex v3 key -> query-param auth
        assert!(is_v3_api_key(
            "0123456789abcdef0123456789abcdef"
        ));
        // v4 read token (long, non-hex) -> Bearer auth
        assert!(!is_v3_api_key(
            "eyJhbGciOiJIUzI1NiJ9.abcdefghijklmnopqrstuvwxyz0123456789"
        ));
        // non-hex character -> not a v3 key
        assert!(!is_v3_api_key("0123456789abcdef0123456789abcdefg"));
        assert!(!is_v3_api_key("too_short"));
    }

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
    fn test_tmdb_tv_response_parses_snake_case_seasons() {
        // Regression guard: TMDB v3 payloads are snake_case.  A camelCase
        // rename silently deserialized every season to None and shipped a
        // dataset with zero episode metadata (2026-34-fix1).
        let body: TmdbTvResponse = serde_json::from_str(
            r#"{"images":{"posters":[]},"seasons":[
                {"season_number":0,"episode_count":37,"name":"Specials"},
                {"season_number":1,"episode_count":25,"name":"Season 1"}]}"#,
        )
        .unwrap();
        let seasons: Vec<(i32, i32)> = body
            .seasons
            .iter()
            .filter_map(|s| s.season_number.map(|n| (n, s.episode_count.unwrap_or(0))))
            .collect();
        assert_eq!(seasons, vec![(0, 37), (1, 25)]);
    }

    #[test]
    fn test_tmdb_episode_dto_parses_snake_case() {
        let body: TmdbSeasonResponse = serde_json::from_str(
            r#"{"episodes":[
                {"id":63066,"episode_number":1,"season_number":1,
                 "name":"Asteroid Blues","overview":"Spike and Jet…",
                 "air_date":"1998-04-03","runtime":25,
                 "vote_average":7.377,"vote_count":57,"still_path":"/a.jpg"},
                {"id":2,"episode_number":2,"season_number":1,
                 "name":"Stray Dog Strut","overview":null,"air_date":null}]}"#,
        )
        .unwrap();
        let eps = map_tmdb_episodes(1, &body.episodes);
        assert_eq!(eps.len(), 2);
        assert_eq!(eps[0].number, 1);
        assert_eq!(eps[0].season_number, Some(1));
        assert_eq!(eps[0].titles.as_ref().unwrap().english.as_deref(), Some("Asteroid Blues"));
        assert_eq!(eps[0].air_date.as_ref().unwrap().year, 1998);
        assert_eq!(eps[0].rating.as_ref().unwrap().average, 7.377);
        assert_eq!(eps[0].rating.as_ref().unwrap().count, Some(57));
        assert!(eps[0].image.as_deref().unwrap().ends_with("/a.jpg"));
        assert_eq!(eps[1].overview, None);
        assert!(eps[1].rating.is_none());
    }

    #[test]
    fn test_tmdb_enrich_phase_id() {
        let phase = TmdbEnrichPhase;
        assert_eq!(phase.id(), PhaseId::TmdbEnrich);
        assert_eq!(phase.name(), "TMDB Enrichment");
    }
}
