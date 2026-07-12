use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;

use anigraph_model::{Artwork, ArtworkProvider, ArtworkType, Episode, EpisodeIds, EpisodeTitle, FuzzyDate};

use crate::checkpoint::{Checkpoint, PhaseId};
use crate::jsonl_writer::JsonlWriter;
use crate::phase::{Phase, PipelineConfig};
use crate::stats::TvdbPhaseStats;

// ── Constants ──────────────────────────────────────────────────────────────

const TVDB_BASE_URL: &str = "https://api4.thetvdb.com/v4";
const TVDB_LOGIN_RETRIES: u32 = 3;
const TVDB_FETCH_RETRIES: u32 = 3;
/// How many entries to process concurrently.  With ~275ms latency per
/// request, 10 concurrent workers keeps the 15 req/s limit saturated without
/// over-pending.
const TVDB_CONCURRENCY: usize = 10;

const ANIME_TMDB: &str = "anime-tmdb.jsonl";
const ANIME_TVDB: &str = "anime-tvdb.jsonl";

/// Max TVDB artwork per type.
const MAX_TVDB_ARTWORK: usize = 5;

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

// ── TVDB API response types ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct TvdbLoginResponse {
    data: TvdbLoginData,
}

#[derive(Debug, Deserialize)]
struct TvdbLoginData {
    token: String,
}

#[derive(Debug, Deserialize)]
struct TvdbEpisodeResponse {
    data: TvdbEpisodeData,
    links: Option<TvdbLinks>,
}

#[derive(Debug, Deserialize)]
struct TvdbEpisodeData {
    episodes: Vec<TvdbEpisode>,
}

#[derive(Debug, Deserialize)]
struct TvdbLinks {
    next: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TvdbEpisode {
    id: Option<i32>,
    number: Option<i32>,
    #[serde(default)]
    absolute_number: Option<i32>,
    #[serde(default)]
    season_number: Option<i32>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    overview: Option<String>,
    #[serde(default)]
    aired: Option<String>,
    #[serde(default)]
    runtime: Option<i32>,
}

/// TVDB v4 extended series response — includes artworks, episodes, etc.
/// We only deserialize the `artworks` field; the rest is ignored via
/// `#[serde(deny_unknown_fields)]` — instead we use `#[serde(default)]`
/// for lenient parsing.
#[derive(Debug, Default, Deserialize)]
struct TvdbExtendedResponse {
    data: TvdbExtendedData,
}

#[derive(Debug, Default, Deserialize)]
struct TvdbExtendedData {
    #[serde(default)]
    artworks: Vec<TvdbArtworkItem>,
}

#[derive(Debug, Default, Deserialize)]
struct TvdbArtworkItem {
    image: Option<String>,
    /// Can be either a plain integer (`1`) or an object (`{"id": 1, "name": "poster"}`)
    /// depending on the endpoint.  We deserialize as `Value` and extract the ID in
    /// [`map_tvdb_artwork`] to handle both formats.
    #[serde(rename = "type", default)]
    artwork_type: Option<Value>,
    width: Option<i32>,
    height: Option<i32>,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    score: Option<f64>,
}

// ── Auth ───────────────────────────────────────────────────────────────────

/// Ensure a valid auth token exists in `token_cache`, logging in if needed.
/// Double-checked locking: quick check outside the mutex, then check again
/// inside so only one worker actually calls /login.
async fn ensure_token(
    http: &reqwest::Client,
    api_key: &str,
    token_cache: &Mutex<Option<String>>,
    bucket: &Mutex<TokenBucket>,
) -> Result<()> {
    // Fast path: token already exists
    if token_cache.try_lock().is_ok_and(|g| g.is_some()) {
        return Ok(());
    }

    let mut guard = token_cache.lock().await;
    if guard.is_some() {
        return Ok(()); // another worker already logged in
    }

    let url = format!("{TVDB_BASE_URL}/login");
    for attempt in 0..=TVDB_LOGIN_RETRIES {
        // Rate-limit login too
        {
            let mut b = bucket.lock().await;
            b.try_consume();
        }

        match http
            .post(&url)
            .header("Content-Type", "application/json")
            .body(format!(r#"{{"apikey":"{api_key}"}}"#))
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    let login: TvdbLoginResponse = resp.json().await?;
                    *guard = Some(login.data.token);
                    tracing::debug!("TVDB: logged in successfully");
                    return Ok(());
                }
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                if attempt < TVDB_LOGIN_RETRIES {
                    let delay = Duration::from_secs(2u64.pow(attempt));
                    tracing::warn!("TVDB login: HTTP {status}, retry {}/{}", attempt + 1, TVDB_LOGIN_RETRIES);
                    tokio::time::sleep(delay).await;
                } else {
                    anyhow::bail!("TVDB login failed: HTTP {status}: {body}");
                }
            }
            Err(e) => {
                if attempt < TVDB_LOGIN_RETRIES {
                    let delay = Duration::from_secs(2u64.pow(attempt));
                    tracing::warn!("TVDB login: {e}, retry {}/{}", attempt + 1, TVDB_LOGIN_RETRIES);
                    tokio::time::sleep(delay).await;
                } else {
                    anyhow::bail!("TVDB login failed after {} retries: {e}", TVDB_LOGIN_RETRIES);
                }
            }
        }
    }
    unreachable!()
}

// ── API fetch helpers (with retry) ─────────────────────────────────────────

/// Acquire a rate-limit token, then make an authenticated GET request.
///
/// Retries on network errors, 5xx, and 429 up to [`TVDB_FETCH_RETRIES`] times
/// with exponential backoff (1s, 2s, 4s).  Automatically re-logs in on 401.
async fn fetch_with_retry(
    http: &reqwest::Client,
    api_key: &str,
    url: &str,
    label: &str,
    token_cache: &Mutex<Option<String>>,
    bucket: &Mutex<TokenBucket>,
) -> Result<reqwest::Response> {
    for attempt in 0..=TVDB_FETCH_RETRIES {
        // Acquire rate-limit token
        {
            let mut b = bucket.lock().await;
            while !b.try_consume() {
                let wait = Duration::from_secs_f64(1.0 / b.rate.max(0.1));
                drop(b);
                tokio::time::sleep(wait).await;
                b = bucket.lock().await;
            }
        }

        // Read the current token outside the lock
        let token = token_cache.lock().await.clone();

        let token_str = token.as_deref().unwrap_or("");

        let mut resp = match http
            .get(url)
            .header("Authorization", format!("Bearer {token_str}"))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if attempt < TVDB_FETCH_RETRIES {
                    let delay = Duration::from_secs(2u64.pow(attempt));
                    tracing::warn!("{label}: {e}, retry {}/{}", attempt + 1, TVDB_FETCH_RETRIES);
                    tokio::time::sleep(delay).await;
                    continue;
                }
                anyhow::bail!("{label}: failed after {} retries: {e}", TVDB_FETCH_RETRIES);
            }
        };

        let status = resp.status();

        // Token expired — re-login and retry
        if status.as_u16() == 401 {
            tracing::info!("{label}: token expired, re-logging in");
            let mut guard = token_cache.lock().await;
            *guard = None; // clear stale token
            drop(guard);
            ensure_token(http, api_key, token_cache, bucket).await?;

            let new_token = token_cache.lock().await.clone();
            let new_token_str = new_token.as_deref().unwrap_or("");

            match http
                .get(url)
                .header("Authorization", format!("Bearer {new_token_str}"))
                .send()
                .await
            {
                Ok(r) => resp = r,
                Err(e) => {
                    if attempt < TVDB_FETCH_RETRIES {
                        let delay = Duration::from_secs(2u64.pow(attempt));
                        tracing::warn!("{label}: {e} (after re-login), retry {}/{}", attempt + 1, TVDB_FETCH_RETRIES);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    anyhow::bail!("{label}: failed after re-login: {e}");
                }
            }
        }

        let status = resp.status();

        if status.is_success() {
            return Ok(resp);
        }

        // Retry on 5xx and 429
        if (status.is_server_error() || status.as_u16() == 429)
            && attempt < TVDB_FETCH_RETRIES {
                let delay = Duration::from_secs(2u64.pow(attempt));
                tracing::warn!("{label}: HTTP {status}, retry {}/{}", attempt + 1, TVDB_FETCH_RETRIES);
                tokio::time::sleep(delay).await;
                continue;
            }

        // 404 → return OK with the response (caller handles 404)
        if status.as_u16() == 404 {
            return Ok(resp);
        }

        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("{label}: HTTP {status}: {body}");
    }
    unreachable!()
}

/// Fetch a page of episodes for a series.
async fn fetch_episodes_page(
    http: &reqwest::Client,
    api_key: &str,
    series_id: i32,
    page: u32,
    token_cache: &Mutex<Option<String>>,
    bucket: &Mutex<TokenBucket>,
) -> Result<(Vec<TvdbEpisode>, bool)> {
    let url = format!("{TVDB_BASE_URL}/series/{series_id}/episodes/default/{page}");
    let label = format!("TVDB episodes {series_id} p{page}");

    let resp = fetch_with_retry(http, api_key, &url, &label, token_cache, bucket).await?;
    let body: TvdbEpisodeResponse = resp.json().await
        .with_context(|| format!("{label}: parse failed"))?;

    let has_next = body.links
        .and_then(|l| l.next)
        .is_some();

    Ok((body.data.episodes, has_next))
}

/// Fetch all episodes for a series (all pages).
async fn fetch_all_episodes(
    http: &reqwest::Client,
    api_key: &str,
    series_id: i32,
    token_cache: &Mutex<Option<String>>,
    bucket: &Mutex<TokenBucket>,
) -> Result<Vec<TvdbEpisode>> {
    let mut all_episodes = Vec::new();
    let mut page = 1u32;

    loop {
        let (episodes, has_next) = fetch_episodes_page(http, api_key, series_id, page, token_cache, bucket).await?;
        all_episodes.extend(episodes);

        if !has_next {
            break;
        }
        page += 1;
    }

    Ok(all_episodes)
}

/// Fetch artwork for a series (optional enrichment).
///
/// TVDB v4 has no dedicated `/series/{id}/artwork` endpoint.  Artwork is
/// returned inside the **extended** series endpoint at
/// `/series/{id}/extended` under `data.artworks`.
///
/// Calling the extended endpoint also returns episodes, characters, and
/// other metadata — we only extract the artworks array and ignore the rest.
async fn fetch_artwork(
    http: &reqwest::Client,
    api_key: &str,
    series_id: i32,
    token_cache: &Mutex<Option<String>>,
    bucket: &Mutex<TokenBucket>,
) -> Result<Vec<TvdbArtworkItem>> {
    let url = format!("{TVDB_BASE_URL}/series/{series_id}/extended");
    let label = format!("TVDB artwork {series_id}");

    let resp = fetch_with_retry(http, api_key, &url, &label, token_cache, bucket).await?;
    let body: TvdbExtendedResponse = resp.json().await
        .with_context(|| format!("{label}: parse failed"))?;

    Ok(body.data.artworks)
}

// ── Episode mapping ────────────────────────────────────────────────────────

/// Parse an `aired` string like "2022-01-15" into a `FuzzyDate`.
fn parse_tvdb_date(s: &str) -> Option<FuzzyDate> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year = parts[0].parse::<i32>().ok()?;
    let month = parts[1].parse::<i32>().ok();
    let day = parts[2].parse::<i32>().ok();
    Some(FuzzyDate { year, month, day })
}

fn map_tvdb_episodes(tvdb_eps: &[TvdbEpisode]) -> Vec<Episode> {
    tvdb_eps
        .iter()
        .filter_map(|ep| {
            let number = ep.number?;
            let tvdb_id = ep.id?;

            let titles = ep.name.as_ref().map(|name| EpisodeTitle {
                english: Some(name.clone()),
                native: None,
                romaji: None,
            });

            let air_date = ep.aired.as_deref().and_then(parse_tvdb_date);

            Some(Episode {
                number,
                absolute: ep.absolute_number,
                season_number: ep.season_number,
                titles,
                air_date,
                runtime: ep.runtime,
                overview: ep.overview.clone(),
                ids: Some(EpisodeIds { tvdb: tvdb_id }),
            })
        })
        .collect()
}

// ── Artwork conversion ─────────────────────────────────────────────────────

fn tvdb_artwork_type_to_type(t: u32) -> ArtworkType {
    match t {
        1 | 4 | 9 => ArtworkType::Poster,
        2 => ArtworkType::Fanart,
        3 => ArtworkType::Banner,
        10 => ArtworkType::Clearlogo,
        11 => ArtworkType::Clearlogo,
        12 => ArtworkType::Backdrop,
        _ => ArtworkType::Unknown,
    }
}

fn map_tvdb_artwork(items: &[TvdbArtworkItem]) -> Vec<Artwork> {
    // Take only the top-scored items
    let mut sorted: Vec<_> = items.iter().collect();
    sorted.sort_by(|a, b| {
        b.score
            .unwrap_or(0.0)
            .partial_cmp(&a.score.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    sorted.truncate(MAX_TVDB_ARTWORK * 3); // 3 types × 5 max = 15

    sorted
        .iter()
        .filter_map(|item| {
            let url = item.image.as_ref()?;

            // Extract artwork type ID from either a plain number or `{"id": N, ...}`
            let type_id: Option<u32> = item.artwork_type.as_ref().and_then(|v| {
                if let Some(n) = v.as_u64() {
                    Some(n as u32)
                } else {
                    v.get("id").and_then(|id| id.as_u64().map(|n| n as u32))
                }
            });
            let artwork_type = type_id.map(tvdb_artwork_type_to_type)
                .unwrap_or(ArtworkType::Unknown);

            // TVDB images are served from a CDN; the `image` field is a path
            let full_url = if url.starts_with("http") {
                url.clone()
            } else {
                format!("https://artworks.thetvdb.com{url}")
            };

            Some(Artwork {
                r#type: artwork_type,
                provider: ArtworkProvider::Tvdb,
                url: full_url,
                width: item.width,
                height: item.height,
                language: item.language.clone(),
            })
        })
        .collect()
}

// ── Enrichment function ────────────────────────────────────────────────────

/// Per-entry results from TVDB enrichment.
struct EnrichResult {
    entry: Value,
    /// How many episodes were found?
    episode_count: u64,
    /// Did any fetch fail?
    failure_occurred: bool,
}

/// Fetch TVDB data for a single entry and merge it in.
async fn enrich_entry(
    http: &reqwest::Client,
    api_key: &str,
    token_cache: &Mutex<Option<String>>,
    bucket: &Mutex<TokenBucket>,
    mut entry: Value,
) -> EnrichResult {
    let anilist_id = entry["id"].as_i64().unwrap_or(0) as i32;

    // Check if entry has a TVDB ID
    let tvdb_id = entry["ids"]["tvdb"].as_i64().map(|v| v as i32);
    let Some(series_id) = tvdb_id else {
        return EnrichResult {
            entry,
            episode_count: 0,
            failure_occurred: false,
        };
    };

    // ── Fetch episodes ─────────────────────────────────────────────────
    let episodes = match fetch_all_episodes(http, api_key, series_id, token_cache, bucket).await {
        Ok(eps) => {
            let mapped = map_tvdb_episodes(&eps);
            if mapped.is_empty() { None } else { Some(mapped) }
        }
        Err(e) => {
            let is_404 = e.to_string().contains("HTTP 404");
            if is_404 {
                tracing::debug!("TVDB: entry {anilist_id} (series {series_id}): 404 (not on TVDB)");
            } else {
                tracing::warn!("TVDB: entry {anilist_id} (series {series_id}): {e:#}");
            }
            return EnrichResult {
                entry,
                episode_count: 0,
                failure_occurred: !is_404,
            };
        }
    };

    // ── Fetch artwork (best-effort, failure doesn't abort episode enrich) ──
    let mut artwork_added = false;

    match fetch_artwork(http, api_key, series_id, token_cache, bucket).await {
        Ok(items) => {
            let artworks = map_tvdb_artwork(&items);
            if !artworks.is_empty() {
                merge_artwork(&mut entry, artworks);
                artwork_added = true;
            }
        }
        Err(e) => {
            let is_404 = e.to_string().contains("HTTP 404");
            if !is_404 {
                tracing::warn!("TVDB: entry {anilist_id} artwork: {e:#}");
            }
        }
    }

    // ── Write enriched data ────────────────────────────────────────────
    if let Some(ref eps) = episodes {
        entry["episodes"] = serde_json::to_value(eps).unwrap_or(Value::Null);
    }

    if (episodes.is_some() || artwork_added)
        && let Some(sources) = entry["sources"].as_array_mut()
            && !sources.iter().any(|s| s == "tvdb") {
                sources.push(Value::String("tvdb".to_string()));
            }

    let episode_count = episodes.as_ref().map_or(0, |e| e.len() as u64);

    EnrichResult {
        entry,
        episode_count,
        failure_occurred: false,
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

/// TVDB enrichment phase: fetch episodes and artwork from TheTVDB.
///
/// Uses ordered-buffered concurrency (same pattern as [`TmdbEnrichPhase`]):
/// up to [`TVDB_CONCURRENCY`] entries are in-flight simultaneously, each
/// making its own paginated episode + artwork requests through the shared
/// rate limiter.  The token is cached behind an `Arc<Mutex<>>` so only one
/// worker ever calls `/login`.
///
/// Pass-through: every entry is written through even on failure.
pub struct TvdbEnrichPhase;

#[async_trait]
impl Phase for TvdbEnrichPhase {
    fn id(&self) -> PhaseId { PhaseId::TvdbEnrich }
    fn name(&self) -> &'static str { "TVDB Enrichment" }

    async fn run(&self, config: &PipelineConfig, checkpoint: &mut Checkpoint) -> Result<u64> {
        let phase_id = self.id();
        let api_key = config.tvdb_api_key.as_ref()
            .context("TVDB_API_KEY not set")?;

        let input_path = config.work_dir.join(ANIME_TMDB);
        let output_path = config.work_dir.join(ANIME_TVDB);

        if !input_path.exists() {
            anyhow::bail!("input not found at {}", input_path.display());
        }

        // ── Determine start state ───────────────────────────────────────
        let total_input = count_jsonl_lines(&input_path)?;
        let start_from = if config.resume {
            if checkpoint.is_completed(&phase_id) {
                tracing::info!("TVDB enrichment already completed, skipping");
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

        tracing::info!(
            "TVDB: processing {batch_size} entries (from line {start_from}) at {} req/s, {} concurrent",
            config.tvdb_rate_limit,
            TVDB_CONCURRENCY,
        );

        // ── Shared state ───────────────────────────────────────────────
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let bucket = Arc::new(Mutex::new(TokenBucket::new(config.tvdb_rate_limit)));
        let token_cache = Arc::new(Mutex::new(None::<String>));
        let api_key = api_key.clone();

        // ── Ordered buffered processing ─────────────────────────────────
        // Each stream item returns: (out_line, idx, has_tvdb_id, episode_count, episodes_found, failure)
        let stream = futures::stream::iter(lines.into_iter().enumerate().map(
            move |(offset, line)| {
                let http = http.clone();
                let bucket = Arc::clone(&bucket);
                let token_cache = Arc::clone(&token_cache);
                let api_key = api_key.clone();

                async move {
                    let idx = start_from + offset as u64;

                    // Ensure we have a token before processing
                    if let Err(e) = ensure_token(&http, &api_key, &token_cache, &bucket).await {
                        tracing::warn!("TVDB: token acquisition failed at line {idx}: {e:#}");
                        return (line, idx, false, 0u64, 0u64, 1u64);
                    }

                    let entry: Value = match serde_json::from_str(&line) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("TVDB: malformed JSON at line {idx}: {e}");
                            return (line, idx, false, 0u64, 0u64, 1u64);
                        }
                    };

                    let has_tvdb_id = entry["ids"]["tvdb"].is_number();
                    if !has_tvdb_id {
                        return (line, idx, false, 0u64, 0u64, 0u64);
                    }

                    let result = enrich_entry(&http, &api_key, &token_cache, &bucket, entry).await;
                    let out_line = serde_json::to_string(&result.entry)
                        .unwrap_or_else(|_| line.clone());

                    let found = if result.episode_count > 0 { 1u64 } else { 0u64 };
                    let failure = if result.failure_occurred { 1u64 } else { 0u64 };

                    (out_line, idx, true, result.episode_count, found, failure)
                }
            },
        ))
        .buffered(TVDB_CONCURRENCY);

        // ── Write results ──────────────────────────────────────────────
        let mut writer = if start_from > 0 {
            JsonlWriter::append(&output_path)?
        } else {
            JsonlWriter::new(&output_path)?
        };

        let mut stats = TvdbPhaseStats::default();
        let mut processed = 0u64;

        tokio::pin!(stream);
        while let Some((out_line, idx, has_id, ep_count, found, failure)) = stream.next().await {
            writer.write_raw(&out_line).context(format!("writing line {idx}"))?;
            processed += 1;

            stats.total_input += 1;
            if has_id { stats.with_ids += 1; }
            stats.total_episodes += ep_count;
            stats.episodes_found += found;
            stats.failures += failure;

            // Progress every 500 entries
            if processed.is_multiple_of(500) {
                let written = start_from + processed;
                if let crate::checkpoint::PhaseState::Simple(s) =
                    checkpoint.phases.get_mut(&phase_id).unwrap() { s.completed = written; }
                tracing::info!(
                    "TVDB: {}/{}  (with_ids={} episodes_found={} total_eps={} failures={})",
                    written, stats.total_input,
                    stats.with_ids, stats.episodes_found,
                    stats.total_episodes, stats.failures,
                );
            }
        }

        writer.flush()?;
        let total_written = start_from + processed;

        tracing::info!(
            "TVDB complete: {} input, {} written.  TVDB IDs: {}, eps found: {} entries ({} total episodes), failures: {}",
            stats.total_input, total_written,
            stats.with_ids, stats.episodes_found, stats.total_episodes, stats.failures,
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
        let bucket = TokenBucket::new(2.0);
        assert!((bucket.tokens - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_token_bucket_can_consume() {
        let mut bucket = TokenBucket::new(2.0);
        assert!(bucket.try_consume());
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume());
    }

    // ── Date parsing ───────────────────────────────────────────────────

    #[test]
    fn test_parse_tvdb_date_full() {
        let d = parse_tvdb_date("2022-01-15").unwrap();
        assert_eq!(d.year, 2022);
        assert_eq!(d.month, Some(1));
        assert_eq!(d.day, Some(15));
    }

    #[test]
    fn test_parse_tvdb_date_invalid() {
        assert!(parse_tvdb_date("not-a-date").is_none());
        assert!(parse_tvdb_date("2022-01").is_none());
        assert!(parse_tvdb_date("").is_none());
    }

    // ── Episode mapping ─────────────────────────────────────────────────

    #[test]
    fn test_map_tvdb_episodes_basic() {
        let tvdb_eps = vec![
            TvdbEpisode {
                id: Some(100),
                number: Some(1),
                absolute_number: Some(1),
                season_number: Some(1),
                name: Some("Episode 1".to_string()),
                overview: Some("First episode".to_string()),
                aired: Some("2022-01-15".to_string()),
                runtime: Some(24),
            },
            TvdbEpisode {
                id: Some(101),
                number: Some(2),
                absolute_number: None,
                season_number: Some(1),
                name: Some("Episode 2".to_string()),
                overview: None,
                aired: Some("2022-01-22".to_string()),
                runtime: Some(24),
            },
        ];

        let eps = map_tvdb_episodes(&tvdb_eps);
        assert_eq!(eps.len(), 2);

        assert_eq!(eps[0].number, 1);
        assert_eq!(eps[0].absolute, Some(1));
        assert_eq!(eps[0].season_number, Some(1));
        assert_eq!(eps[0].titles.as_ref().unwrap().english.as_deref(), Some("Episode 1"));
        assert_eq!(eps[0].overview.as_deref(), Some("First episode"));
        assert_eq!(eps[0].air_date.as_ref().unwrap().year, 2022);
        assert_eq!(eps[0].runtime, Some(24));
        assert_eq!(eps[0].ids.as_ref().unwrap().tvdb, 100);
    }

    #[test]
    fn test_map_tvdb_episodes_skips_missing_number_or_id() {
        let tvdb_eps = vec![
            TvdbEpisode { id: None, number: Some(1), ..Default::default() },
            TvdbEpisode { id: Some(100), number: None, ..Default::default() },
            TvdbEpisode { id: Some(101), number: Some(3), ..Default::default() },
        ];

        let eps = map_tvdb_episodes(&tvdb_eps);
        assert_eq!(eps.len(), 1);
        assert_eq!(eps[0].number, 3);
    }

    #[test]
    fn test_map_tvdb_episodes_empty() {
        let eps = map_tvdb_episodes(&[]);
        assert!(eps.is_empty());
    }

    // ── Artwork mapping ────────────────────────────────────────────────

    #[test]
    fn test_tvdb_artwork_type_to_type() {
        assert_eq!(format!("{:?}", tvdb_artwork_type_to_type(1)), "Poster");
        assert_eq!(format!("{:?}", tvdb_artwork_type_to_type(2)), "Fanart");
        assert_eq!(format!("{:?}", tvdb_artwork_type_to_type(3)), "Banner");
        assert_eq!(format!("{:?}", tvdb_artwork_type_to_type(10)), "Clearlogo");
        assert_eq!(format!("{:?}", tvdb_artwork_type_to_type(12)), "Backdrop");
        assert_eq!(format!("{:?}", tvdb_artwork_type_to_type(99)), "Unknown");
    }

    #[test]
    fn test_map_tvdb_artwork_empty() {
        let artworks = map_tvdb_artwork(&[]);
        assert!(artworks.is_empty());
    }

    #[test]
    fn test_map_tvdb_artwork_basic() {
        let items = vec![
            TvdbArtworkItem {
                image: Some("/banners/poster.jpg".to_string()),
                artwork_type: Some(serde_json::json!(1)),
                width: Some(500),
                height: Some(750),
                language: Some("eng".to_string()),
                score: Some(8.0),
            }
        ];

        let artworks = map_tvdb_artwork(&items);
        assert_eq!(artworks.len(), 1);
        assert_eq!(format!("{:?}", artworks[0].r#type), "Poster");
        assert_eq!(format!("{:?}", artworks[0].provider), "Tvdb");
        assert!(artworks[0].url.contains("artworks.thetvdb.com"));
        assert_eq!(artworks[0].width, Some(500));
    }

    // ── Phase identity ─────────────────────────────────────────────────

    #[test]
    fn test_tvdb_enrich_phase_id() {
        let phase = TvdbEnrichPhase;
        assert_eq!(phase.id(), PhaseId::TvdbEnrich);
        assert_eq!(phase.name(), "TVDB Enrichment");
    }
}
