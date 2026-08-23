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

// ── Show caching + season attribution ────────────────────────────────────

/// Per-show data cached across sibling AniList entries that share one TMDB
/// show id (franchises such as *Initial D* or *Naruto*).
#[derive(Default)]
struct CachedShow {
    images: TmdbImages,
    /// Season number → that season's episodes (sorted).
    episodes_by_season: std::collections::BTreeMap<i32, Vec<Episode>>,
    episode_total: usize,
    failure_occurred: bool,
}

/// Fetch a show's artwork and its complete multi-season episode list once.
async fn fetch_full_show(
    http: &reqwest::Client,
    api_key: &str,
    bucket: &Mutex<TokenBucket>,
    tv_id: i32,
) -> Result<CachedShow> {
    let mut show = CachedShow::default();
    if let Some(resp) = fetch_tv(http, api_key, bucket, tv_id).await? {
        show.images = resp.images;
        if let Err(e) = fetch_show_episodes(http, api_key, bucket, tv_id, &resp.seasons).await {
            tracing::warn!("TMDB TV {tv_id}: episode fetch failed: {e:#}");
            show.failure_occurred = true;
        } else {
            show.episode_total = show.episodes_by_season.values().map(Vec::len).sum();
        }
    } // else: 404 — stale cross-reference
    Ok(show)
}

/// Comparable ordinal for a serialized fuzzy date ({year, month?, day?}).
/// Range-end dates default to Dec 31, range-start to Jan 1.
/// A resolved calendar position as (year, month, day).
type DateOrd = (i32, i32, i32);

fn fuzzy_ord(v: Option<&Value>, is_end: bool) -> Option<DateOrd> {
    let v = v?;
    let y = v.get("year")?.as_i64()? as i32;
    let m = v.get("month").and_then(Value::as_i64).map(|m| m as i32)
        .unwrap_or(if is_end { 12 } else { 1 });
    let d = v.get("day").and_then(Value::as_i64).map(|d| d as i32)
        .unwrap_or(if is_end { 31 } else { 1 });
    Some((y, m, d))
}

fn episode_air_ord(ep: &Episode) -> Option<DateOrd> {
    ep.air_date
        .as_ref()
        .map(|f| (f.year, f.month.unwrap_or(1), f.day.unwrap_or(1)))
}

/// An entry's [start, end] airing window.  A missing end date means
/// open-ended (RELEASING / long-runners like One Piece) and extends to the
/// far future; a missing start extends to the far past.
fn entry_window(entry: &Value) -> Option<(DateOrd, DateOrd)> {
    let dates = entry.get("dates")?;
    if dates.get("start").is_none() && dates.get("end").is_none() {
        return None;
    }
    let s = fuzzy_ord(dates.get("start"), false).unwrap_or((0, 1, 1));
    let e = fuzzy_ord(dates.get("end"), true).unwrap_or((9999, 12, 31));
    Some((s.min(e), s.max(e)))
}

/// Attribute TMDB seasons to sibling AniList entries sharing one show id.
///
/// A season is claimed by every entry whose airing window contains ≥40% of
/// the season's dated episodes — split-cour siblings share a season while
/// unrelated franchise entries stay out.  Entries without air dates only win
/// when they are the show's sole non-movie claimant; MOVIE-format entries
/// never claim TV seasons.
fn attribute_seasons(
    seasons: &std::collections::BTreeMap<i32, Vec<Episode>>,
    entries: &[(usize, &Value)],
) -> Vec<Option<Vec<Episode>>> {
    let mut out: Vec<Option<Vec<Episode>>> = vec![None; entries.len()];

    let eligible: Vec<usize> = (0..entries.len())
        .filter(|&i| entries[i].1.get("format").and_then(Value::as_str) != Some("MOVIE"))
        .collect();
    if eligible.is_empty() {
        return out;
    }

    let windows: Vec<Option<(DateOrd, DateOrd)>> =
        entries.iter().map(|(_, e)| entry_window(e)).collect();

    // Sole claimant with no air dates at all: hand over the entire show.
    // (With dates, the general ≥40% window rule below applies so a lone
    // entry mapped to a multi-season franchise show doesn't inherit every
    // season — e.g. AoT S1 mapped to the show containing S1–S4.)
    if eligible.len() == 1 && windows[eligible[0]].is_none() {
        let all: Vec<Episode> = seasons.values().flatten().cloned().collect();
        if !all.is_empty() {
            out[eligible[0]] = Some(all);
        }
        return out;
    }

    for eps in seasons.values() {
        let dated = eps.iter().filter(|e| e.air_date.is_some()).count();
        if dated == 0 {
            continue; // cannot place a fully undated season among siblings
        }
        let threshold = ((dated * 2) / 5).max(1); // ≥40% of dated episodes
        for &ei in &eligible {
            let Some((w0, w1)) = windows[ei] else { continue };
            let inside = eps
                .iter()
                .filter(|e| matches!(episode_air_ord(e), Some(d) if d >= w0 && d <= w1))
                .count();
            if inside >= threshold {
                out[ei].get_or_insert_with(Vec::new).extend(eps.iter().cloned());
            }
        }
    }

    for v in out.iter_mut().flatten() {
        v.sort_by_key(|e| (e.season_number.unwrap_or(1), e.number));
    }
    out
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

        let total_input = count_jsonl_lines(&input_path)?;
        checkpoint.phases.insert(
            phase_id.clone(),
            crate::checkpoint::PhaseState::Simple(
                crate::checkpoint::SimpleState::new(total_input),
            ),
        );

        let start = Instant::now();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("building HTTP client")?;
        let bucket = Arc::new(Mutex::new(TokenBucket::new(TMDB_RATE_LIMIT)));
        let api_key = Arc::new(api_key.clone());

        // ── Parse input ─────────────────────────────────────────────────
        let reader = std::fs::File::open(&input_path)
            .with_context(|| format!("opening {} for reading", input_path.display()))?;
        let raw_lines: Vec<String> = std::io::BufRead::lines(std::io::BufReader::new(reader))
            .map_while(|l| l.ok())
            .collect();

        let mut entries: Vec<Value> = Vec::with_capacity(raw_lines.len());
        let mut malformed = 0u64;
        for line in &raw_lines {
            match serde_json::from_str::<Value>(line) {
                Ok(v) => entries.push(v),
                Err(e) => {
                    malformed += 1;
                    tracing::warn!("TMDB: malformed JSON line dropped: {e}");
                }
            }
        }

        // ── Group sibling entries by shared TMDB show id ────────────────
        let mut groups: std::collections::HashMap<i32, Vec<usize>> =
            std::collections::HashMap::new();
        let mut movie_refs: Vec<(usize, i32)> = Vec::new(); // (entry idx, movie id)
        for (i, e) in entries.iter().enumerate() {
            if let Some(tv) = e["ids"]["tmdbTv"].as_i64() {
                groups.entry(tv as i32).or_default().push(i);
            }
            if let Some(ms) = e["ids"]["tmdbMovie"].as_array() {
                for m in ms {
                    if let Some(mid) = m.as_i64() {
                        movie_refs.push((i, mid as i32));
                    }
                }
            }
        }
        let show_ids: Vec<i32> = groups.keys().copied().collect();
        tracing::info!(
            "TMDB: {} entries → {} unique shows, {} movie-id refs",
            entries.len(),
            show_ids.len(),
            movie_refs.len()
        );

        // ── Fetch each unique show once: images + all-season episodes ───
        let mut shows: std::collections::HashMap<i32, CachedShow> =
            std::collections::HashMap::new();
        let mut show_failures = 0u64;

        let fetch_futs = show_ids.iter().copied().map(|tid| {
            let http = http.clone();
            let bucket = Arc::clone(&bucket);
            let key = Arc::clone(&api_key);
            async move { (tid, fetch_full_show(&http, &key, &bucket, tid).await) }
        });
        let fetched = futures::StreamExt::buffered(
            futures::stream::iter(fetch_futs),
            TMDB_CONCURRENCY,
        )
        .collect::<Vec<_>>()
        .await;
        for (tid, res) in fetched {
            match res {
                Ok(show) => {
                    if show.failure_occurred {
                        show_failures += 1;
                    }
                    shows.insert(tid, show);
                }
                Err(e) => {
                    show_failures += 1;
                    tracing::warn!("TMDB: show {tid} failed: {e:#}");
                }
            }
        }

        // ── Attribute seasons within each sibling group ─────────────────
        let mut assigned: Vec<Option<Vec<Episode>>> = vec![None; entries.len()];
        for (&tv_id, recs) in &groups {
            let Some(show) = shows.get(&tv_id) else { continue };
            let refs: Vec<(usize, &Value)> =
                recs.iter().map(|&ri| (ri, &entries[ri])).collect();
            let got = attribute_seasons(&show.episodes_by_season, &refs);
            for (k, &ri) in recs.iter().enumerate() {
                assigned[ri] = got[k].clone();
            }
        }

        // ── Prefetch unique movie images ────────────────────────────────
        let unique_movies: Vec<i32> = {
            let mut m: Vec<i32> = movie_refs.iter().map(|(_, id)| *id).collect();
            m.sort_unstable();
            m.dedup();
            m
        };
        let mut movie_images: std::collections::HashMap<i32, TmdbImages> =
            std::collections::HashMap::new();
        let movie_futs = unique_movies.iter().copied().map(|mid| {
            let http = http.clone();
            let bucket = Arc::clone(&bucket);
            let key = Arc::clone(&api_key);
            async move { (mid, fetch_movie(&http, &key, &bucket, mid).await) }
        });
        let movie_results = futures::StreamExt::buffered(
            futures::stream::iter(movie_futs),
            TMDB_CONCURRENCY,
        )
        .collect::<Vec<_>>()
        .await;
        let mut movie_image_failures = 0u64;
        for (mid, res) in movie_results {
            match res {
                Ok(Some(resp)) => {
                    movie_images.insert(mid, resp.images);
                }
                Ok(None) => {}
                Err(e) => {
                    movie_image_failures += 1;
                    tracing::warn!("TMDB: movie {mid} images failed: {e:#}");
                }
            }
        }

        // ── Apply enrichment and write results in original order ────────
        let mut writer = JsonlWriter::new(&output_path)?;
        let mut stats = TmdbPhaseStats::default();
        let mut total_episodes = 0u64;
        let mut entries_with_episodes = 0u64;
        let mut processed = 0u64;

        for (i, entry) in entries.iter_mut().enumerate() {
            let tv_id = entry["ids"]["tmdbTv"].as_i64().map(|v| v as i32);
            let has_movie_ids = entry["ids"]["tmdbMovie"]
                .as_array()
                .is_some_and(|a| !a.is_empty());

            let mut artwork_added = false;
            let mut pc = 0usize;
            let mut bc = 0usize;
            let mut lc = 0usize;
            let mut ser_fail = false;
            let mut eps_attached = 0usize;

            if let Some(t) = tv_id
                && let Some(show) = shows.get(&t)
            {
                let (arts, p, b, l) = tmdb_images_to_artwork(&show.images);
                pc = p;
                bc = b;
                lc = l;
                if !arts.is_empty() {
                    merge_artwork(entry, arts);
                    artwork_added = true;
                }
            }

            let movie_ids: Vec<i32> = entry["ids"]["tmdbMovie"]
                .as_array()
                .map(|a| a.iter().filter_map(Value::as_i64).map(|v| v as i32).collect())
                .unwrap_or_default();
            for mid in movie_ids {
                if let Some(images) = movie_images.get(&mid) {
                    let (arts, p, b, l) = tmdb_images_to_artwork(images);
                    pc += p;
                    bc += b;
                    lc += l;
                    if !arts.is_empty() {
                        merge_artwork(entry, arts);
                        artwork_added = true;
                    }
                }
            }

            if let Some(eps) = &assigned[i]
                && !eps.is_empty()
            {
                eps_attached = eps.len();
                match serde_json::to_value(eps) {
                    Ok(v) => entry["episodes"] = v,
                    Err(e) => {
                        tracing::warn!("TMDB: episode serialize failed: {e}");
                        ser_fail = true;
                    }
                }
            }

            if (artwork_added || eps_attached > 0)
                && let Some(sources) = entry["sources"].as_array_mut()
                && !sources.iter().any(|s| s == "tmdb")
            {
                sources.push(Value::String("tmdb".to_string()));
            }

            stats.total_input += 1;
            if tv_id.is_some() {
                stats.with_tv_ids += 1;
            }
            if has_movie_ids {
                stats.with_movie_ids += 1;
            }
            if artwork_added {
                stats.artwork_found += 1;
            } else if tv_id.is_some() || has_movie_ids {
                stats.no_artwork += 1;
            }
            stats.total_poster_count += pc as u64;
            stats.total_backdrop_count += bc as u64;
            stats.total_logo_count += lc as u64;
            if ser_fail {
                stats.failures += 1;
            }
            total_episodes += eps_attached as u64;
            if eps_attached > 0 {
                entries_with_episodes += 1;
            }

            writer.write(entry).context("writing enriched entry")?;
            processed += 1;

            if processed.is_multiple_of(1000) {
                if let crate::checkpoint::PhaseState::Simple(s) =
                    checkpoint.phases.get_mut(&phase_id).unwrap()
                {
                    s.completed = processed;
                }
                crate::progress::log_progress(
                    "TMDB",
                    processed,
                    total_input,
                    Some(start),
                    &[
                        ("shows", show_ids.len() as u64),
                        ("artwork", stats.artwork_found),
                        ("episodes", total_episodes),
                        ("failures", stats.failures),
                    ],
                );
            }
        }

        writer.flush()?;
        stats.failures += show_failures + movie_image_failures;

        tracing::info!(
            "TMDB complete: {} input, {} written ({} malformed dropped).  Shows: {}, TV IDs: {}, Movie IDs: {}, \
             artwork found: {}, images: {}p+{}b+{}l, episodes: {} across {} shows, failures: {} ({})",
            total_input,
            processed,
            malformed,
            show_ids.len(),
            stats.with_tv_ids,
            stats.with_movie_ids,
            stats.artwork_found,
            stats.total_poster_count,
            stats.total_backdrop_count,
            stats.total_logo_count,
            total_episodes,
            entries_with_episodes,
            stats.failures,
            show_failures + movie_image_failures,
        );

        if let crate::checkpoint::PhaseState::Simple(s) =
            checkpoint.phases.get_mut(&phase_id).unwrap()
        {
            s.completed = processed;
        }
        checkpoint.complete_simple(&phase_id);
        checkpoint.save(&config.checkpoint_path).context("saving checkpoint")?;

        Ok(processed)
    }
}

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

    // ── Season attribution ────────────────────────────────────────────

    fn dep(season: i32, number: i32, y: i32, m: i32, d: i32) -> Episode {
        Episode {
            number,
            season_number: Some(season),
            air_date: Some(FuzzyDate { year: y, month: Some(m), day: Some(d) }),
            ..Default::default()
        }
    }

    fn entry_val(format: &str, dates: Value) -> Value {
        serde_json::json!({ "format": format, "dates": dates })
    }

    fn counts(res: &[Option<Vec<Episode>>]) -> Vec<usize> {
        res.iter().map(|o| o.as_ref().map_or(0, |v| v.len())).collect()
    }

    #[test]
    fn test_attribution_sole_undated_entry_gets_everything() {
        let mut seasons = std::collections::BTreeMap::new();
        seasons.insert(0, vec![dep(0, 1, 2010, 1, 1), dep(0, 2, 2010, 2, 1)]);
        seasons.insert(1, vec![dep(1, 1, 2010, 4, 1), dep(1, 2, 2010, 4, 8), dep(1, 3, 2010, 4, 15)]);
        let e0 = entry_val("TV", serde_json::json!({}));
        let entries = vec![(0usize, &e0)];
        let res = attribute_seasons(&seasons, &entries);
        assert_eq!(counts(&res), vec![5]);
    }

    #[test]
    fn test_attribution_open_ended_long_runner_claims_all() {
        let mut seasons = std::collections::BTreeMap::new();
        seasons.insert(1, vec![dep(1, 1, 1999, 11, 1)]);
        seasons.insert(9, vec![dep(9, 1, 2024, 6, 1)]); // far-future season arc
        let e0 = entry_val("TV", serde_json::json!({
            "start": {"year": 1999, "month": 10}
        }));
        let entries = vec![(0usize, &e0)];
        let res = attribute_seasons(&seasons, &entries);
        assert_eq!(counts(&res), vec![2], "open-ended end must claim future seasons");
    }

    #[test]
    fn test_attribution_disjoint_siblings_slice_their_own_seasons() {
        let mut seasons = std::collections::BTreeMap::new();
        seasons.insert(1, vec![dep(1, 1, 2019, 1, 5), dep(1, 2, 2019, 2, 5)]);
        seasons.insert(2, vec![dep(2, 1, 2020, 1, 5), dep(2, 2, 2020, 2, 5)]);
        let e0 = entry_val("TV", serde_json::json!({
            "start": {"year": 2019, "month": 1}, "end": {"year": 2019, "month": 6}}));
        let e1 = entry_val("TV", serde_json::json!({
            "start": {"year": 2020, "month": 1}, "end": {"year": 2020, "month": 6}}));
        let entries = vec![(0usize, &e0), (1usize, &e1)];
        let res = attribute_seasons(&seasons, &entries);
        assert_eq!(counts(&res), vec![2, 2]);
        assert_eq!(res[0].as_ref().unwrap()[0].season_number, Some(1));
        assert_eq!(res[1].as_ref().unwrap()[0].season_number, Some(2));
    }

    #[test]
    fn test_attribution_movies_never_claim_seasons() {
        let mut seasons = std::collections::BTreeMap::new();
        seasons.insert(1, vec![dep(1, 1, 2019, 1, 5)]);
        let e0 = entry_val("MOVIE", serde_json::json!({}));
        let entries = vec![(0usize, &e0)];
        let res = attribute_seasons(&seasons, &entries);
        assert_eq!(counts(&res), vec![0]);
    }

    #[test]
    fn test_attribution_undated_multi_group_yields_nothing() {
        let mut seasons = std::collections::BTreeMap::new();
        seasons.insert(1, vec![dep(1, 1, 2019, 1, 5)]);
        let e0 = entry_val("TV", serde_json::json!({}));
        let e1 = entry_val("TV", serde_json::json!({}));
        let entries = vec![(0usize, &e0), (1usize, &e1)];
        let res = attribute_seasons(&seasons, &entries);
        assert_eq!(counts(&res), vec![0, 0]);
    }

    #[test]
    fn test_attribution_split_cour_siblings_share_a_season() {
        let mut seasons = std::collections::BTreeMap::new();
        seasons.insert(1, vec![
            dep(1, 1, 2021, 1, 10), dep(1, 2, 2021, 2, 10),
            dep(1, 3, 2021, 3, 10), dep(1, 4, 2021, 4, 10),
        ]);
        let e0 = entry_val("TV", serde_json::json!({
            "start": {"year": 2021, "month": 1}, "end": {"year": 2021, "month": 2}}));
        let e1 = entry_val("TV", serde_json::json!({
            "start": {"year": 2021, "month": 3}, "end": {"year": 2021, "month": 4}}));
        let entries = vec![(0usize, &e0), (1usize, &e1)];
        let res = attribute_seasons(&seasons, &entries);
        assert_eq!(counts(&res), vec![4, 4], "split-cour siblings share the season");
    }

    #[test]
    fn test_attribution_unrelated_window_claims_nothing() {
        let mut seasons = std::collections::BTreeMap::new();
        seasons.insert(1, vec![dep(1, 1, 2019, 1, 5), dep(1, 2, 2019, 2, 5)]);
        let e0 = entry_val("TV", serde_json::json!({
            "start": {"year": 2019, "month": 1}, "end": {"year": 2019, "month": 6}}));
        let e1 = entry_val("OVA", serde_json::json!({
            "start": {"year": 2030, "month": 1}, "end": {"year": 2030, "month": 6}}));
        let entries = vec![(0usize, &e0), (1usize, &e1)];
        let res = attribute_seasons(&seasons, &entries);
        assert_eq!(counts(&res), vec![2, 0]);
    }

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
