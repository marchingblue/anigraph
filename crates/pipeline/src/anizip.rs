//! ani.zip enrichment phase.
//!
//! [ani.zip](https://api.ani.zip) provides AniDB-derived per-episode metadata
//! keyed by AniDB ID: multilingual titles (`en`, `ja`, `x-jat` romaji, …),
//! TVDB-sourced overviews, AniDB summaries, ratings, air dates, runtimes and
//! still images.
//!
//! This phase merges that data into every entry carrying an `ids.anidb`
//! cross-reference:
//!
//! - Episodes fetched by the TMDB phase are *filled in* — missing titles,
//!   overviews, ratings, dates and stills are added; existing values win.
//! - Entries without any episodes yet (shows absent from TMDB) gain their
//!   full episode list straight from ani.zip.
//! - Duplicate `(seasonNumber, number)` pairs are collapsed and the final
//!   list is sorted by `(seasonNumber, number)` — specials live in season 0.
//!
//! The upstream site has no documented owner or SLA ("ghost site"), so this
//! phase is strictly best-effort: failures pass entries through unchanged and
//! never fail the pipeline.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;

use anigraph_model::{Episode, EpisodeRating, EpisodeTitle, FuzzyDate};

use crate::checkpoint::{Checkpoint, PhaseId};
use crate::jsonl_writer::JsonlWriter;
use crate::phase::{Phase, PipelineConfig};

// ── Constants ──────────────────────────────────────────────────────────────

const ANIZIP_BASE_URL: &str = "https://api.ani.zip/mappings";
/// ani.zip blocks requests without a User-Agent header outright.
const ANIZIP_USER_AGENT: &str = concat!("anigraph/", env!("CARGO_PKG_VERSION"));
const ANIZIP_CONCURRENCY: usize = 5;
const ANIZIP_RATE_LIMIT: f64 = 8.0;
const ANIZIP_MAX_RETRIES: u32 = 2;
const ANIME_TVDB: &str = "anime-tvdb.jsonl";
const ANIME_TMDB: &str = "anime-tmdb.jsonl";
const ANIME_CROSSREF: &str = "anime-crossref.jsonl";
const ANIME_ANIZIP: &str = "anime-anizip.jsonl";

// ── Token-bucket rate limiter (same design as tmdb/tvdb) ───────────────────

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

// ── API response types ─────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
struct AnizipResponse {
    #[serde(default)]
    episodes: std::collections::BTreeMap<String, AnizipEpisode>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnizipEpisode {
    #[serde(default)]
    season_number: Option<i32>,
    #[serde(default)]
    episode_number: Option<i32>,
    #[serde(default)]
    absolute_episode_number: Option<i32>,
    #[serde(default)]
    title: Option<std::collections::HashMap<String, String>>,
    /// TVDB-derived synopsis.
    #[serde(default)]
    overview: Option<String>,
    /// AniDB-derived synopsis.
    #[serde(default)]
    summary: Option<String>,
    /// AniDB rating, e.g. `"7.02"`.
    #[serde(default)]
    rating: Option<String>,
    /// Air date, e.g. `"1998-10-24"` (`airDate` in newer payloads).
    #[serde(default)]
    air_date: Option<String>,
    #[serde(default)]
    airdate: Option<String>,
    /// Length in minutes (may be fractional).
    #[serde(default)]
    length: Option<f64>,
    #[serde(default)]
    runtime: Option<i32>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    anidb_eid: Option<i64>,
}

impl AnizipEpisode {
    fn air_date(&self) -> Option<&String> {
        self.air_date.as_ref().or(self.airdate.as_ref())
    }

    fn best_overview(&self) -> Option<String> {
        let ov = self.overview.as_deref().map(str::trim).filter(|s| !s.is_empty());
        let sum = self.summary.as_deref().map(str::trim).filter(|s| !s.is_empty());
        ov.or(sum).map(String::from)
    }

    fn title(&self, lang: &str) -> Option<String> {
        self.title
            .as_ref()?
            .get(lang)
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(String::from)
    }

    fn rating(&self) -> Option<EpisodeRating> {
        let raw = self.rating.as_deref()?.trim();
        if raw.is_empty() {
            return None;
        }
        // Ratings come as strings like "7.02".
        match raw.parse::<f64>() {
            Ok(avg) if (0.0..=10.0).contains(&avg) => {
                Some(EpisodeRating { average: avg, count: None })
            }
            _ => None,
        }
    }

    fn runtime_minutes(&self) -> Option<i32> {
        if let Some(rt) = self.runtime {
            return Some(rt);
        }
        self.length.map(|l| l.round() as i32).filter(|&m| m > 0)
    }

    fn date(&self) -> Option<FuzzyDate> {
        let s = self.air_date()?;
        let mut parts = s.split('-');
        let year: i32 = parts.next()?.parse().ok()?;
        let month: Option<i32> = parts.next().and_then(|m| m.parse().ok());
        let day: Option<i32> = parts.next().and_then(|d| d.parse().ok());
        Some(FuzzyDate { year, month, day })
    }
}

// ── Fetch ──────────────────────────────────────────────────────────────────

async fn fetch_with_retry(
    http: &reqwest::Client,
    url: &str,
    bucket: &Mutex<TokenBucket>,
    label: &str,
) -> Result<Option<AnizipResponse>> {
    let mut attempt = 0u32;
    loop {
        while !bucket.lock().await.try_consume() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let resp = http
            .get(url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .with_context(|| format!("{label}: request failed"))?;

        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        match resp.error_for_status() {
            Ok(resp) => {
                return Some(resp.json().await.with_context(|| format!("{label}: parse failed")))
                    .transpose();
            }
            Err(e) => {
                if attempt < ANIZIP_MAX_RETRIES {
                    attempt += 1;
                    let delay = Duration::from_secs(2u64.pow(attempt));
                    tracing::warn!("{label}: {e}, retry {attempt}/{}", ANIZIP_MAX_RETRIES);
                    tokio::time::sleep(delay).await;
                } else {
                    anyhow::bail!("{label}: failed after {} retries: {e}", ANIZIP_MAX_RETRIES);
                }
            }
        }
    }
}

// ── Mapping / merging ──────────────────────────────────────────────────────

/// Parse an ani.zip episode key. Numeric keys are regular episodes; `S<n>`
/// keys are specials (mapped to season 0); anything else (`P` parodies etc.)
/// is ignored.
fn parse_episode_key(key: &str) -> Option<(i32, i32)> {
    let key = key.trim();
    if let Ok(n) = key.parse::<i32>() {
        return Some((1, n));
    }
    let rest = key.strip_prefix('S')?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse::<i32>().ok().map(|n| (0, n))
}

fn non_empty(s: Option<String>) -> Option<String> {
    s.filter(|v| !v.trim().is_empty())
}

/// Apply ani.zip data onto one existing episode, filling only empty fields.
fn fill_episode(ep: &mut Episode, az: &AnizipEpisode) {
    if ep.titles.is_none() {
        ep.titles = Some(EpisodeTitle::default());
    }
    if let Some(titles) = ep.titles.as_mut() {
        if titles.english.is_none() {
            titles.english = non_empty(az.title("en"));
        }
        if titles.native.is_none() {
            titles.native = non_empty(az.title("ja"));
        }
        if titles.romaji.is_none() {
            titles.romaji = non_empty(az.title("x-jat"));
        }
        // Drop a titles object if it stayed entirely empty.
        if titles.english.is_none() && titles.native.is_none() && titles.romaji.is_none() {
            ep.titles = None;
        }
    }
    if ep.overview.is_none() {
        ep.overview = non_empty(az.best_overview());
    }
    if ep.rating.is_none() {
        ep.rating = az.rating();
    }
    if ep.air_date.is_none() {
        ep.air_date = az.date();
    }
    if ep.runtime.is_none() {
        ep.runtime = az.runtime_minutes();
    }
    if ep.image.is_none() {
        ep.image = non_empty(az.image.clone());
    }
    if let Some(eid) = az.anidb_eid {
        let ids = ep.ids.get_or_insert_with(Default::default);
        if ids.anidb.is_none() {
            ids.anidb = Some(i32::try_from(eid).unwrap_or(0));
        }
    }
    if ep.absolute.is_none() {
        ep.absolute = az.absolute_episode_number;
    }
}

/// Build a brand-new episode from an ani.zip record (for shows whose entries
/// have no episodes yet).
fn episode_from_anizip(season: i32, number: i32, az: &AnizipEpisode) -> Episode {
    let mut ep = Episode {
        number,
        absolute: az.absolute_episode_number,
        season_number: Some(season),
        ..Default::default()
    };
    fill_episode(&mut ep, az);
    ep
}

/// Merge ani.zip episode data into an entry's `episodes` array.
///
/// Returns true when anything changed.
fn merge_anizip_episodes(entry: &mut Value, resp: &AnizipResponse) -> bool {
    if resp.episodes.is_empty() {
        return false;
    }

    let existing: Vec<Episode> = match entry["episodes"].take() {
        Value::Null => Vec::new(),
        v => serde_json::from_value(v).unwrap_or_default(),
    };

    let mut merged: Vec<(i32, i32, Episode)> = existing
        .into_iter()
        .map(|ep| (ep.season_number.unwrap_or(1), ep.number, ep))
        .collect();

    for (key, az) in &resp.episodes {
        // Prefer explicit numbering from the payload (TVDB-based), falling
        // back to parsing the anidb-style key ("1", "S1", …).
        let (season, number) = match (az.season_number, az.episode_number) {
            (Some(s), Some(n)) => (s, n),
            _ => match parse_episode_key(key) {
                Some(k) => k,
                None => continue,
            },
        };

        if let Some(slot) = merged.iter_mut().find(|(s, n, _)| *s == season && *n == number) {
            fill_episode(&mut slot.2, az);
        } else {
            merged.push((season, number, episode_from_anizip(season, number, az)));
        }
    }

    // Collapse duplicate (season, number) pairs — keep the record that has a
    // title — then sort specials (season 0) first and by number.
    merged.sort_by_key(|(s, n, ep)| (*s, *n, ep.titles.is_none()));
    merged.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
    merged.sort_by_key(|(s, n, _)| (*s, *n));

    let episodes: Vec<Episode> = merged.into_iter().map(|(_, _, ep)| ep).collect();
    entry["episodes"] = serde_json::to_value(&episodes).expect("episodes always serialize");
    true
}

// ── Enrichment ─────────────────────────────────────────────────────────────

/// Per-entry enrichment result.
struct EnrichResult {
    entry: Value,
    /// Did ani.zip contribute anything?
    enriched: bool,
    /// Total episodes on the entry after merging.
    episode_count: usize,
    failure_occurred: bool,
}

async fn enrich_entry(
    http: &reqwest::Client,
    bucket: &Mutex<TokenBucket>,
    mut entry: Value,
) -> EnrichResult {
    let anidb_id = entry["ids"]["anidb"].as_i64();

    let Some(anidb_id) = anidb_id else {
        return EnrichResult { entry, enriched: false, episode_count: 0, failure_occurred: false };
    };

    let url = format!("{ANIZIP_BASE_URL}?anidb_id={anidb_id}");
    match fetch_with_retry(http, &url, bucket, &format!("ani.zip {anidb_id}")).await {
        Ok(Some(resp)) => {
            let enriched = merge_anizip_episodes(&mut entry, &resp);
            let episode_count = entry["episodes"].as_array().map(|a| a.len()).unwrap_or(0);
            if enriched
                && let Some(sources) = entry["sources"].as_array_mut()
                && !sources.iter().any(|s| s == "anizip")
            {
                sources.push(Value::String("anizip".to_string()));
            }
            EnrichResult { entry, enriched, episode_count, failure_occurred: false }
        }
        Ok(None) => {
            EnrichResult { entry, enriched: false, episode_count: 0, failure_occurred: false }
        }
        Err(e) => {
            tracing::warn!("ani.zip: anidb {anidb_id} failed: {e:#}");
            EnrichResult { entry, enriched: false, episode_count: 0, failure_occurred: true }
        }
    }
}

// ── Phase ──────────────────────────────────────────────────────────────────

pub struct AnizipEnrichPhase;

#[async_trait]
impl Phase for AnizipEnrichPhase {
    fn id(&self) -> PhaseId {
        PhaseId::AnizipEnrich
    }

    fn name(&self) -> &'static str {
        "ani.zip Enrichment"
    }

    async fn run(&self, config: &PipelineConfig, checkpoint: &mut Checkpoint) -> Result<u64> {
        let phase_id = self.id();
        // Prefer the most-enriched upstream file available; fall back down
        // the chain so this phase still works when TMDB/TVDB were skipped.
        let candidates = [ANIME_TVDB, ANIME_TMDB, ANIME_CROSSREF];
        let input_path = candidates
            .iter()
            .map(|f| config.work_dir.join(f))
            .find(|p| p.exists())
            .context("no upstream input found (expected anime-tmdb.jsonl)")?;
        let output_path = config.work_dir.join(ANIME_ANIZIP);

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
            .user_agent(ANIZIP_USER_AGENT)
            .build()
            .context("building HTTP client")?;
        let bucket = Arc::new(Mutex::new(TokenBucket::new(ANIZIP_RATE_LIMIT)));

        let reader = std::fs::File::open(&input_path)
            .with_context(|| format!("opening {} for reading", input_path.display()))?;
        let lines: Vec<String> = std::io::BufRead::lines(std::io::BufReader::new(reader))
            .map_while(|l| l.ok())
            .collect();

        // Stream items: (out_line, idx, enriched, episode_count, failure)
        let stream = futures::stream::iter(lines.into_iter().enumerate().map(
            |(offset, line)| {
                let http = http.clone();
                let bucket = Arc::clone(&bucket);
                async move {
                    let idx = offset as u64;
                    let entry: Value = match serde_json::from_str(&line) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("anizip: malformed JSON at line {idx}: {e}");
                            return (line, idx, false, 0u64, 0u64);
                        }
                    };
                    let result = enrich_entry(&http, &bucket, entry).await;
                    let out_line =
                        serde_json::to_string(&result.entry).unwrap_or_else(|_| line.clone());
                    (
                        out_line,
                        idx,
                        result.enriched,
                        result.episode_count as u64,
                        u64::from(result.failure_occurred),
                    )
                }
            },
        ))
        .buffered(ANIZIP_CONCURRENCY);

        let mut writer = JsonlWriter::new(&output_path)?;
        let mut processed = 0u64;
        let mut enriched_count = 0u64;
        let mut total_episodes = 0u64;
        let mut entries_with_episodes = 0u64;
        let mut failures = 0u64;

        tokio::pin!(stream);
        while let Some((out_line, idx, enriched, eps, failure)) = stream.next().await {
            writer.write_raw(&out_line).context(format!("writing line {idx}"))?;
            processed += 1;
            if enriched {
                enriched_count += 1;
            }
            if eps > 0 {
                entries_with_episodes += 1;
                total_episodes += eps;
            }
            failures += failure;

            if processed.is_multiple_of(500) {
                if let crate::checkpoint::PhaseState::Simple(s) =
                    checkpoint.phases.get_mut(&phase_id).unwrap()
                {
                    s.completed = processed;
                }
                crate::progress::log_progress(
                    "ani.zip",
                    processed,
                    total_input,
                    Some(start),
                    &[
                        ("enriched", enriched_count),
                        ("episodes", total_episodes),
                        ("failures", failures),
                    ],
                );
            }
        }
        writer.flush()?;

        tracing::info!(
            "ani.zip complete: {} input, {} enriched, {} episodes across {} shows, {} failures",
            processed,
            enriched_count,
            total_episodes,
            entries_with_episodes,
            failures,
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
    use serde_json::json;

    fn az(
        season: Option<i32>,
        number: Option<i32>,
        en: Option<&str>,
        summary: Option<&str>,
        rating: Option<&str>,
    ) -> AnizipEpisode {
        AnizipEpisode {
            season_number: season,
            episode_number: number,
            title: en.map(|t| std::collections::HashMap::from([("en".to_string(), t.to_string())])),
            summary: summary.map(String::from),
            rating: rating.map(String::from),
            ..Default::default()
        }
    }

    #[test]
    fn test_parse_episode_key_numeric() {
        assert_eq!(parse_episode_key("1"), Some((1, 1)));
        assert_eq!(parse_episode_key("26"), Some((1, 26)));
    }

    #[test]
    fn test_parse_episode_key_specials() {
        assert_eq!(parse_episode_key("S1"), Some((0, 1)));
        assert_eq!(parse_episode_key("S12"), Some((0, 12)));
    }

    #[test]
    fn test_parse_episode_key_ignored() {
        assert_eq!(parse_episode_key("P1"), None);
        assert_eq!(parse_episode_key("T1"), None);
        assert_eq!(parse_episode_key(""), None);
        assert_eq!(parse_episode_key("Sabc"), None);
    }

    #[test]
    fn test_rating_parsing() {
        let r = az(Some(1), Some(1), Some("T"), None, Some("7.02")).rating().unwrap();
        assert!((r.average - 7.02).abs() < 1e-9);
        assert!(r.count.is_none());
        assert!(az(Some(1), Some(1), None, None, Some("junk")).rating().is_none());
        assert!(az(Some(1), Some(1), None, None, None).rating().is_none());
        assert!(az(Some(1), Some(1), None, None, Some("")).rating().is_none());
    }

    #[test]
    fn test_merge_creates_episodes_when_missing() {
        let mut entry = json!({"id": 5, "ids": {"anidb": 23}});
        let resp = AnizipResponse {
            episodes: [
                ("1".to_string(), az(Some(1), Some(1), Some("Asteroid Blues"), Some("Spike heads to Tijuana"), Some("7.02"))),
                ("S1".to_string(), az(None, None, Some("Session XX"), None, Some("5.57"))),
            ]
            .into_iter()
            .collect(),
        };
        assert!(merge_anizip_episodes(&mut entry, &resp));
        let eps = entry["episodes"].as_array().unwrap();
        assert_eq!(eps.len(), 2);
        // Specials (season 0) sort before season 1.
        assert_eq!(eps[0]["seasonNumber"], 0);
        assert_eq!(eps[0]["titles"]["english"], "Session XX");
        assert_eq!(eps[0]["rating"]["average"], 5.57);
        assert_eq!(eps[1]["titles"]["english"], "Asteroid Blues");
        assert_eq!(eps[1]["overview"], "Spike heads to Tijuana");
        assert_eq!(eps[1]["rating"]["average"], 7.02);
        // NOTE: `sources` tagging happens in enrich_entry, not here.
    }

    #[test]
    fn test_merge_fills_without_overwriting() {
        let mut entry = json!({
            "id": 1,
            "sources": ["anilist", "tmdb"],
            "episodes": [{
                "number": 1,
                "seasonNumber": 1,
                "titles": {"english": "TMDB Title"},
                "overview": "TMDB overview"
            }]
        });
        let resp = AnizipResponse {
            episodes: [(
                "1".to_string(),
                az(Some(1), Some(1), Some("ani.zip Title"), Some("ani.zip summary"), Some("8.5")),
            )]
            .into_iter()
            .collect(),
        };
        assert!(merge_anizip_episodes(&mut entry, &resp));
        let eps = entry["episodes"].as_array().unwrap();
        assert_eq!(eps.len(), 1);
        // Existing values must win.
        assert_eq!(eps[0]["titles"]["english"], "TMDB Title");
        assert_eq!(eps[0]["overview"], "TMDB overview");
        // Missing fields get filled.
        assert_eq!(eps[0]["rating"]["average"], 8.5);
    }

    #[test]
    fn test_merge_dedups_duplicate_keys() {
        let mut entry = json!({
            "id": 1,
            "episodes": [
                {"number": 1, "seasonNumber": 1},
                {"number": 1, "seasonNumber": 1}
            ]
        });
        let resp = AnizipResponse {
            episodes: [("1".to_string(), az(Some(1), Some(1), Some("Ep One"), None, None))]
                .into_iter()
                .collect(),
        };
        assert!(merge_anizip_episodes(&mut entry, &resp));
        let eps = entry["episodes"].as_array().unwrap();
        assert_eq!(eps.len(), 1, "duplicates must collapse");
        assert_eq!(eps[0]["titles"]["english"], "Ep One");
    }

    #[test]
    fn test_merge_empty_response_is_noop() {
        let mut entry = json!({"id": 1});
        let resp = AnizipResponse::default();
        assert!(!merge_anizip_episodes(&mut entry, &resp));
        assert!(entry.get("episodes").is_none());
        assert!(entry.get("sources").is_none());
    }

    #[test]
    fn test_phase_id() {
        let phase = AnizipEnrichPhase;
        assert_eq!(phase.id(), PhaseId::AnizipEnrich);
        assert_eq!(phase.name(), "ani.zip Enrichment");
    }
}




