use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use anigraph_model::CrossIds;

use crate::checkpoint::{Checkpoint, PhaseId};
use crate::jsonl_writer::JsonlWriter;
use crate::phase::{Phase, PipelineConfig};

// ── Constants ──────────────────────────────────────────────────────────────

const FRIBB_URL: &str =
    "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-full.json";
const FRIBB_DOWNLOAD_RETRIES: u32 = 3;

/// Input file from the enumeration phase.
const ANIME_BASE: &str = "anime-base.jsonl";
/// Output file after cross-referencing.
const ANIME_CROSSREF: &str = "anime-crossref.jsonl";

// ── Fribb JSON schema ──────────────────────────────────────────────────────

/// Single entry in the Fribb/anime-lists JSON file.
///
/// All fields except `anilist_id` are optional — the file is
/// community-maintained and has no schema guarantees. Entries without
/// `anilist_id` are skipped (they can't be joined to our data).
///
/// The `themoviedb_id` and `imdb_id` fields are stored as raw `Value`
/// because their shapes vary: `{"tv": 26209}` vs `{"movie": [128, 129]}`
/// for TMDB, and `["tt0102847"]` vs `"tt0102847"` for IMDB.  They're
/// parsed into the structured fields at cross-reference time.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct FribbEntry {
    anilist_id: Option<i32>,

    mal_id: Option<i32>,
    anidb_id: Option<i32>,
    kitsu_id: Option<i32>,
    tvdb_id: Option<i32>,

    /// Raw `themoviedb_id` value — parsed into `tmdb_tv` / `tmdb_movie` later.
    #[serde(rename = "themoviedb_id")]
    themoviedb_id: Option<Value>,
    #[serde(deserialize_with = "deserialize_imdb")]
    imdb_id: Option<String>,

    #[serde(rename = "anime-planet_id")]
    anime_planet_id: Option<String>,
    anisearch_id: Option<i32>,
    livechart_id: Option<i32>,
    simkl_id: Option<i32>,
    animecountdown_id: Option<i32>,
    animenewsnetwork_id: Option<i32>,
}

// ── IMDB field parser ──────────────────────────────────────────────────────

/// Parse `imdb_id` — Fribb stores it as `["tt0102847"]` (array).
/// We take the first element if present.
fn deserialize_imdb<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v: Option<Value> = Option::deserialize(deserializer).unwrap_or(None);
    match v {
        Some(Value::Array(arr)) => {
            if let Some(first) = arr.first()
                && let Some(s) = first.as_str() {
                    return Ok(Some(s.to_string()));
                }
            Ok(None)
        }
        Some(Value::String(s)) => Ok(Some(s)),
        _ => Ok(None),
    }
}

// ── Phase ──────────────────────────────────────────────────────────────────

/// Cross-reference phase: map AniList IDs → provider IDs via Fribb/anime-lists.
///
/// This is a `Simple` phase (not paginated). It:
/// 1. Downloads the Fribb JSON file from GitHub
/// 2. Parses it into a `HashMap<anilist_id, FribbEntry>` (skipping entries without ID)
/// 3. Reads `anime-base.jsonl`, looks up each entry's ID, populates `CrossIds`
/// 4. Writes `anime-crossref.jsonl`
/// 5. Reports match stats (matched / unmatched / total)
pub struct CrossRefPhase;

#[async_trait]
impl Phase for CrossRefPhase {
    fn id(&self) -> PhaseId {
        PhaseId::FribbCrossref
    }

    fn name(&self) -> &'static str {
        "Fribb Cross-Reference"
    }

    async fn run(&self, config: &PipelineConfig, checkpoint: &mut Checkpoint) -> Result<u64> {
        let phase_id = self.id();
        let anime_input = config.work_dir.join(ANIME_BASE);
        let anime_output = config.work_dir.join(ANIME_CROSSREF);

        // ── Ensure input exists ──────────────────────────────────────────
        if !anime_input.exists() {
            anyhow::bail!(
                "anime base file not found at {}. Run the enumeration phase first.",
                anime_input.display()
            );
        }

        // ── Determine start state (fresh vs resume) ──────────────────────
        let total_input = count_jsonl_lines(&anime_input)?;
        let start_from = if config.resume {
            if checkpoint.is_completed(&phase_id) {
                tracing::info!("Fribb cross-ref already completed, skipping");
                let total = match checkpoint.phases.get(&phase_id) {
                    Some(crate::checkpoint::PhaseState::Simple(s)) => s.completed,
                    _ => checkpoint.items_written(&phase_id),
                };
                return Ok(total);
            }
            // Count already-processed lines in existing output
            if anime_output.exists() {
                count_jsonl_lines(&anime_output)?
            } else {
                0
            }
        } else {
            checkpoint.phases.insert(
                phase_id.clone(),
                crate::checkpoint::PhaseState::Simple(
                    crate::checkpoint::SimpleState::new(total_input),
                ),
            );
            0
        };

        // ── Download + parse Fribb ───────────────────────────────────────
        tracing::info!("Downloading Fribb/anime-lists from GitHub...");
        let fribb_map = download_and_parse_fribb().await
            .context("downloading and parsing Fribb/anime-lists")?;
        tracing::info!(
            "Fribb map loaded: {} entries with anilist_id",
            fribb_map.len()
        );

        // Wall-clock start, used for the ETA in the progress indicator.
        let start = Instant::now();

        // ── Read input, cross-reference, write output ────────────────────
        let reader = std::fs::File::open(&anime_input)
            .context("opening anime-base.jsonl for reading")?;
        let buf_reader = std::io::BufReader::new(reader);
        let lines: Vec<String> = std::io::BufRead::lines(buf_reader)
            .map_while(|l| l.ok())
            .collect();

        let mut matched = 0u64;
        let mut unmatched = 0u64;
        let mut written = 0u64;

        let output_file = if start_from > 0 {
            JsonlWriter::append(&anime_output)?
        } else {
            JsonlWriter::new(&anime_output)?
        };
        let mut writer = output_file;

        for (idx, line) in lines.iter().enumerate() {
            let idx = idx as u64;

            // Skip already-processed lines on resume
            if idx < start_from {
                continue;
            }

            // Parse the entry (defensive — skip malformed lines)
            let mut entry: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("crossref: malformed JSON at line {idx}: {e}");
                    continue;
                }
            };

            let anilist_id = entry["id"].as_i64().unwrap_or(0);
            if anilist_id == 0 {
                tracing::warn!("crossref: entry at line {idx} has no id, skipping");
                continue;
            }

            // Look up in Fribb map
            if let Some(fribb) = fribb_map.get(&(anilist_id as i32)) {
                let mut cross_ids = build_cross_ids(fribb);

                // Preserve the MAL ID from the enumeration phase if Fribb
                // doesn't have one — AniList's `idMal` is often fresher.
                if cross_ids.mal.is_none() {
                    cross_ids.mal = entry["ids"]["mal"]
                        .as_i64()
                        .map(|m| m as i32);
                }

                entry["ids"] = serde_json::to_value(&cross_ids)
                    .context("serializing cross-ids")?;
                matched += 1;
            } else {
                unmatched += 1;
            }

            // Update SimpleState progress periodically
            if written > 0 && written.is_multiple_of(1000)
                && let crate::checkpoint::PhaseState::Simple(s) =
                    checkpoint.phases.get_mut(&phase_id).unwrap()
                {
                    s.completed = written;
                }

            writer.write(&entry).context(format!("writing line {idx}"))?;
            written += 1;

            // Progress logging every 5K entries
            if idx > 0 && idx.is_multiple_of(5000) {
                crate::progress::log_progress(
                    "crossref",
                    idx,
                    total_input,
                    Some(start),
                    &[("matched", matched), ("unmatched", unmatched)],
                );
            }
        }

        writer.flush()?;

        // ── Report ───────────────────────────────────────────────────────
        tracing::info!(
            "Fribb cross-ref complete: {total_input} total, {matched} matched, {unmatched} unmatched"
        );
        if unmatched > 0 {
            let pct = (unmatched as f64 / total_input as f64) * 100.0;
            tracing::info!(
                "{unmatched} entries ({pct:.1}%) had no Fribb match — likely recently added anime"
            );
        }

        if let crate::checkpoint::PhaseState::Simple(s) =
            checkpoint.phases.get_mut(&phase_id).unwrap()
        {
            s.completed = written;
        }
        checkpoint.complete_simple(&phase_id);
        checkpoint
            .save(&config.checkpoint_path)
            .context("saving final checkpoint")?;

        Ok(written)
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Download the Fribb JSON file with retry and parse it into a map keyed by `anilist_id`.
async fn download_and_parse_fribb() -> Result<HashMap<i32, FribbEntry>> {
    let bytes = download_with_retry(FRIBB_URL, FRIBB_DOWNLOAD_RETRIES).await?;
    let mut map = HashMap::new();

    // Parse entry-by-entry for defensive handling
    let raw: Vec<Value> = serde_json::from_slice(&bytes)
        .context("parsing Fribb JSON")?;

    for (i, entry_val) in raw.iter().enumerate() {
        let entry: FribbEntry = match FribbEntry::deserialize(entry_val) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("fribb: skipping malformed entry {i}: {e}");
                continue;
            }
        };

        if let Some(aid) = entry.anilist_id {
            map.insert(aid, entry);
        }
    }

    Ok(map)
}

/// Download a URL with exponential backoff retry.
/// Uses a single `reqwest::Client` with a 30-second timeout.
async fn download_with_retry(url: &str, max_retries: u32) -> Result<Vec<u8>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("building reqwest client")?;

    for attempt in 0..=max_retries {
        match client.get(url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    return Ok(resp.bytes().await.context("reading response body")?.to_vec());
                }
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                if attempt < max_retries {
                    let delay = Duration::from_secs(2u64.pow(attempt + 1));
                    tracing::warn!(
                        "fribb download attempt {}/{}: HTTP {status}, retrying in {delay:?}",
                        attempt + 1,
                        max_retries + 1,
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    anyhow::bail!(
                        "fribb download failed after {} attempts: HTTP {status}: {body}",
                        max_retries + 1,
                    );
                }
            }
            Err(e) => {
                if attempt < max_retries {
                    let delay = Duration::from_secs(2u64.pow(attempt + 1));
                    tracing::warn!(
                        "fribb download attempt {}/{}: {e}, retrying in {delay:?}",
                        attempt + 1,
                        max_retries + 1,
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    anyhow::bail!(
                        "fribb download failed after {} attempts: {e}",
                        max_retries + 1,
                    );
                }
            }
        }
    }

    unreachable!()
}

/// Build a `CrossIds` struct from a `FribbEntry`.
///
/// TMDB IDs are extracted from the raw `themoviedb_id` value which can be
/// `{"tv": 26209}` (TV series) or `{"movie": [128, 129]}` (one or more movies).
fn build_cross_ids(fribb: &FribbEntry) -> CrossIds {
    let (tmdb_tv, tmdb_movie) = parse_tmdb_value(&fribb.themoviedb_id);

    CrossIds {
        mal: fribb.mal_id,
        anidb: fribb.anidb_id,
        kitsu: fribb.kitsu_id,
        tvdb: fribb.tvdb_id,
        tmdb_tv,
        tmdb_movie,
        imdb: fribb.imdb_id.clone(),
        anime_planet: fribb.anime_planet_id.clone(),
        anisearch: fribb.anisearch_id,
        livechart: fribb.livechart_id,
        simkl: fribb.simkl_id,
        animecountdown: fribb.animecountdown_id,
        animenewsnetwork: fribb.animenewsnetwork_id,
    }
}

/// Parse `themoviedb_id` raw value into TV and movie components.
///
/// Fribb format for TMDB:
/// - `{"tv": 26209}` — a TV series on TMDB
/// - `{"movie": [128]}` or `{"movie": [128, 129]}` — one or more movies on TMDB
///
/// These use *different* ID namespaces on TMDB: the same numeric ID can refer
/// to a TV show and a completely unrelated movie. Keeping them separate is
/// essential for the enrichment phase to know which endpoint to call
/// (`/tv/{id}` vs `/movie/{id}`).
fn parse_tmdb_value(v: &Option<Value>) -> (Option<i32>, Vec<i32>) {
    let Some(Value::Object(map)) = v else {
        return (None, Vec::new());
    };

    let tmdb_tv = map
        .get("tv")
        .and_then(|v| v.as_i64())
        .map(|id| id as i32);

    let tmdb_movie = map
        .get("movie")
        .and_then(|v| match v {
            Value::Array(arr) => Some(
                arr.iter()
                    .filter_map(|x| x.as_i64().map(|i| i as i32))
                    .collect(),
            ),
            Value::Number(n) => n.as_i64().map(|id| vec![id as i32]),
            _ => None,
        })
        .unwrap_or_default();

    (tmdb_tv, tmdb_movie)
}

/// Count the number of lines in a JSONL file.
fn count_jsonl_lines(path: &Path) -> Result<u64> {
    let reader = std::fs::File::open(path)
        .with_context(|| format!("counting lines in {}", path.display()))?;
    let buf = std::io::BufReader::new(reader);
    Ok(std::io::BufRead::lines(buf)
        .map_while(|l| l.ok())
        .count() as u64)
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_build_cross_ids_all_fields() {
        let entry = FribbEntry {
            anilist_id: Some(1),
            mal_id: Some(1),
            anidb_id: Some(2),
            kitsu_id: Some(3),
            tvdb_id: Some(72025),
            themoviedb_id: Some(json!({"tv": 26209, "movie": [128, 129]})),
            imdb_id: Some("tt0119698".to_string()),
            anime_planet_id: Some("crest-of-the-stars".to_string()),
            anisearch_id: Some(3039),
            livechart_id: Some(4157),
            simkl_id: Some(36462),
            animecountdown_id: Some(36462),
            animenewsnetwork_id: Some(14),
        };

        let ids = build_cross_ids(&entry);
        assert_eq!(ids.mal, Some(1));
        assert_eq!(ids.anidb, Some(2));
        assert_eq!(ids.kitsu, Some(3));
        assert_eq!(ids.tvdb, Some(72025));
        assert_eq!(ids.tmdb_tv, Some(26209));
        assert_eq!(ids.tmdb_movie, vec![128, 129]);
        assert_eq!(ids.imdb.as_deref(), Some("tt0119698"));
        assert_eq!(ids.anime_planet.as_deref(), Some("crest-of-the-stars"));
        assert_eq!(ids.anisearch, Some(3039));
        assert_eq!(ids.livechart, Some(4157));
        assert_eq!(ids.simkl, Some(36462));
        assert_eq!(ids.animecountdown, Some(36462));
        assert_eq!(ids.animenewsnetwork, Some(14));
    }

    #[test]
    fn test_build_cross_ids_minimal() {
        let entry = FribbEntry {
            anilist_id: Some(1),
            ..Default::default()
        };

        let ids = build_cross_ids(&entry);
        assert_eq!(ids.mal, None);
        assert_eq!(ids.tvdb, None);
        assert!(ids.tmdb_movie.is_empty());
        assert_eq!(ids.anime_planet, None);
    }

    // ── TMDB value parsing ───────────────────────────────────────────────

    #[test]
    fn test_parse_tmdb_tv_only() {
        let v = Some(json!({"tv": 26209}));
        let (tv, movies) = parse_tmdb_value(&v);
        assert_eq!(tv, Some(26209));
        assert!(movies.is_empty());
    }

    #[test]
    fn test_parse_tmdb_movie_single() {
        let v = Some(json!({"movie": [128]}));
        let (tv, movies) = parse_tmdb_value(&v);
        assert_eq!(tv, None);
        assert_eq!(movies, vec![128]);
    }

    #[test]
    fn test_parse_tmdb_movie_multi() {
        let v = Some(json!({"movie": [128, 129, 130]}));
        let (tv, movies) = parse_tmdb_value(&v);
        assert_eq!(tv, None);
        assert_eq!(movies, vec![128, 129, 130]);
    }

    #[test]
    fn test_parse_tmdb_both() {
        let v = Some(json!({"tv": 26209, "movie": [128]}));
        let (tv, movies) = parse_tmdb_value(&v);
        assert_eq!(tv, Some(26209));
        assert_eq!(movies, vec![128]);
    }

    #[test]
    fn test_parse_tmdb_none() {
        let (tv, movies) = parse_tmdb_value(&None);
        assert_eq!(tv, None);
        assert!(movies.is_empty());
    }

    #[test]
    fn test_parse_tmdb_invalid() {
        let v = Some(json!("invalid"));
        let (tv, movies) = parse_tmdb_value(&v);
        assert_eq!(tv, None);
        assert!(movies.is_empty());
    }

    // ── IMDB deserialization ─────────────────────────────────────────────

    #[test]
    fn test_fribb_entry_deserialize_imdb_array() {
        let raw = json!({
            "anilist_id": 1,
            "imdb_id": ["tt0119698"]
        });
        let entry: FribbEntry = serde_json::from_value(raw).unwrap();
        assert_eq!(entry.imdb_id.as_deref(), Some("tt0119698"));
    }

    #[test]
    fn test_fribb_entry_deserialize_imdb_string() {
        let raw = json!({
            "anilist_id": 1,
            "imdb_id": "tt0119698"
        });
        let entry: FribbEntry = serde_json::from_value(raw).unwrap();
        assert_eq!(entry.imdb_id.as_deref(), Some("tt0119698"));
    }

    #[test]
    fn test_fribb_entry_deserialize_missing_optional_fields() {
        let raw = json!({
            "anilist_id": 1,
            "type": "TV"
        });
        let entry: FribbEntry = serde_json::from_value(raw).unwrap();
        assert_eq!(entry.anilist_id, Some(1));
        assert_eq!(entry.mal_id, None);
        assert_eq!(entry.tvdb_id, None);
        assert_eq!(entry.themoviedb_id, None);
        assert_eq!(entry.imdb_id, None);
    }

    #[test]
    fn test_fribb_entry_deserialize_unknown_fields_ignored() {
        let raw = json!({
            "anilist_id": 1,
            "unknown_field": "some_value",
            "season": { "tvdb": 1 }
        });
        // The `#[serde(default)]` on the struct means unknown fields are ignored
        let entry: FribbEntry = serde_json::from_value(raw).unwrap();
        assert_eq!(entry.anilist_id, Some(1));
    }

    #[test]
    fn test_count_jsonl_lines() {
        use std::io::Write;
        let dir = std::env::temp_dir().join("anigraph-crossref-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.jsonl");

        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "{{\"id\": 1}}").unwrap();
        writeln!(f, "{{\"id\": 2}}").unwrap();
        writeln!(f, "{{\"id\": 3}}").unwrap();
        f.flush().unwrap();

        assert_eq!(count_jsonl_lines(&path).unwrap(), 3);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
