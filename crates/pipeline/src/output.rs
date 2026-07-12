use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::checkpoint::{Checkpoint, PhaseId};
use crate::phase::{Phase, PipelineConfig};

// ── Constants ──────────────────────────────────────────────────────────────

const ZSTD_COMPRESSION_LEVEL: i32 = 12;

const ANIME_TVDB: &str = "anime-tvdb.jsonl";
const MANGA_BASE: &str = "manga-base.jsonl";
const ANIME_OUTPUT: &str = "anigraph-anime.jsonl.zst";
const MANGA_OUTPUT: &str = "anigraph-manga.jsonl.zst";
const CHECKSUMS_FILE: &str = "checksums.txt";
const MANIFEST_FILE: &str = "manifest.json";

/// Result of compressing a single JSONL file.
struct CompressResult {
    input_size: u64,
    output_size: u64,
    line_count: u64,
}

/// Both BLAKE3 and SHA256 hashes for a single file.
struct FileHashes {
    blake3: String,
    sha256: String,
}

// ── Phase ──────────────────────────────────────────────────────────────────

/// Output phase: compress final JSONL files with zstd, generate checksums
/// (BLAKE3 + SHA256), and write a machine-readable manifest.json.
///
/// This is the last phase in the pipeline.  It reads:
/// - `anime-tvdb.jsonl` (enriched anime) → compresses to `anigraph-anime.jsonl.zst`
/// - `manga-base.jsonl` (manga, no enrichment) → compresses to `anigraph-manga.jsonl.zst`
///
/// And writes:
/// - `checksums.txt` — BLAKE3 and SHA256 hashes of compressed files
/// - `manifest.json` — version, entry counts, file sizes, both hashes
pub struct OutputPhase;

#[async_trait]
impl Phase for OutputPhase {
    fn id(&self) -> PhaseId {
        PhaseId::Output
    }

    fn name(&self) -> &'static str {
        "Output"
    }

    async fn run(&self, config: &PipelineConfig, checkpoint: &mut Checkpoint) -> Result<u64> {
        let phase_id = self.id();

        // Determine work mode (fresh vs resume)
        let completed = if config.resume {
            if !checkpoint.phases.contains_key(&phase_id) {
                checkpoint.phases.insert(
                    phase_id.clone(),
                    crate::checkpoint::PhaseState::Simple(
                        crate::checkpoint::SimpleState::new(2),
                    ),
                );
            }
            checkpoint.is_completed(&phase_id)
        } else {
            checkpoint.phases.insert(
                phase_id.clone(),
                crate::checkpoint::PhaseState::Simple(
                    crate::checkpoint::SimpleState::new(2), // 2 output files
                ),
            );
            false
        };

        if completed {
            tracing::info!("Output phase already completed, skipping");
            return Ok(2);
        }

        std::fs::create_dir_all(&config.output_dir)
            .context("creating output directory")?;

        // ── Collect per-file data for manifest ──────────────────────────
        #[derive(serde::Serialize)]
        struct FileInfo {
            size: u64,
            blake3: String,
            sha256: String,
        }

        let mut manifest_entries: Vec<(&str, FileInfo)> = Vec::new();
        let mut total_anime = 0u64;
        let mut total_manga = 0u64;
        let mut files_written = 0u64;

        // ── Anime: anime-tvdb.jsonl → anime.jsonl.zst ──────────────────
        {
            let input = config.work_dir.join(ANIME_TVDB);
            let output = config.output_dir.join(ANIME_OUTPUT);

            if input.exists() {
                let result = compress_jsonl(&input, &output, "anime")?;
                let hashes = compute_hashes(&output)?;
                total_anime = result.line_count;

                tracing::info!(
                    "Anime: {} entries, {} → {} ({:.1}%), blake3={} sha256={}",
                    result.line_count,
                    format_size(result.input_size),
                    format_size(result.output_size),
                    compression_ratio(result.input_size, result.output_size),
                    &hashes.blake3[..12],
                    &hashes.sha256[..12],
                );

                manifest_entries.push((ANIME_OUTPUT, FileInfo {
                    size: result.output_size,
                    blake3: hashes.blake3,
                    sha256: hashes.sha256,
                }));
                files_written += 1;
            } else {
                tracing::warn!("Anime input not found at {}, skipping", input.display());
            }
        }

        // ── Manga: manga-base.jsonl → manga.jsonl.zst ──────────────────
        {
            let input = config.work_dir.join(MANGA_BASE);
            let output = config.output_dir.join(MANGA_OUTPUT);

            if input.exists() {
                let result = compress_jsonl(&input, &output, "manga")?;
                let hashes = compute_hashes(&output)?;
                total_manga = result.line_count;

                tracing::info!(
                    "Manga: {} entries, {} → {} ({:.1}%), blake3={} sha256={}",
                    result.line_count,
                    format_size(result.input_size),
                    format_size(result.output_size),
                    compression_ratio(result.input_size, result.output_size),
                    &hashes.blake3[..12],
                    &hashes.sha256[..12],
                );

                manifest_entries.push((MANGA_OUTPUT, FileInfo {
                    size: result.output_size,
                    blake3: hashes.blake3,
                    sha256: hashes.sha256,
                }));
                files_written += 1;
            } else {
                tracing::warn!("Manga input not found at {}, skipping", input.display());
            }
        }

        // ── Write checksums.txt ─────────────────────────────────────────
        let checksums_path = config.output_dir.join(CHECKSUMS_FILE);
        let mut checksums = String::new();

        for (filename, info) in &manifest_entries {
            checksums.push_str(&format!("blake3:{}  {}\n", info.blake3, filename));
            checksums.push_str(&format!("sha256:{}  {}\n", info.sha256, filename));
        }

        std::fs::write(&checksums_path, &checksums)
            .context("writing checksums.txt")?;
        tracing::info!("Checksums written to {}", checksums_path.display());

        // ── Write manifest.json ─────────────────────────────────────────
        let now = chrono::Utc::now();
        let version = now.format("%Y-%m-%d").to_string();

        let mut files_map = serde_json::Map::new();
        for (filename, info) in &manifest_entries {
            files_map.insert(
                filename.to_string(),
                serde_json::json!({
                    "size": info.size,
                    "blake3": info.blake3,
                    "sha256": info.sha256,
                }),
            );
        }

        let mut entries_map = serde_json::Map::new();
        entries_map.insert("anime".to_string(), serde_json::json!(total_anime));
        entries_map.insert("manga".to_string(), serde_json::json!(total_manga));

        let manifest = serde_json::json!({
            "version": version,
            "generatedAt": now.to_rfc3339(),
            "entries": entries_map,
            "files": files_map,
        });

        let manifest_path = config.output_dir.join(MANIFEST_FILE);
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .context("serializing manifest")?;
        std::fs::write(&manifest_path, &manifest_json)
            .context("writing manifest.json")?;
        tracing::info!("Manifest written to {}", manifest_path.display());

        // ── Finalize ────────────────────────────────────────────────────
        if let crate::checkpoint::PhaseState::Simple(s) =
            checkpoint.phases.get_mut(&phase_id).unwrap()
        {
            s.completed = files_written;
        }
        checkpoint.complete_simple(&phase_id);
        checkpoint
            .save(&config.checkpoint_path)
            .context("saving final checkpoint")?;

        Ok(files_written)
    }
}

// ── Compression ────────────────────────────────────────────────────────────

/// Read a JSONL file, compress it with zstd, write to output.
fn compress_jsonl(input: &Path, output: &Path, label: &str) -> Result<CompressResult> {
    let input_size = std::fs::metadata(input)
        .with_context(|| format!("reading metadata for {label} input"))?
        .len();

    let reader = std::fs::File::open(input)
        .with_context(|| format!("opening {label} input"))?;
    let buf_reader = std::io::BufReader::new(reader);

    let out_file = std::fs::File::create(output)
        .with_context(|| format!("creating {label} output"))?;
    let mut encoder = zstd::stream::Encoder::new(out_file, ZSTD_COMPRESSION_LEVEL)
        .with_context(|| format!("creating zstd encoder for {label}"))?;

    let mut line_count = 0u64;

    for line in std::io::BufRead::lines(buf_reader) {
        let line = line.with_context(|| format!("reading {label} input line"))?;
        encoder
            .write_all(line.as_bytes())
            .with_context(|| format!("writing compressed {label} line"))?;
        encoder
            .write_all(b"\n")
            .with_context(|| format!("writing compressed {label} newline"))?;
        line_count += 1;
    }

    let out_file = encoder
        .finish()
        .with_context(|| format!("finalizing zstd stream for {label}"))?;
    let output_size = out_file
        .metadata()
        .map(|m| m.len())
        .unwrap_or(0);

    Ok(CompressResult {
        input_size,
        output_size,
        line_count,
    })
}

/// Compute BLAKE3 and SHA256 hashes of a file.
fn compute_hashes(path: &Path) -> Result<FileHashes> {
    let data = std::fs::read(path)
        .with_context(|| format!("reading {} for hashing", path.display()))?;

    let blake3 = blake3::hash(&data).to_hex().to_string();
    let sha256 = format!("{:x}", Sha256::digest(&data));

    Ok(FileHashes { blake3, sha256 })
}

fn compression_ratio(input: u64, output: u64) -> f64 {
    if input > 0 {
        (1.0 - output as f64 / input as f64) * 100.0
    } else {
        0.0
    }
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{bytes} B")
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_format_size_bytes() {
        assert_eq!(format_size(500), "500 B");
    }

    #[test]
    fn test_format_size_kb() {
        assert_eq!(format_size(1500), "1.5 KB");
    }

    #[test]
    fn test_format_size_mb() {
        assert_eq!(format_size(2_500_000), "2.5 MB");
    }

    #[test]
    fn test_compress_jsonl_roundtrip() {
        let dir = std::env::temp_dir().join("anigraph-output-test");
        std::fs::create_dir_all(&dir).unwrap();

        // Create a small test JSONL file
        let input_path = dir.join("test.jsonl");
        let output_path = dir.join("test.jsonl.zst");

        {
            let mut f = std::fs::File::create(&input_path).unwrap();
            writeln!(f, r#"{{"id":1,"name":"test1"}}"#).unwrap();
            writeln!(f, r#"{{"id":2,"name":"test2"}}"#).unwrap();
            writeln!(f, r#"{{"id":3,"name":"test3"}}"#).unwrap();
        }

        // Compress
        let result = compress_jsonl(&input_path, &output_path, "test").unwrap();

        assert!(result.input_size > 0);
        assert!(result.output_size > 0);
        assert_eq!(result.line_count, 3);

        // Verify hashes are computed correctly
        let hashes = compute_hashes(&output_path).unwrap();
        assert_eq!(hashes.blake3.len(), 64); // BLAKE3 hex = 64 chars
        assert_eq!(hashes.sha256.len(), 64); // SHA256 hex = 64 chars

        // Decompress and verify
        let decompressed = {
            let reader = std::fs::File::open(&output_path).unwrap();
            let mut decoder = zstd::stream::Decoder::new(reader).unwrap();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut buf).unwrap();
            String::from_utf8(buf).unwrap()
        };

        assert!(decompressed.contains(r#""id":1"#));
        assert!(decompressed.contains(r#""id":2"#));
        assert!(decompressed.contains(r#""id":3"#));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_phase_id() {
        let phase = OutputPhase;
        assert_eq!(phase.id(), PhaseId::Output);
        assert_eq!(phase.name(), "Output");
    }
}
