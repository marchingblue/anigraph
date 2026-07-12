//! `anigraph-pipeline` — orchestration of the dataset generation pipeline.
//!
//! This crate owns the multi-phase build process:
//!
//! 1. **Enumerate** — fetch all anime and manga entries from AniList
//! 2. **Cross-reference** — resolve TVDB/TMDB IDs via Fribb/anime-lists
//! 3. **Enrich (TMDB)** — fetch artwork from TMDB
//! 4. **Enrich (TVDB)** — fetch episodes and artwork from TheTVDB
//! 5. **Output** — compress to final JSONL, generate manifest
//!
//! Each phase is independently runnable and resumable via the checkpoint system.

pub mod checkpoint;
pub mod crossref;
#[cfg(unix)]
pub mod lock;
pub mod enumerate;
pub mod jsonl_writer;
pub mod output;
pub mod phase;
pub mod runner;
pub mod stats;
pub mod tmdb;
pub mod tvdb;
