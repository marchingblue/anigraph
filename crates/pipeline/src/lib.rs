//! `anigraph-pipeline` — orchestration of the dataset generation pipeline.
//!
//! This crate owns the multi-phase build process:
//!
//! 1. **Enumerate** — fetch all anime and manga entries from AniList
//! 2. **Cross-reference** — resolve external IDs via Fribb/anime-lists
//! 3. **Enrich (TMDB)** — fetch artwork *and per-episode metadata* from TMDB
//! 4. **Enrich (TVDB)** — fetch additional artwork from TheTVDB
//! 5. **Enrich (ani.zip)** — fill in multilingual episode titles, AniDB
//!    summaries/ratings, and add episodes for shows TMDB doesn't know
//! 6. **Output** — compress to final JSONL, generate checksums + manifest
//!
//! Runs are always fresh (CI); checkpoint persistence is disabled and phase
//! progress is kept in memory only.

pub mod anizip;
pub mod checkpoint;
pub mod crossref;
#[cfg(unix)]
pub mod lock;
pub mod enumerate;
pub mod jsonl_writer;
pub mod output;
pub mod progress;
pub mod phase;
pub mod runner;
pub mod stats;
pub mod tmdb;
pub mod tvdb;
