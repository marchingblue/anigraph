//! `anigraph-model` — canonical data types for anigraph dataset entries.
//!
//! This crate has zero network I/O and no dependencies beyond serde.
//! It is the single source of truth for what fields an anigraph entry contains.
//!
//! # Crate organisation
//!
//! - [`shared`] — types shared by both anime and manga entries
//!   (titles, dates, artwork, relations, studios, authors, scores, enums)
//! - [`anime`] — [`AnimeEntry`] struct with episode data
//! - [`manga`] — [`MangaEntry`] struct (no episodes / season / studios)

pub mod anime;
pub mod manga;
pub mod shared;

// Re-export the most important types at crate root for convenience.
pub use anime::{AnimeEntry, Episode, EpisodeIds, EpisodeTitle};
pub use manga::MangaEntry;
pub use shared::*;
