//! Rouge external provider integrations (AniList, AODB, ...).
//!
//! This crate owns all network I/O for talking to external knowledge
//! sources. The `library` crate depends only on the *optimized* artifacts
//! these providers emit (e.g. `aodb.bin`); it never performs network calls.
//!
//! # Modules
//!
//! - [`anilist`] — live AniList API client (auth, queries, collections).
//! - [`aodb`] — offline database lifecycle (download + build `aodb.bin`).

pub mod anilist;
