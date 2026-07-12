
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;

use anigraph_model::{
    AgeRating, AnimeEntry, Author, AuthorRole, CrossIds, EntryType, FuzzyDate,
    MangaEntry, MangaFormat, MediaSource, MediaStatus, Relation, RelationType, Score, Season,
    SeasonInfo, Studio, Title,
};
use rouge_providers::anilist::client::AnilistClient;

use crate::checkpoint::{Checkpoint, PhaseId};
use crate::jsonl_writer::JsonlWriter;
use crate::phase::{Phase, PipelineConfig};

// ── Constants ──────────────────────────────────────────────────────────────

const PER_PAGE: u32 = 50;

/// Size of each ID window for the enumeration sweep. AniList rejects `Page`
/// queries past ~5,000 cumulative entries, so we walk the ID space in fixed
/// windows via the `id_in` filter instead of paginating by page number.
const WINDOW_SIZE: i32 = 2000;

/// The two possible media types for enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaType {
    Anime,
    Manga,
}

impl MediaType {
    fn as_graphql_str(&self) -> &'static str {
        match self {
            Self::Anime => "ANIME",
            Self::Manga => "MANGA",
        }
    }

    fn phase_id(&self) -> PhaseId {
        match self {
            Self::Anime => PhaseId::AnilistAnime,
            Self::Manga => PhaseId::AnilistManga,
        }
    }

    fn output_filename(&self) -> &'static str {
        match self {
            Self::Anime => "anime-base.jsonl",
            Self::Manga => "manga-base.jsonl",
        }
    }
}

/// Enumeration phase: fetches all entries of a given media type from AniList.
pub struct EnumeratePhase {
    pub media_type: MediaType,
}

#[async_trait]
impl Phase for EnumeratePhase {
    fn id(&self) -> PhaseId {
        self.media_type.phase_id()
    }

    fn name(&self) -> &'static str {
        match self.media_type {
            MediaType::Anime => "AniList Anime Enumeration",
            MediaType::Manga => "AniList Manga Enumeration",
        }
    }

    async fn run(&self, config: &PipelineConfig, checkpoint: &mut Checkpoint) -> Result<u64> {
        let id = self.id();
        let client = AnilistClient::new_unauthenticated();
        let output_path = config.work_dir.join(self.media_type.output_filename());

        // ── Probe the ID range to size the window sweep ───────────────────
        // AniList rejects `Page` queries past ~5,000 cumulative entries, so we
        // cannot paginate by page number from 1. Instead we walk the ID space
        // in fixed windows using the `id_in` filter, which keeps each query's
        // result set tiny and immune to the depth limit.
        let max_id = client
            .max_id(self.media_type.as_graphql_str())
            .await
            .context("probing AniList max media id")?;
        let total_windows = ((max_id.max(0) as u64) / WINDOW_SIZE as u64) + 1;

        // Rough estimate of total pages, used only as a progress indicator.
        // At most one entry exists per AniList ID, so `max_id / PER_PAGE` is a
        // hard upper bound; real density is far lower (most IDs are unused), so
        // we scale by a per-type fraction. This is cosmetic — resume logic does
        // not depend on it.
        let density = match self.media_type {
            MediaType::Anime => 0.10,
            MediaType::Manga => 0.45,
        };
        let estimated_pages =
            ((max_id.max(0) as f64) * density / PER_PAGE as f64).ceil() as u64;

        // ── Create/open the output file ────────────────────────────────
        std::fs::create_dir_all(&config.work_dir)
            .context("creating working directory")?;

        let (mut window_start, mut inner_page, mut writer) = if config.resume {
            if checkpoint.is_completed(&id) {
                tracing::info!("{} already completed, skipping", self.name());
                return Ok(checkpoint.items_written(&id));
            }
            let window = checkpoint
                .paginated(&id)
                .and_then(|s| s.next_window_start)
                .unwrap_or(1);
            let inner = checkpoint
                .paginated(&id)
                .and_then(|s| s.current_inner_page)
                .unwrap_or(1);
            let byte_pos = checkpoint.resume_byte_offset(&id);
            let mut w = JsonlWriter::append(&output_path)
                .context("opening output file for resume")?;
            // Truncate to the exact byte position of the last fully written
            // page so a partial page (from a crash mid-page) is dropped.
            if byte_pos > 0 {
                w.truncate_to(byte_pos)?;
            }
            tracing::info!(
                "Resuming {} from window id>={}, inner page {} (truncated to byte {})",
                self.name(),
                window,
                inner,
                byte_pos,
            );
            (window, inner, w)
        } else {
            // Fresh run
            checkpoint.begin_paginated(&id, estimated_pages, PER_PAGE, Some(25.0));
            let w = JsonlWriter::new(&output_path)?;
            tracing::info!(
                "Starting {} (id range 1..={}, ~{} windows of {})",
                self.name(),
                max_id,
                total_windows,
                WINDOW_SIZE,
            );
            (1i32, 1u32, w)
        };

        // ── Main fetch loop (ID-window sweep) ─────────────────────────────
        // Each outer iteration is one ID window. Within a window we paginate
        // with `page` until `hasNextPage` is false (a window can contain more
        // than `PER_PAGE` matches). A **page** is the atomic unit of checkpoint
        // progress: after every page is written, flushed, and checkpointed, so a
        // crash mid-page simply re-fetches that single page on resume
        // (idempotent — the output is truncated to the last completed page
        // first, no duplicates, no gaps).
        let mut pages_completed = checkpoint
            .paginated(&id)
            .map(|s| s.last_completed_page)
            .unwrap_or(0);

        loop {
            if window_start > max_id {
                break;
            }

            // Build the ID window [window_start, window_start + WINDOW_SIZE).
            let ids: Vec<i32> = (window_start..window_start + WINDOW_SIZE).collect();

            // Paginate within the window, writing + flushing + checkpointing
            // each page as it arrives so progress is continuous and a kill never
            // loses more than one page.
            let mut page_written = 0u64;
            // Retry budget for the "empty page but has_next=true" anomaly.
            // A genuine end-of-window returns has_next=false, handled
            // separately below. A page that is empty *with* has_next=true is
            // either AniList's known anomaly or a transient glitch. We retry
            // a few times so a transient empty response can't silently skip
            // the rest of the window's pages (which would lose entries).
            let mut empty_strikes = 0u32;
            const MAX_EMPTY_STRIKES: u32 = 3;
            loop {
                let (media_list, has_next) = client
                    .fetch_page(
                        inner_page,
                        PER_PAGE,
                        self.media_type.as_graphql_str(),
                        &ids,
                    )
                    .await
                    .with_context(|| {
                        format!("fetching window id>={window_start} page {inner_page}")
                    })
                    .map_err(|e| {
                        checkpoint.fail_phase(&id, &format!("{e:#}"));
                        if let Err(save_err) = checkpoint.save(&config.checkpoint_path) {
                            tracing::error!("failed to save checkpoint after error: {save_err}");
                        }
                        e
                    })?;

                // Genuine end of window: no matches at all.
                if media_list.is_empty() && !has_next {
                    tracing::debug!(
                        "window id>={window_start} page {inner_page}: empty (has_next=false), ending window",
                    );
                    break;
                }

                // Empty but has_next=true — anomaly or transient glitch.
                if media_list.is_empty() {
                    empty_strikes += 1;
                    if empty_strikes >= MAX_EMPTY_STRIKES {
                        tracing::warn!(
                            "window id>={window_start} page {inner_page}: {empty_strikes}x empty \
                             with has_next=true; ending window to avoid an infinite loop"
                        );
                        break;
                    }
                    tracing::warn!(
                        "window id>={window_start} page {inner_page}: empty but has_next=true \
                         (strike {empty_strikes}/{MAX_EMPTY_STRIKES}), retrying"
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue; // retry the SAME page; do not advance inner_page
                }

                empty_strikes = 0;

                for (idx, media) in media_list.iter().enumerate() {
                    if media.is_null() || !media.is_object() {
                        let media_id = media["id"].as_i64().unwrap_or(0);
                        tracing::warn!(
                            "window id>={window_start} page {inner_page}: null entry at idx={idx}, anilist_id={media_id}",
                        );
                        if media_id > 0 {
                            checkpoint.record_failure(&id, media_id as u64);
                        }
                        continue;
                    }

                    // Write directly — AnimeEntry and MangaEntry are different
                    // types so we can't bind them to the same variable.
                    match self.media_type {
                        MediaType::Anime => {
                            let Some(entry) = anilist_media_to_anime_entry(media) else {
                                let mid = media["id"].as_i64().unwrap_or(0);
                                tracing::warn!("anime entry missing id at window id>={}, page={}, idx={idx}, id={mid}", window_start, inner_page);
                                continue;
                            };
                            writer.write(&entry)
                                .context(format!("writing anime entry {} to output", entry.id))?;
                        }
                        MediaType::Manga => {
                            let Some(entry) = anilist_media_to_manga_entry(media) else {
                                let mid = media["id"].as_i64().unwrap_or(0);
                                tracing::warn!("manga entry missing id at window id>={}, page={}, idx={idx}, id={mid}", window_start, inner_page);
                                continue;
                            };
                            writer.write(&entry)
                                .context(format!("writing manga entry {} to output", entry.id))?;
                        }
                    }
                    page_written += 1;
                }

                // Flush every page so the output grows visibly and durably.
                writer.flush().context("flushing output page")?;

                // ── Checkpoint this page ──────────────────────────────────
                // The resume cursor is the (window, inner_page) of the NEXT
                // page to fetch. If this was the last page of the window,
                // that cursor jumps to the first page of the next window.
                let byte_offset = writer.byte_offset();
                pages_completed += 1;
                checkpoint.complete_page(&id, page_written, byte_offset);
                if has_next {
                    checkpoint.set_window_cursor(&id, window_start, inner_page + 1);
                } else {
                    checkpoint
                        .set_window_cursor(&id, window_start + WINDOW_SIZE, 1);
                }
                checkpoint
                    .save(&config.checkpoint_path)
                    .context("saving checkpoint")?;

                if !has_next {
                    break;
                }
                inner_page += 1;
            }

            // Advance to the next window. `inner_page` resets to 1; the resume
            // cursor already points at this next window (set above on the last
            // page of the previous window).
            window_start += WINDOW_SIZE;
            inner_page = 1;

            // Periodic progress logging
            let total_written = checkpoint.items_written(&id);
            if pages_completed.is_multiple_of(10) {
                tracing::info!(
                    "{}: {} pages (id>={}) — {} items written",
                    self.name(),
                    pages_completed,
                    window_start,
                    total_written,
                );
            }
        }

        // ── Finalize ──────────────────────────────────────────────────────
        checkpoint.complete_paginated(&id);
        checkpoint
            .save(&config.checkpoint_path)
            .context("saving final checkpoint")?;
        writer.flush()?;

        let total = checkpoint.items_written(&id);
        tracing::info!("{} complete: {} entries written", self.name(), total);
        Ok(total)
    }
}

// ── Author allowlist ───────────────────────────────────────────────────────

const AUTHOR_ROLE_ALLOWLIST: &[&str] = &[
    "Story & Art",
    "Story",
    "Art",
    "Original Creator",
    "Original Story",
];

/// Filter staff edges to only those matching the author allowlist.
///
/// AniList's `staff.edges` mixes genuine authors (Story & Art, Story, Art,
/// Original Creator, Original Story) with a large set of *production* staff
/// roles (Director, Storyboard, Script, Animation Director, ADR Director,
/// Theme Song Performance, …). Those production roles are **not** authors and
/// must never pollute the `authors` field — they are simply skipped here.
///
/// We intentionally do **not** warn on the skipped production roles: they are
/// expected and numerous, so logging them would flood the run output. Only the
/// allowlisted roles become `Author` entries. The sync test
/// [`test_author_role_allowlist_parse_sync`] guarantees every allowlisted role
/// has a matching [`parse_author_role`] arm, so `parse_author_role`'s fallback
/// is unreachable in practice.
fn filter_authors(media: &Value, _media_id: i32) -> Vec<Author> {
    let mut authors = Vec::new();
    if let Some(edges) = media["staff"]["edges"].as_array() {
        for edge in edges {
            let Some(role) = edge["role"].as_str() else {
                continue;
            };
            if AUTHOR_ROLE_ALLOWLIST.contains(&role) {
                authors.push(Author {
                    id: edge["node"]["id"].as_i64().unwrap_or(0) as i32,
                    name: edge["node"]["name"]["full"]
                        .as_str()
                        .unwrap_or("")
                        .to_string(),
                    role: parse_author_role(role),
                });
            }
            // Non-allowlisted roles are production staff, not authors — skip
            // silently. (No warning: expected and far too noisy to log.)
        }
    }
    authors
}

/// Map an author role string from the AniList staff edge to an `AuthorRole` enum.
///
/// The match arms **must** stay in sync with [`AUTHOR_ROLE_ALLOWLIST`] — every
/// role in the allowlist needs a corresponding case here.  The `_ =>` fallback
/// exists only as a safety net for future drift: if a new role gets added to
/// the allowlist but *not* to this function, the fallback will fire a warning
/// instead of silently dropping the entry.
///
/// The fallback maps to `OriginalCreator` rather than panicking because this
/// code runs in an unattended pipeline where a panic would kill the entire
/// multi-hour run for a single misconfigured role.  The warning + sentinel is
/// preferable to a 0‑entries batch.
fn parse_author_role(s: &str) -> AuthorRole {
    match s {
        "Story & Art" => AuthorRole::StoryArt,
        "Story" => AuthorRole::Story,
        "Art" => AuthorRole::Art,
        "Original Creator" => AuthorRole::OriginalCreator,
        "Original Story" => AuthorRole::OriginalStory,
        _ => {
            tracing::warn!("Unknown author role in parse: {s}");
            AuthorRole::OriginalCreator
        }
    }
}

// ── Field parsers ──────────────────────────────────────────────────────────

fn parse_fuzzy_date(date: &Value) -> Option<FuzzyDate> {
    if date.is_null() || !date.is_object() {
        return None;
    }
    let year = date["year"].as_i64()?;
    Some(FuzzyDate {
        year: year as i32,
        month: date["month"].as_i64().map(|m| m as i32),
        day: date["day"].as_i64().map(|d| d as i32),
    })
}

fn flatten_tags(media: &Value) -> Option<Vec<String>> {
    let tags = media["tags"].as_array()?;
    if tags.is_empty() {
        return None;
    }
    let names: Vec<String> = tags
        .iter()
        .filter_map(|t| t["name"].as_str().map(String::from))
        .collect();
    if names.is_empty() {
        None
    } else {
        Some(names)
    }
}

fn parse_relations(media: &Value) -> Option<Vec<Relation>> {
    let edges = media["relations"]["edges"].as_array()?;
    if edges.is_empty() {
        return None;
    }
    let relations: Vec<Relation> = edges
        .iter()
        .filter_map(|edge| {
            let node = edge.get("node")?;
            let target = node["id"].as_i64()?;
            let target_type = match node["type"].as_str() {
                Some("ANIME") => EntryType::Anime,
                _ => EntryType::Manga,
            };
            let rel_type = match edge["relationType"].as_str() {
                Some("SEQUEL") => RelationType::Sequel,
                Some("PREQUEL") => RelationType::Prequel,
                Some("SIDE_STORY") => RelationType::SideStory,
                Some("ADAPTATION") => RelationType::Adaptation,
                Some("SPIN_OFF") => RelationType::SpinOff,
                Some("CHARACTER") => RelationType::Character,
                Some("SUMMARY") => RelationType::Summary,
                Some("ALTERNATIVE") => RelationType::Alternative,
                Some("PARENT") => RelationType::Parent,
                Some("CONTAINS") => RelationType::Contains,
                _ => RelationType::Unknown,
            };
            Some(Relation {
                r#type: rel_type,
                target_type,
                target: target as i32,
            })
        })
        .collect();
    if relations.is_empty() {
        None
    } else {
        Some(relations)
    }
}

fn parse_studios(media: &Value) -> Option<Vec<Studio>> {
    let nodes = media["studios"]["nodes"].as_array()?;
    if nodes.is_empty() {
        return None;
    }
    let studios: Vec<Studio> = nodes
        .iter()
        .filter_map(|n| {
            let id = n["id"].as_i64()?;
            let name = n["name"].as_str()?;
            Some(Studio {
                id: id as i32,
                name: name.to_string(),
            })
        })
        .collect();
    if studios.is_empty() {
        None
    } else {
        Some(studios)
    }
}

fn parse_anilist_artwork(media: &Value) -> Option<Vec<anigraph_model::Artwork>> {
    let mut artworks = Vec::new();
    if let Some(url) = media["coverImage"]["large"].as_str() {
        artworks.push(anigraph_model::Artwork {
            r#type: anigraph_model::ArtworkType::Poster,
            provider: anigraph_model::ArtworkProvider::Anilist,
            url: url.to_string(),
            width: None,
            height: None,
            language: None,
        });
    }
    if let Some(url) = media["bannerImage"].as_str() {
        artworks.push(anigraph_model::Artwork {
            r#type: anigraph_model::ArtworkType::Banner,
            provider: anigraph_model::ArtworkProvider::Anilist,
            url: url.to_string(),
            width: None,
            height: None,
            language: None,
        });
    }
    if artworks.is_empty() {
        None
    } else {
        Some(artworks)
    }
}

// ── Main mapping functions ────────────────────────────────────────────────

/// Convert an AniList GraphQL media object to an AnimeEntry.
///
/// Returns `None` if the media object lacks an `id` field — this is the
/// only hard-required field. All other fields have sensible defaults or
/// are `Option`-al.
pub fn anilist_media_to_anime_entry(media: &Value) -> Option<AnimeEntry> {
    let id = media["id"].as_i64()? as i32;

    let mal_id = media["idMal"].as_i64().map(|m| m as i32);
    let ids = mal_id.map(|mal| CrossIds {
        mal: Some(mal),
        anidb: None,
        kitsu: None,
        tvdb: None,
        tmdb_tv: None,
        tmdb_movie: Vec::new(),
        imdb: None,
        anime_planet: None,
        anisearch: None,
        livechart: None,
        simkl: None,
        animecountdown: None,
        animenewsnetwork: None,
    });

    Some(AnimeEntry {
        id,
        r#type: EntryType::Anime,
        sources: vec!["anilist".to_string()],

        ids,

        titles: Title {
            romaji: media["title"]["romaji"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            english: media["title"]["english"].as_str().map(String::from),
            native: media["title"]["native"].as_str().map(String::from),
        },

        synonyms: media["synonyms"]
            .as_array()
            .filter(|a| !a.is_empty())
            .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect()),

        description: media["description"].as_str().map(String::from),

        format: match media["format"].as_str() {
            Some("TV") => anigraph_model::AnimeFormat::Tv,
            Some("MOVIE") => anigraph_model::AnimeFormat::Movie,
            Some("OVA") => anigraph_model::AnimeFormat::Ova,
            Some("ONA") => anigraph_model::AnimeFormat::Ona,
            Some("SPECIAL") => anigraph_model::AnimeFormat::Special,
            Some("MUSIC") => anigraph_model::AnimeFormat::Music,
            Some("TV_SHORT") => anigraph_model::AnimeFormat::TvShort,
            _ => anigraph_model::AnimeFormat::Unknown,
        },

        episodes_count: media["episodes"].as_i64().unwrap_or(0) as i32,
        duration: media["duration"].as_i64().map(|d| d as i32),

        status: match media["status"].as_str() {
            Some("FINISHED") => MediaStatus::Finished,
            Some("RELEASING") => MediaStatus::Releasing,
            Some("NOT_YET_RELEASED") => MediaStatus::NotYetReleased,
            Some("CANCELLED") => MediaStatus::Cancelled,
            Some("HIATUS") => MediaStatus::Hiatus,
            _ => MediaStatus::Unknown,
        },

        source: parse_source(media["source"].as_str()),
        age_rating: match media["isAdult"].as_bool() {
            Some(true) => Some(AgeRating::R18),
            _ => None,
        },

        season: {
            let season = match media["season"].as_str() {
                Some("WINTER") => Some(Season::Winter),
                Some("SPRING") => Some(Season::Spring),
                Some("SUMMER") => Some(Season::Summer),
                Some("FALL") => Some(Season::Fall),
                _ => None,
            };
            let year = media["seasonYear"].as_i64();
            season.zip(year).map(|(s, y)| SeasonInfo {
                season: s,
                year: y as i32,
            })
        },

        dates: {
            let start = parse_fuzzy_date(&media["startDate"]);
            let end = parse_fuzzy_date(&media["endDate"]);
            if start.is_some() || end.is_some() {
                Some(anigraph_model::DateRange { start, end })
            } else {
                None
            }
        },

        genres: media["genres"]
            .as_array()
            .filter(|a| !a.is_empty())
            .map(|a| a.iter().filter_map(|g| g.as_str().map(String::from)).collect()),

        tags: flatten_tags(media),

        studios: parse_studios(media),

        authors: {
            let authors = filter_authors(media, id);
            if authors.is_empty() {
                None
            } else {
                Some(authors)
            }
        },

        score: {
            let avg = media["averageScore"].as_i64();
            let mean = media["meanScore"].as_i64();
            let pop = media["popularity"].as_i64();
            if avg.is_some() || mean.is_some() || pop.is_some() {
                Some(Score {
                    average: avg.map(|s| s as i32),
                    mean: mean.map(|s| s as i32),
                    popularity: pop.map(|p| p as i32),
                })
            } else {
                None
            }
        },

        artwork: parse_anilist_artwork(media),
        relations: parse_relations(media),
        episodes: None, // Filled by TVDB enrichment phase
    })
}

/// Convert an AniList GraphQL media object to a MangaEntry.
///
/// Returns `None` if the media object lacks an `id` field.
pub fn anilist_media_to_manga_entry(media: &Value) -> Option<MangaEntry> {
    let id = media["id"].as_i64()? as i32;

    let mal_id = media["idMal"].as_i64().map(|m| m as i32);
    let ids = mal_id.map(|mal| CrossIds {
        mal: Some(mal),
        anidb: None,
        kitsu: None,
        tvdb: None,
        tmdb_tv: None,
        tmdb_movie: Vec::new(),
        imdb: None,
        anime_planet: None,
        anisearch: None,
        livechart: None,
        simkl: None,
        animecountdown: None,
        animenewsnetwork: None,
    });

    Some(MangaEntry {
        id,
        r#type: EntryType::Manga,
        sources: vec!["anilist".to_string()],

        ids,

        titles: Title {
            romaji: media["title"]["romaji"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            english: media["title"]["english"].as_str().map(String::from),
            native: media["title"]["native"].as_str().map(String::from),
        },

        synonyms: media["synonyms"]
            .as_array()
            .filter(|a| !a.is_empty())
            .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect()),

        description: media["description"].as_str().map(String::from),

        format: match media["format"].as_str() {
            Some("MANGA") => MangaFormat::Manga,
            Some("NOVEL") => MangaFormat::Novel,
            Some("ONE_SHOT") => MangaFormat::OneShot,
            _ => MangaFormat::Unknown,
        },

        chapters_count: media["chapters"].as_i64().map(|c| c as i32),
        volumes_count: media["volumes"].as_i64().map(|v| v as i32),

        status: match media["status"].as_str() {
            Some("FINISHED") => MediaStatus::Finished,
            Some("RELEASING") => MediaStatus::Releasing,
            Some("NOT_YET_RELEASED") => MediaStatus::NotYetReleased,
            Some("CANCELLED") => MediaStatus::Cancelled,
            Some("HIATUS") => MediaStatus::Hiatus,
            _ => MediaStatus::Unknown,
        },

        source: parse_source(media["source"].as_str()),
        age_rating: match media["isAdult"].as_bool() {
            Some(true) => Some(AgeRating::R18),
            _ => None,
        },

        dates: {
            let start = parse_fuzzy_date(&media["startDate"]);
            let end = parse_fuzzy_date(&media["endDate"]);
            if start.is_some() || end.is_some() {
                Some(anigraph_model::DateRange { start, end })
            } else {
                None
            }
        },

        genres: media["genres"]
            .as_array()
            .filter(|a| !a.is_empty())
            .map(|a| a.iter().filter_map(|g| g.as_str().map(String::from)).collect()),

        tags: flatten_tags(media),

        authors: {
            let authors = filter_authors(media, id);
            if authors.is_empty() {
                None
            } else {
                Some(authors)
            }
        },

        score: {
            let avg = media["averageScore"].as_i64();
            let mean = media["meanScore"].as_i64();
            let pop = media["popularity"].as_i64();
            if avg.is_some() || mean.is_some() || pop.is_some() {
                Some(Score {
                    average: avg.map(|s| s as i32),
                    mean: mean.map(|s| s as i32),
                    popularity: pop.map(|p| p as i32),
                })
            } else {
                None
            }
        },

        artwork: parse_anilist_artwork(media),
        relations: parse_relations(media),
    })
}

fn parse_source(s: Option<&str>) -> Option<MediaSource> {
    match s {
        Some("ORIGINAL") => Some(MediaSource::Original),
        Some("MANGA") => Some(MediaSource::Manga),
        Some("LIGHT_NOVEL") => Some(MediaSource::LightNovel),
        Some("VISUAL_NOVEL") => Some(MediaSource::VisualNovel),
        Some("VIDEO_GAME") => Some(MediaSource::VideoGame),
        Some("OTHER") => Some(MediaSource::Other),
        Some("NOVEL") => Some(MediaSource::Novel),
        Some("DOUJIN") => Some(MediaSource::Doujin),
        Some("WEB_MANGA") => Some(MediaSource::WebManga),
        Some("PRINT") => Some(MediaSource::Print),
        Some("COMIC") => Some(MediaSource::Comic),
        Some("BOOK") => Some(MediaSource::Book),
        Some("CARD_GAME") => Some(MediaSource::CardGame),
        Some("MIXED_MEDIA") => Some(MediaSource::MixedMedia),
        Some("RADIO") => Some(MediaSource::Radio),
        Some("PICTURE_BOOK") => Some(MediaSource::PictureBook),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anigraph_model::{AnimeFormat, AuthorRole, MediaSource, MediaStatus, MangaFormat};
    use serde_json::json;

    // ── Allowlist / parse_author_role sync ────────────────────────────────

    /// Verify that every role in `AUTHOR_ROLE_ALLOWLIST` maps to its
    /// **specific** `AuthorRole` variant via `parse_author_role`.
    ///
    /// If a new role is added to the allowlist but the corresponding match arm
    /// in `parse_author_role` is forgotten, the function's `_ =>` fallback
    /// would silently map it to `OriginalCreator`.  This test catches that
    /// drift by checking each role against its expected variant.  A new
    /// allowlist entry without a corresponding match arm here causes a
    /// compile-time panic (`other => panic!`).
    #[test]
    fn test_author_role_allowlist_parse_sync() {
        for &role in AUTHOR_ROLE_ALLOWLIST {
            let parsed = parse_author_role(role);
            match role {
                "Story & Art" => assert_eq!(parsed, AuthorRole::StoryArt),
                "Story" => assert_eq!(parsed, AuthorRole::Story),
                "Art" => assert_eq!(parsed, AuthorRole::Art),
                "Original Creator" => assert_eq!(parsed, AuthorRole::OriginalCreator),
                "Original Story" => assert_eq!(parsed, AuthorRole::OriginalStory),
                other => panic!("allowlist entry {other:?} has no assertion in sync test"),
            }
        }
    }

    // ── Full anime entry mapping ──────────────────────────────────────────

    #[test]
    fn test_anime_mapping_full() {
        let media = json!({
            "id": 1,
            "title": {
                "romaji": "Cowboy Bebop",
                "english": "Cowboy Bebop",
                "native": "カウボーイビバップ"
            },
            "synonyms": ["Cowboy Bebop (JP)"],
            "description": "A space western...",
            "format": "TV",
            "episodes": 26,
            "duration": 24,
            "status": "FINISHED",
            "source": "ORIGINAL",
            "isAdult": false,
            "season": "SPRING",
            "seasonYear": 1998,
            "startDate": { "year": 1998, "month": 4, "day": 3 },
            "endDate": { "year": 1999, "month": 4, "day": 24 },
            "genres": ["Action", "Adventure", "Drama", "Sci-Fi"],
            "tags": [
                { "name": "Space", "rank": 95 },
                { "name": "Noir", "rank": 90 }
            ],
            "studios": { "nodes": [{ "id": 14, "name": "Sunrise" }] },
            "staff": {
                "edges": [
                    {
                        "role": "Original Creator",
                        "node": { "id": 12, "name": { "full": "Hajime Yatate" } }
                    },
                    {
                        "role": "Director",
                        "node": { "id": 5, "name": { "full": "Shinichiro Watanabe" } }
                    }
                ]
            },
            "averageScore": 86,
            "meanScore": 83,
            "popularity": 45000,
            "coverImage": { "large": "https://example.com/cover.jpg" },
            "bannerImage": "https://example.com/banner.jpg",
            "idMal": 1,
            "relations": {
                "edges": [
                    { "node": { "id": 5, "type": "ANIME" }, "relationType": "SEQUEL" }
                ]
            }
        });

        let entry = anilist_media_to_anime_entry(&media).unwrap();

        assert_eq!(entry.id, 1);
        assert_eq!(entry.titles.romaji, "Cowboy Bebop");
        assert_eq!(entry.titles.english.as_deref(), Some("Cowboy Bebop"));
        assert_eq!(entry.titles.native.as_deref(), Some("カウボーイビバップ"));
        assert_eq!(entry.synonyms, Some(vec!["Cowboy Bebop (JP)".to_string()]));
        assert_eq!(entry.description.as_deref(), Some("A space western..."));
        assert_eq!(entry.format, AnimeFormat::Tv);
        assert_eq!(entry.episodes_count, 26);
        assert_eq!(entry.duration, Some(24));
        assert_eq!(entry.status, MediaStatus::Finished);
        assert_eq!(entry.source, Some(MediaSource::Original));
        assert!(entry.season.is_some());
        assert!(entry.dates.is_some());
        assert!(entry.genres.is_some());
        assert_eq!(entry.genres.as_ref().unwrap().len(), 4);
        assert!(entry.tags.is_some());
        assert_eq!(entry.tags.as_ref().unwrap(), &["Space", "Noir"]);
        assert!(entry.studios.is_some());
        assert_eq!(entry.studios.as_ref().unwrap()[0].name, "Sunrise");
        // Only allowlisted author roles — Director is excluded
        assert!(entry.authors.is_some());
        assert_eq!(entry.authors.as_ref().unwrap().len(), 1);
        assert_eq!(entry.authors.as_ref().unwrap()[0].name, "Hajime Yatate");
        assert_eq!(entry.authors.as_ref().unwrap()[0].role, AuthorRole::OriginalCreator);
        assert!(entry.score.is_some());
        assert!(entry.artwork.is_some());
        assert!(entry.relations.is_some());
        assert_eq!(entry.relations.as_ref().unwrap()[0].target, 5);
        assert!(entry.ids.is_some());
        assert_eq!(entry.ids.as_ref().unwrap().mal, Some(1));
    }

    // ── Full manga entry mapping ─────────────────────────────────────────

    #[test]
    fn test_manga_mapping_full() {
        let media = json!({
            "id": 30000,
            "title": {
                "romaji": "Shingeki no Kyojin",
                "english": "Attack on Titan",
                "native": "進撃の巨人"
            },
            "synonyms": ["AoT"],
            "description": "A dark fantasy...",
            "format": "MANGA",
            "chapters": 139,
            "volumes": 34,
            "status": "FINISHED",
            "source": "ORIGINAL",
            "isAdult": false,
            "startDate": { "year": 2009, "month": 9, "day": 9 },
            "endDate": { "year": 2021, "month": 4, "day": 9 },
            "genres": ["Action", "Drama", "Fantasy", "Mystery"],
            "tags": [
                { "name": "Giant", "rank": 90 },
                { "name": "Military", "rank": 85 }
            ],
            "staff": {
                "edges": [
                    {
                        "role": "Story & Art",
                        "node": { "id": 123, "name": { "full": "Hajime Isayama" } }
                    }
                ]
            },
            "averageScore": 86,
            "meanScore": 85,
            "popularity": 100000,
            "coverImage": { "large": "https://example.com/manga-cover.jpg" },
            "bannerImage": null,
            "idMal": 23390,
            "relations": {
                "edges": []
            }
        });

        let entry = anilist_media_to_manga_entry(&media).unwrap();

        assert_eq!(entry.id, 30000);
        assert_eq!(entry.titles.romaji, "Shingeki no Kyojin");
        assert_eq!(entry.format, MangaFormat::Manga);
        assert_eq!(entry.chapters_count, Some(139));
        assert_eq!(entry.volumes_count, Some(34));
        assert_eq!(entry.status, MediaStatus::Finished);
        assert_eq!(entry.source, Some(MediaSource::Original));
        assert!(entry.authors.is_some());
        assert_eq!(entry.authors.as_ref().unwrap()[0].name, "Hajime Isayama");
        // No banner, so only poster artwork
        assert!(entry.artwork.is_some());
        assert_eq!(entry.artwork.as_ref().unwrap().len(), 1);
        // Empty relations list — should be None
        assert!(entry.relations.is_none());
        assert!(entry.ids.is_some());
        assert_eq!(entry.ids.as_ref().unwrap().mal, Some(23390));
    }

    // ── Minimal entries (only required fields) ────────────────────────────

    #[test]
    fn test_anime_mapping_minimal() {
        let media = json!({
            "id": 999,
            "title": { "romaji": "Minimal Test", "english": null, "native": null },
            "format": "UNKNOWN",
            "status": "NOT_YET_RELEASED",
            "isAdult": true
        });

        let entry = anilist_media_to_anime_entry(&media).unwrap();

        assert_eq!(entry.id, 999);
        assert_eq!(entry.titles.romaji, "Minimal Test");
        assert_eq!(entry.titles.english, None);
        assert_eq!(entry.titles.native, None);
        assert_eq!(entry.format, AnimeFormat::Unknown);
        assert_eq!(entry.episodes_count, 0);
        assert_eq!(entry.duration, None);
        assert_eq!(entry.status, MediaStatus::NotYetReleased);
        assert_eq!(entry.source, None);
        assert_eq!(entry.age_rating, Some(anigraph_model::AgeRating::R18));
        assert!(entry.season.is_none());
        assert!(entry.synonyms.is_none());
        assert!(entry.description.is_none());
        assert!(entry.genres.is_none());
        assert!(entry.tags.is_none());
        assert!(entry.studios.is_none());
        assert!(entry.authors.is_none());
        assert!(entry.score.is_none());
        assert!(entry.artwork.is_none());
        assert!(entry.relations.is_none());
        assert!(entry.ids.is_none());
    }

    #[test]
    fn test_manga_mapping_minimal() {
        let media = json!({
            "id": 888,
            "title": { "romaji": "Manga Minimal", "english": null, "native": null },
            "format": "ONE_SHOT",
            "status": "CANCELLED",
            "isAdult": false
        });

        let entry = anilist_media_to_manga_entry(&media).unwrap();

        assert_eq!(entry.id, 888);
        assert_eq!(entry.format, MangaFormat::OneShot);
        assert_eq!(entry.chapters_count, None);
        assert_eq!(entry.volumes_count, None);
        assert_eq!(entry.status, MediaStatus::Cancelled);
        assert_eq!(entry.age_rating, None);
        assert!(entry.dates.is_none());
    }

    // ── Age rating ────────────────────────────────────────────────────────

    #[test]
    fn test_age_rating_r18() {
        let media = json!({
            "id": 10, "isAdult": true,
            "title": { "romaji": "R18 Test" },
            "format": "TV",
            "status": "RELEASING"
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert_eq!(entry.age_rating, Some(anigraph_model::AgeRating::R18));
    }

    #[test]
    fn test_age_rating_safe() {
        let media = json!({
            "id": 10, "isAdult": false,
            "title": { "romaji": "Safe Test" },
            "format": "TV",
            "status": "RELEASING"
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert_eq!(entry.age_rating, None);
    }

    #[test]
    fn test_age_rating_missing() {
        let media = json!({
            "id": 10,
            "title": { "romaji": "Missing Adult Test" },
            "format": "TV",
            "status": "RELEASING"
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert_eq!(entry.age_rating, None);
    }

    // ── Author filtering ──────────────────────────────────────────────────

    #[test]
    fn test_author_filter_allowlist() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "Author Filter" },
            "format": "TV",
            "status": "FINISHED",
            "staff": {
                "edges": [
                    { "role": "Story & Art", "node": { "id": 1, "name": { "full": "A" } } },
                    { "role": "Story", "node": { "id": 2, "name": { "full": "B" } } },
                    { "role": "Art", "node": { "id": 3, "name": { "full": "C" } } },
                    { "role": "Original Creator", "node": { "id": 4, "name": { "full": "D" } } },
                    { "role": "Original Story", "node": { "id": 5, "name": { "full": "E" } } }
                ]
            }
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.authors.is_some());
        assert_eq!(entry.authors.as_ref().unwrap().len(), 5);
    }

    #[test]
    fn test_author_filter_excludes_non_author() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "Author Exclude" },
            "format": "TV",
            "status": "FINISHED",
            "staff": {
                "edges": [
                    { "role": "Original Creator", "node": { "id": 1, "name": { "full": "Real Author" } } },
                    { "role": "Director", "node": { "id": 2, "name": { "full": "Director Person" } } },
                    { "role": "Producer", "node": { "id": 3, "name": { "full": "Producer Person" } } },
                    { "role": "Assistant", "node": { "id": 4, "name": { "full": "Assistant Person" } } }
                ]
            }
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.authors.is_some());
        assert_eq!(entry.authors.as_ref().unwrap().len(), 1);
        assert_eq!(entry.authors.as_ref().unwrap()[0].name, "Real Author");
    }

    #[test]
    fn test_no_staff_returns_no_authors() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "No Staff" },
            "format": "TV",
            "status": "FINISHED",
            "staff": { "edges": [] }
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.authors.is_none());
    }

    #[test]
    fn test_no_staff_field_returns_no_authors() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "Null Staff" },
            "format": "TV",
            "status": "FINISHED"
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.authors.is_none());
    }

    // ── Relations ─────────────────────────────────────────────────────────

    #[test]
    fn test_relation_types() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "Relations" },
            "format": "TV",
            "status": "FINISHED",
            "relations": {
                "edges": [
                    { "node": { "id": 10, "type": "ANIME" }, "relationType": "SEQUEL" },
                    { "node": { "id": 20, "type": "ANIME" }, "relationType": "PREQUEL" },
                    { "node": { "id": 30, "type": "MANGA" }, "relationType": "ADAPTATION" },
                    { "node": { "id": 40, "type": "ANIME" }, "relationType": "SIDE_STORY" },
                    { "node": { "id": 50, "type": "ANIME" }, "relationType": "SPIN_OFF" },
                    { "node": { "id": 60, "type": "MANGA" }, "relationType": "CHARACTER" },
                    { "node": { "id": 70, "type": "ANIME" }, "relationType": "SUMMARY" },
                    { "node": { "id": 80, "type": "MANGA" }, "relationType": "PARENT" },
                    { "node": { "id": 90, "type": "ANIME" }, "relationType": "CONTAINS" },
                    { "node": { "id": 100, "type": "MANGA" }, "relationType": "UNKNOWN_TYPE" }
                ]
            }
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.relations.is_some());
        let rels = entry.relations.as_ref().unwrap();
        assert_eq!(rels.len(), 10);
        assert_eq!(rels[0].r#type, RelationType::Sequel);
        assert_eq!(rels[0].target, 10);
        assert_eq!(rels[0].target_type, EntryType::Anime);
        assert_eq!(rels[2].r#type, RelationType::Adaptation);
        assert_eq!(rels[2].target_type, EntryType::Manga);
        assert_eq!(rels[9].r#type, RelationType::Unknown);
    }

    #[test]
    fn test_empty_relations_is_none() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "Empty Relations" },
            "format": "TV",
            "status": "FINISHED",
            "relations": { "edges": [] }
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.relations.is_none());
    }

    // ── Source parsing ────────────────────────────────────────────────────

    #[test]
    fn test_parse_source_known() {
        for (s, expected) in [
            ("ORIGINAL", MediaSource::Original),
            ("MANGA", MediaSource::Manga),
            ("LIGHT_NOVEL", MediaSource::LightNovel),
            ("VISUAL_NOVEL", MediaSource::VisualNovel),
            ("VIDEO_GAME", MediaSource::VideoGame),
            ("OTHER", MediaSource::Other),
            ("NOVEL", MediaSource::Novel),
            ("DOUJIN", MediaSource::Doujin),
            ("WEB_MANGA", MediaSource::WebManga),
            ("PRINT", MediaSource::Print),
            ("COMIC", MediaSource::Comic),
            ("BOOK", MediaSource::Book),
            ("CARD_GAME", MediaSource::CardGame),
            ("MIXED_MEDIA", MediaSource::MixedMedia),
            ("RADIO", MediaSource::Radio),
            ("PICTURE_BOOK", MediaSource::PictureBook),
        ] {
            assert_eq!(parse_source(Some(s)), Some(expected), "source={s}");
        }
    }

    #[test]
    fn test_parse_source_unknown() {
        assert_eq!(parse_source(Some("NOT_A_REAL_SOURCE")), None);
        assert_eq!(parse_source(None), None);
    }

    // ── Season parsing ────────────────────────────────────────────────────

    #[test]
    fn test_season_mapping() {
        for (season_str, expected_name) in [
            ("WINTER", Season::Winter),
            ("SPRING", Season::Spring),
            ("SUMMER", Season::Summer),
            ("FALL", Season::Fall),
        ] {
            let media = json!({
                "id": 1,
                "title": { "romaji": "Season Test" },
                "format": "TV",
                "status": "FINISHED",
                "season": season_str,
                "seasonYear": 2024
            });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.season.is_some());
            assert_eq!(entry.season.as_ref().unwrap().season, expected_name);
            assert_eq!(entry.season.as_ref().unwrap().year, 2024);
        }
    }

    #[test]
    fn test_missing_season_is_none() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "No Season" },
            "format": "TV",
            "status": "FINISHED"
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.season.is_none());
    }

    // ── Fuzzy date parsing ────────────────────────────────────────────────

    #[test]
    fn test_fuzzy_date_full() {
        let date = json!({ "year": 1998, "month": 4, "day": 3 });
        let fd = parse_fuzzy_date(&date).unwrap();
        assert_eq!(fd.year, 1998);
        assert_eq!(fd.month, Some(4));
        assert_eq!(fd.day, Some(3));
    }

    #[test]
    fn test_fuzzy_date_year_only() {
        let date = json!({ "year": 2020, "month": null, "day": null });
        let fd = parse_fuzzy_date(&date).unwrap();
        assert_eq!(fd.year, 2020);
        assert_eq!(fd.month, None);
        assert_eq!(fd.day, None);
    }

    #[test]
    fn test_fuzzy_date_null_is_none() {
        assert!(parse_fuzzy_date(&json!(null)).is_none());
    }

    #[test]
    fn test_fuzzy_date_no_year_is_none() {
        let date = json!({ "month": 4 });
        assert!(parse_fuzzy_date(&date).is_none());
    }

    // ── Artwork ───────────────────────────────────────────────────────────

    #[test]
    fn test_artwork_both_present() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "Art" },
            "format": "TV",
            "status": "FINISHED",
            "coverImage": { "large": "https://example.com/c.jpg" },
            "bannerImage": "https://example.com/b.jpg"
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.artwork.is_some());
        assert_eq!(entry.artwork.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_artwork_cover_only() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "Cover Only" },
            "format": "TV",
            "status": "FINISHED",
            "coverImage": { "large": "https://example.com/c.jpg" }
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.artwork.is_some());
        assert_eq!(entry.artwork.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_artwork_no_cover_no_banner() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "No Art" },
            "format": "TV",
            "status": "FINISHED"
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.artwork.is_none());
    }

    // ── Status mapping ────────────────────────────────────────────────────

    #[test]
    fn test_status_mapping() {
        for (s, expected) in [
            ("FINISHED", MediaStatus::Finished),
            ("RELEASING", MediaStatus::Releasing),
            ("NOT_YET_RELEASED", MediaStatus::NotYetReleased),
            ("CANCELLED", MediaStatus::Cancelled),
            ("HIATUS", MediaStatus::Hiatus),
            ("UNKNOWN_STATUS", MediaStatus::Unknown),
        ] {
            let media = json!({
                "id": 1,
                "title": { "romaji": "Status" },
                "format": "TV",
                "status": s
            });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert_eq!(entry.status, expected, "status={s}");
        }
    }

    // ── Format mapping (anime) ────────────────────────────────────────────

    #[test]
    fn test_anime_format_mapping() {
        for (s, expected) in [
            ("TV", AnimeFormat::Tv),
            ("MOVIE", AnimeFormat::Movie),
            ("OVA", AnimeFormat::Ova),
            ("ONA", AnimeFormat::Ona),
            ("SPECIAL", AnimeFormat::Special),
            ("MUSIC", AnimeFormat::Music),
            ("TV_SHORT", AnimeFormat::TvShort),
            ("BOGUS_FORMAT", AnimeFormat::Unknown),
        ] {
            let media = json!({
                "id": 1,
                "title": { "romaji": "Format" },
                "format": s,
                "status": "FINISHED"
            });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert_eq!(entry.format, expected, "format={s}");
        }
    }

    // ── Format mapping (manga) ────────────────────────────────────────────

    #[test]
    fn test_manga_format_mapping() {
        for (s, expected) in [
            ("MANGA", MangaFormat::Manga),
            ("NOVEL", MangaFormat::Novel),
            ("ONE_SHOT", MangaFormat::OneShot),
            ("BOGUS_FORMAT", MangaFormat::Unknown),
        ] {
            let media = json!({
                "id": 1,
                "title": { "romaji": "Manga Format" },
                "format": s,
                "status": "FINISHED"
            });
        let entry = anilist_media_to_manga_entry(&media).unwrap();
        assert_eq!(entry.format, expected, "format={s}");
        }
    }

    // ── CrossIds — idMal ──────────────────────────────────────────────────

    #[test]
    fn test_cross_ids_with_mal() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "MAL Test" },
            "format": "TV",
            "status": "FINISHED",
            "idMal": 42
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.ids.is_some());
        assert_eq!(entry.ids.as_ref().unwrap().mal, Some(42));
    }

    #[test]
    fn test_cross_ids_without_mal() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "No MAL" },
            "format": "TV",
            "status": "FINISHED"
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.ids.is_none());
    }

    // ── Score ─────────────────────────────────────────────────────────────

    #[test]
    fn test_score_all_fields() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "Score" },
            "format": "TV",
            "status": "FINISHED",
            "averageScore": 80,
            "meanScore": 75,
            "popularity": 5000
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.score.is_some());
        let s = entry.score.as_ref().unwrap();
        assert_eq!(s.average, Some(80));
        assert_eq!(s.mean, Some(75));
        assert_eq!(s.popularity, Some(5000));
    }

    #[test]
    fn test_score_none() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "No Score" },
            "format": "TV",
            "status": "FINISHED"
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.score.is_none());
    }

    // ── Tags ──────────────────────────────────────────────────────────────

    #[test]
    fn test_tags_empty_returns_none() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "No Tags" },
            "format": "TV",
            "status": "FINISHED",
            "tags": []
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.tags.is_none());
    }

    #[test]
    fn test_synonyms_empty_returns_none() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "No Synonyms" },
            "format": "TV",
            "status": "FINISHED",
            "synonyms": []
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.synonyms.is_none());
    }

    // ── Genres ────────────────────────────────────────────────────────────

    #[test]
    fn test_genres_empty_returns_none() {
        let media = json!({
            "id": 1,
            "title": { "romaji": "No Genres" },
            "format": "TV",
            "status": "FINISHED",
            "genres": []
        });
        let entry = anilist_media_to_anime_entry(&media).unwrap();
        assert!(entry.genres.is_none());
    }
}

