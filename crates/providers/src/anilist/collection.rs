use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::anilist::auth::TokenSet;

/// In-memory index of the user's AniList collection for fast exact-title lookup.
///
/// Currently fetched fresh from the API each time (no local caching).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AnilistCollection {
    entries: Vec<CollectionEntry>,
    #[serde(skip)]
    title_index: HashMap<String, i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionEntry {
    pub anilist_id: i32,
    pub status: String,
    pub progress: i32,
    pub title_romaji: Option<String>,
    pub title_english: Option<String>,
    pub title_native: Option<String>,
    pub title_user_preferred: Option<String>,
    pub episodes: Option<i32>,
    pub format: Option<String>,
    pub cover_url: Option<String>,
}

impl CollectionEntry {
    /// All title variants for matching
    pub fn title_variants(&self) -> Vec<&str> {
        let mut titles = Vec::new();
        if let Some(ref t) = self.title_romaji {
            titles.push(t.as_str());
        }
        if let Some(ref t) = self.title_english
            && !titles.contains(&t.as_str())
        {
            titles.push(t.as_str());
        }
        if let Some(ref t) = self.title_native
            && !titles.contains(&t.as_str())
        {
            titles.push(t.as_str());
        }
        if let Some(ref t) = self.title_user_preferred
            && !titles.contains(&t.as_str())
        {
            titles.push(t.as_str());
        }
        titles
    }
}

impl AnilistCollection {
    pub fn from_entries(entries: Vec<CollectionEntry>) -> Self {
        let mut title_index = HashMap::with_capacity(entries.len() * 4);
        for e in &entries {
            for t in [
                e.title_romaji.as_deref(),
                e.title_english.as_deref(),
                e.title_native.as_deref(),
                e.title_user_preferred.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                title_index
                    .entry(t.to_ascii_lowercase())
                    .or_insert(e.anilist_id);
            }
        }
        Self {
            entries,
            title_index,
        }
    }

    /// Exact case-insensitive title match (O(1))
    pub fn exact_title_match(&self, query: &str) -> Option<&CollectionEntry> {
        let id = self.title_index.get(&query.to_ascii_lowercase())?;
        self.entries.iter().find(|e| e.anilist_id == *id)
    }

    /// Tokenized match: check if all tokens in query appear in any title
    pub fn tokenized_match(&self, query: &str) -> Option<&CollectionEntry> {
        let query_tokens: Vec<&str> = query.split_whitespace().collect();
        if query_tokens.is_empty() {
            return None;
        }

        let mut best: Option<(&CollectionEntry, usize)> = None;

        for entry in &self.entries {
            let mut max_matched = 0;
            for title in entry.title_variants() {
                let title_lower = title.to_lowercase();
                let matched = query_tokens
                    .iter()
                    .filter(|t| title_lower.contains(&t.to_lowercase()))
                    .count();
                max_matched = max_matched.max(matched);
            }
            if max_matched > 0 {
                let ratio = max_matched as f64 / query_tokens.len() as f64;
                if ratio >= 0.8 && (best.is_none() || max_matched > best.unwrap().1) {
                    best = Some((entry, max_matched));
                }
            }
        }

        best.map(|(e, _)| e)
    }

    /// Find all entries whose romaji title starts with the given base string.
    /// Used for season disambiguation: "Go-toubun no Hanayome" matches both
    /// "Go-toubun no Hanayome" and "Go-toubun no Hanayome ∬".
    pub fn entries_with_base_title(&self, base: &str) -> Vec<&CollectionEntry> {
        let base_lower = base.to_ascii_lowercase();
        self.entries
            .iter()
            .filter(|e| {
                if let Some(ref t) = e.title_romaji {
                    let t_lower = t.to_ascii_lowercase();
                    t_lower == base_lower || t_lower.starts_with(&base_lower)
                } else {
                    false
                }
            })
            .collect()
    }

    /// Fetch the user's anime collection from AniList GraphQL API.
    pub async fn fetch(token: &TokenSet) -> Result<Self> {
        let client = reqwest::Client::new();

        // First, get the user's numeric ID (MediaListCollection doesn't accept "me")
        let viewer_resp: serde_json::Value = client
            .post("https://graphql.anilist.co")
            .bearer_auth(&token.access_token)
            .json(&serde_json::json!({
                "query": "{ Viewer { id } }"
            }))
            .send()
            .await
            .context("sending AniList viewer request")?
            .json()
            .await
            .context("parsing AniList viewer response")?;

        let user_id = viewer_resp["data"]["Viewer"]["id"]
            .as_i64()
            .context("no user ID in AniList viewer response")?;

        tracing::info!("AniList user ID: {user_id}");

        // Now fetch the collection with the numeric ID
        let gql = serde_json::json!({
            "query": r#"
                query Collection($userId: Int!) {
                    MediaListCollection(userId: $userId, type: ANIME) {
                        lists {
                            name
                            status
                            entries {
                                mediaId
                                status
                                progress
                                media {
                                    id
                                    title { romaji english native userPreferred }
                                    format episodes season seasonYear
                                    coverImage { large color }
                                }
                            }
                        }
                    }
                }
            "#,
            "variables": { "userId": user_id }
        });

        let resp: serde_json::Value = client
            .post("https://graphql.anilist.co")
            .bearer_auth(&token.access_token)
            .json(&gql)
            .send()
            .await
            .context("sending AniList collection request")?
            .json()
            .await
            .context("parsing AniList collection response")?;

        let lists = resp["data"]["MediaListCollection"]["lists"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        let mut all_entries = Vec::new();

        for list in &lists {
            let entries = list["entries"].as_array().cloned().unwrap_or_default();
            for entry in &entries {
                let media = &entry["media"];
                let anilist_id = media["id"].as_i64().unwrap_or(0) as i32;
                if anilist_id == 0 {
                    continue;
                }

                all_entries.push(CollectionEntry {
                    anilist_id,
                    status: entry["status"].as_str().unwrap_or("CURRENT").to_string(),
                    progress: entry["progress"].as_i64().unwrap_or(0) as i32,
                    title_romaji: media["title"]["romaji"].as_str().map(String::from),
                    title_english: media["title"]["english"].as_str().map(String::from),
                    title_native: media["title"]["native"].as_str().map(String::from),
                    title_user_preferred: media["title"]["userPreferred"]
                        .as_str()
                        .map(String::from),
                    episodes: media["episodes"].as_i64().map(|e| e as i32),
                    format: media["format"].as_str().map(String::from),
                    cover_url: media["coverImage"]["large"].as_str().map(String::from),
                });
            }
        }

        tracing::info!(
            "fetched {} entries from AniList collection",
            all_entries.len()
        );
        Ok(Self::from_entries(all_entries))
    }

    pub fn entries(&self) -> &[CollectionEntry] {
        &self.entries
    }

    /// Look up a collection entry by AniList ID.
    pub fn by_id(&self, id: i32) -> Option<&CollectionEntry> {
        self.entries.iter().find(|e| e.anilist_id == id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
