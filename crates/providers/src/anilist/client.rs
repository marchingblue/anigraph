use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;

// ── Error type ────────────────────────────────────────────────────────────────

/// Structured errors from the AniList API, so callers can discriminate without
/// string‑matching on `anyhow::Error` messages.
#[derive(Debug)]
#[non_exhaustive]
pub enum AnilistError {
    /// HTTP 429 — server says slow down.
    /// Contains the server‑supplied `Retry‑After` duration (or a default).
    RateLimited(Duration),
    /// HTTP 4xx other than 429 — client error (bad query, auth issue, …).
    ClientError { status: u16, body: String },
    /// HTTP 5xx — transient server error.
    ServerError { status: u16, body: String },
    /// GraphQL‑level error (malformed query, unknown field, …).
    GraphqlError(String),
    /// Network / transport error (timeout, DNS failure, connection reset, …).
    NetworkError(reqwest::Error),
}

impl std::fmt::Display for AnilistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimited(d) => write!(f, "AniList rate limited, retry after {d:?}"),
            Self::ClientError { status, body } => {
                write!(f, "AniList client error {status}: {body}")
            }
            Self::ServerError { status, body } => {
                write!(f, "AniList server error {status}: {body}")
            }
            Self::GraphqlError(msg) => write!(f, "AniList GraphQL error: {msg}"),
            Self::NetworkError(e) => write!(f, "AniList network error: {e}"),
        }
    }
}

impl std::error::Error for AnilistError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NetworkError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for AnilistError {
    fn from(e: reqwest::Error) -> Self {
        Self::NetworkError(e)
    }
}

// ── Rate Limiter ──────────────────────────────────────────────────────────────

/// Token-bucket rate limiter for AniList API.
///
/// AniList rate-limits by **query complexity**, not raw request count, and the
/// full 27-field enumeration query is expensive. The safe ceiling for this
/// query shape seems to be around 20 req/min (one token every ~3s); higher
/// values have triggered 429s. If the query is later lightened (fewer expensive
/// fields), this can go back up toward 25-30. The bucket refills one token
/// every `60_000 / rpm` ms.
struct RateLimiter {
    tokens: f64,
    last_refill: Instant,
    /// Minimum interval between requests (burst protection).
    min_interval: Duration,
    last_request: Instant,
    /// Target requests per minute.
    rate: f64,
}

impl RateLimiter {
    fn new(tokens_per_minute: u32) -> Self {
        let rate = tokens_per_minute as f64;
        Self {
            tokens: rate,
            last_refill: Instant::now(),
            min_interval: Duration::from_millis(60_000 / tokens_per_minute as u64),
            last_request: Instant::now() - Duration::from_secs(10),
            rate,
        }
    }

    fn tokens_per_minute(&self) -> f64 {
        self.rate
    }

    /// Wait until a token is available. Respects both bucket and burst limits.
    async fn acquire(&mut self) {
        // Refill tokens
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * (self.tokens_per_minute() / 60.0))
            .min(self.tokens_per_minute());
        self.last_refill = now;

        // If no tokens, wait for refill
        if self.tokens < 1.0 {
            let wait =
                Duration::from_secs_f64((1.0 - self.tokens) * 60.0 / self.tokens_per_minute());
            tracing::debug!(
                wait_ms = wait.as_millis(),
                "rate limiter: waiting for token"
            );
            tokio::time::sleep(wait).await;
            self.tokens = 1.0;
            self.last_refill = Instant::now();
        }

        // Burst protection: enforce minimum interval
        let since_last = self.last_request.elapsed();
        if since_last < self.min_interval {
            let wait = self.min_interval - since_last;
            tracing::debug!(wait_ms = wait.as_millis(), "rate limiter: burst protection");
            tokio::time::sleep(wait).await;
        }

        self.tokens -= 1.0;
        self.last_request = Instant::now();
    }

    /// Handle a 429 response by sleeping for the retry duration.
    ///
    /// The wait is **capped at 120 seconds** so a transient rate-limit
    /// cannot hang the pipeline for longer than 2 minutes per strike. (At the
    /// configured ~12 req/min we should never hit this, but if AniList's
    /// complexity budget is temporarily tighter we recover quickly instead of
    /// stalling for 5 minutes.)
    async fn handle_rate_limit(&mut self, retry_after_secs: u64) {
        const MAX_WAIT: Duration = Duration::from_secs(120);
        let wait = Duration::from_secs(retry_after_secs).min(MAX_WAIT);
        tracing::warn!(
            wait_secs = wait.as_secs(),
            "rate limiter: 429 received, sleeping (capped at {}s)",
            MAX_WAIT.as_secs(),
        );
        tokio::time::sleep(wait).await;
        // Refill bucket after being rate limited
        self.tokens = self.tokens_per_minute();
        self.last_refill = Instant::now();
    }
}

/// The rich enumeration query used to fetch all anime/manga entries from AniList.
///
/// **Staff (authors) is deliberately excluded** — it was the dominant server-time
/// cost (~7s+ vs ~2.5s for the same query without it). A dedicated staff
/// enrichment phase can backfill `Author`s in a future pass.
pub const ENUMERATION_QUERY: &str = r#"
    query EnumPage($page: Int, $perPage: Int, $type: MediaType, $ids: [Int]) {
        Page(page: $page, perPage: $perPage) {
            pageInfo { hasNextPage }
            media(type: $type, sort: ID, id_in: $ids) {
                id
                title { romaji english native }
                synonyms
                description(asHtml: false)
                format
                episodes
                duration
                status
                source(version: 2)
                isAdult
                season
                seasonYear
                startDate { year month day }
                endDate { year month day }
                genres
                tags { name }
                studios(isMain: true) { nodes { id name } }
                averageScore
                meanScore
                popularity
                coverImage { large }
                bannerImage
                idMal
                relations { edges { node { id type } relationType } }
            }
        }
    }
"#;

// ── Client ────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AnilistClient {
    client: reqwest::Client,
    token: Option<crate::anilist::auth::TokenSet>,
    limiter: Arc<Mutex<RateLimiter>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnimeDetail {
    pub id: i32,
    pub title_romaji: Option<String>,
    pub title_english: Option<String>,
    pub title_native: Option<String>,
    pub format: Option<String>,
    pub episodes: Option<i32>,
    pub status: Option<String>,
    pub season: Option<String>,
    pub season_year: Option<i32>,
    pub description: Option<String>,
    pub cover_url: Option<String>,
    pub banner_url: Option<String>,
    pub average_score: Option<f64>,
    pub popularity: Option<i32>,
    pub genres: Vec<String>,
    pub mean_score: Option<f64>,
    pub favourites: Option<i32>,
    pub trending: Option<i32>,
    pub next_airing_episode: Option<i32>,
}

/// Parse an `AnimeDetail` from a GraphQL `Media` JSON object.
fn anime_detail_from_media(media: &Value) -> AnimeDetail {
    let genres = media["genres"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|g| g.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    AnimeDetail {
        id: media["id"].as_i64().unwrap_or(0) as i32,
        title_romaji: media["title"]["romaji"].as_str().map(String::from),
        title_english: media["title"]["english"].as_str().map(String::from),
        title_native: media["title"]["native"].as_str().map(String::from),
        format: media["format"].as_str().map(String::from),
        episodes: media["episodes"].as_i64().map(|e| e as i32),
        status: media["status"].as_str().map(String::from),
        season: media["season"].as_str().map(String::from),
        season_year: media["seasonYear"].as_i64().map(|y| y as i32),
        description: media["description"].as_str().map(String::from),
        cover_url: media["coverImage"]["large"].as_str().map(String::from),
        banner_url: media["bannerImage"].as_str().map(String::from),
        average_score: media["averageScore"].as_f64(),
        popularity: media["popularity"].as_i64().map(|p| p as i32),
        genres,
        mean_score: media["meanScore"].as_f64(),
        favourites: media["favourites"].as_i64().map(|f| f as i32),
        trending: media["trending"].as_i64().map(|t| t as i32),
        next_airing_episode: media["nextAiringEpisode"]["episode"]
            .as_i64()
            .map(|e| e as i32),
    }
}

impl AnilistClient {
    /// Create a client without authentication (for public API queries).
    pub fn new_unauthenticated() -> Self {
        Self {
            client: reqwest::Client::new(),
            token: None,
            limiter: Arc::new(Mutex::new(RateLimiter::new(20))),
        }
    }

    /// Create an authenticated client.
    pub fn new(token: crate::anilist::auth::TokenSet) -> Self {
        Self {
            client: reqwest::Client::new(),
            token: Some(token),
            limiter: Arc::new(Mutex::new(RateLimiter::new(20))),
        }
    }

    // ── Low-level request helpers ────────────────────────────────────────────

    /// Make a **single** GraphQL request and return a structured error on failure.
    ///
    /// This does **no** retries — it is the inner building‑block for
    /// [`execute`](Self::execute), which adds retry logic.
    async fn execute_inner(&self, gql: &Value) -> std::result::Result<Value, AnilistError> {
        // Acquire rate limit token (pre‑emptive, not reactive).
        {
            let mut limiter = self.limiter.lock().await;
            limiter.acquire().await;
        }

        let mut req = self.client.post("https://graphql.anilist.co").json(gql);
        if let Some(ref token) = self.token {
            req = req.bearer_auth(&token.access_token);
        }

        let resp = req.send().await?; // NetworkError via From<reqwest::Error>
        let status = resp.status();

        // 429 — rate limited
        if status == 429 {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60);
            return Err(AnilistError::RateLimited(Duration::from_secs(retry_after)));
        }

        // Non‑success HTTP status → distinguish client vs server errors
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let code = status.as_u16();
            return if status.is_client_error() {
                Err(AnilistError::ClientError {
                    status: code,
                    body,
                })
            } else {
                Err(AnilistError::ServerError {
                    status: code,
                    body,
                })
            };
        }

        // Success — parse the JSON body
        let resp: Value = resp.json().await?; // NetworkError

        // GraphQL‑level errors
        if let Some(errors) = resp["errors"].as_array()
            && let Some(first) = errors.first()
        {
            let msg = first["message"].as_str().unwrap_or("unknown error");
            // AniList sometimes returns rate‑limit info inside the GraphQL body
            if msg.contains("Too Many Requests") {
                return Err(AnilistError::RateLimited(Duration::from_secs(60)));
            }
            return Err(AnilistError::GraphqlError(msg.to_string()));
        }

        Ok(resp)
    }

    /// Execute a GraphQL request with full retry logic.
    ///
    /// **Rate limits (429):** retried up to 10 consecutive times with a
    /// server‑supplied delay (capped at 300 s).  These do **not** consume the
    /// transient‑error retry budget.
    ///
    /// **Server errors (5xx) + network errors:** retried up to 3 times with
    /// exponential backoff (2 s, 4 s, 8 s).  These share the retry budget.
    ///
    /// **Client errors (4xx non‑429) + GraphQL errors:** fatal — no retry.
    async fn execute(&self, gql: Value) -> anyhow::Result<Value> {
        const MAX_RATE_LIMIT_STRIKES: u32 = 10;
        const MAX_TRANSIENT_RETRIES: u32 = 3;

        let mut rate_limit_strikes = 0u32;
        let mut transient_retries = 0u32;

        loop {
            match self.execute_inner(&gql).await {
                Ok(val) => return Ok(val),

                // Rate limits — retry without counting against transient budget
                Err(AnilistError::RateLimited(retry_after)) => {
                    rate_limit_strikes += 1;
                    if rate_limit_strikes > MAX_RATE_LIMIT_STRIKES {
                        anyhow::bail!(
                            "Rate limited {MAX_RATE_LIMIT_STRIKES}x consecutively — giving up"
                        );
                    }
                    let wait = retry_after.min(Duration::from_secs(300));
                    {
                        let mut limiter = self.limiter.lock().await;
                        limiter.handle_rate_limit(wait.as_secs()).await;
                    }
                    tracing::warn!(
                        "rate limit strike {rate_limit_strikes}/{MAX_RATE_LIMIT_STRIKES}, \
                         waited {wait:?}, retrying"
                    );
                }

                // Server errors — retry with exponential backoff
                Err(AnilistError::ServerError { status, body }) => {
                    transient_retries += 1;
                    if transient_retries > MAX_TRANSIENT_RETRIES {
                        anyhow::bail!(
                            "AniList server error {status} after {MAX_TRANSIENT_RETRIES} retries: {body}"
                        );
                    }
                    let delay = Duration::from_secs(2u64.pow(transient_retries));
                    tracing::warn!(
                        "server error {status} (retry {transient_retries}/{MAX_TRANSIENT_RETRIES}), \
                         backing off {delay:?}: {body}"
                    );
                    tokio::time::sleep(delay).await;
                }

                // Network errors — retry with exponential backoff
                Err(AnilistError::NetworkError(e)) => {
                    transient_retries += 1;
                    if transient_retries > MAX_TRANSIENT_RETRIES {
                        anyhow::bail!(
                            "Network error after {MAX_TRANSIENT_RETRIES} retries: {e}"
                        );
                    }
                    let delay = Duration::from_secs(2u64.pow(transient_retries));
                    tracing::warn!(
                        "network error (retry {transient_retries}/{MAX_TRANSIENT_RETRIES}), \
                         backing off {delay:?}: {e}"
                    );
                    tokio::time::sleep(delay).await;
                }

                // Fatal errors — propagate immediately
                Err(AnilistError::ClientError { status, body }) => {
                    anyhow::bail!("AniList client error {status}: {body}");
                }
                Err(AnilistError::GraphqlError(msg)) => {
                    anyhow::bail!("AniList GraphQL error: {msg}");
                }
            }
        }
    }

    // ── Public API methods ────────────────────────────────────────────────────

    /// Fetch a page of media entries for enumeration.
    ///
    /// Returns (media_list, has_next_page). Pages are 1-indexed.
    /// `media_type` should be "ANIME" or "MANGA".
    ///
    /// This method returns raw `serde_json::Value` — the pipeline crate
    /// handles mapping from AniList's response shape to the model structs.
    pub async fn fetch_page(
        &self,
        page: u32,
        per_page: u32,
        media_type: &str,
        ids: &[i32],
    ) -> anyhow::Result<(Vec<Value>, bool)> {
        let gql = serde_json::json!({
            "query": ENUMERATION_QUERY,
            "variables": {
                "page": page,
                "perPage": per_page,
                "type": media_type,
                "ids": ids,
            }
        });

        let resp = self.execute(gql).await?;
        let page_data = &resp["data"]["Page"];
        let has_next = page_data["pageInfo"]["hasNextPage"].as_bool().unwrap_or(false);
        let media = page_data["media"].as_array().cloned().unwrap_or_default();
        Ok((media, has_next))
    }

    /// Probe the highest AniList media ID for the given type.
    ///
    /// Used to size the ID-window enumeration sweep: the catalog is walked in
    /// fixed ID windows from 1 up to this value. Returns `0` if the catalog is
    /// unexpectedly empty.
    pub async fn max_id(&self, media_type: &str) -> anyhow::Result<i32> {
        let gql = serde_json::json!({
            "query": "query($t: MediaType) { Page(perPage: 1) { media(type: $t, sort: ID_DESC) { id } } }",
            "variables": { "t": media_type }
        });

        let resp = self.execute(gql).await?;
        let media = &resp["data"]["Page"]["media"];
        let first = media.as_array().and_then(|a| a.first());
        Ok(first
            .and_then(|m| m["id"].as_i64())
            .map(|v| v as i32)
            .unwrap_or(0))
    }

    pub async fn search_anime(
        &self,
        query: &str,
    ) -> anyhow::Result<Vec<AnimeDetail>> {
        let gql = serde_json::json!({
            "query": r#"
                query Search($search: String) {
                    Page(perPage: 10) {
                        media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
                            id
                            title { romaji english native userPreferred }
                            format episodes status season seasonYear
                            description(asHtml: false)
                            coverImage { large color }
                            bannerImage
                            averageScore popularity meanScore favourites trending
                            genres
                        }
                    }
                }
            "#,
            "variables": { "search": query }
        });

        let resp = self.execute(gql).await?;

        let media_list = resp["data"]["Page"]["media"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        Ok(media_list.into_iter().map(|m| anime_detail_from_media(&m)).collect())
    }

    /// Fetch full anime details including description, genres, etc.
    pub async fn fetch_anime_detail(&self, anilist_id: i32) -> anyhow::Result<AnimeDetail> {
        let gql = serde_json::json!({
            "query": r#"
                query Media($id: Int!) {
                    Media(id: $id, type: ANIME) {
                        id
                        title { romaji english native userPreferred }
                        format episodes status season seasonYear
                        description(asHtml: false)
                        coverImage { large color }
                        bannerImage
                        averageScore popularity meanScore favourites trending
                        genres
                        nextAiringEpisode { episode }
                    }
                }
            "#,
            "variables": { "id": anilist_id }
        });

        let resp = self.execute(gql).await?;
        let media = &resp["data"]["Media"];
        Ok(anime_detail_from_media(media))
    }

    /// Batch fetch anime details using GraphQL aliasing.
    /// Fetches up to 50 anime in a single request.
    pub async fn fetch_anime_details_batch(&self, ids: &[i32]) -> anyhow::Result<Vec<AnimeDetail>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // Build aliased query. IDs are integers so injection via string formatting is not a risk.
        let mut aliases = Vec::with_capacity(ids.len());
        for (i, id) in ids.iter().enumerate() {
            aliases.push(format!(
                "a{i}: Media(id: {id}, type: ANIME) {{
                    id
                    title {{ romaji english native userPreferred }}
                    format episodes status season seasonYear
                    description(asHtml: false)
                    coverImage {{ large color }}
                    bannerImage
                    averageScore popularity meanScore favourites trending
                    genres
                    nextAiringEpisode {{ episode }}
                }}"
            ));
        }
        let query = format!("query {{ {} }}", aliases.join("\n"));

        let gql = serde_json::json!({
            "query": query,
        });

        let resp = self.execute(gql).await?;
        let data = &resp["data"];

        if data.is_null() {
            anyhow::bail!("AniList returned null data for batch request");
        }

        let mut results = Vec::with_capacity(ids.len());
        for (i, _id) in ids.iter().enumerate() {
            let alias = format!("a{i}");
            if let Some(media) = data.get(&alias)
                && media.is_object()
            {
                results.push(anime_detail_from_media(media));
            }
        }

        Ok(results)
    }
}
