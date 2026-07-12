use serde::{Deserialize, Serialize};

// ── Entry Type ───────────────────────────────────────────────────────────────

/// Discriminator between anime and manga entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    #[serde(rename = "ANIME")]
    Anime,
    #[serde(rename = "MANGA")]
    Manga,
}

// ── Dates ────────────────────────────────────────────────────────────────────

/// A date that may have partial precision (year-only, year+month, or full date).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FuzzyDate {
    pub year: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub month: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub day: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DateRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<FuzzyDate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<FuzzyDate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeasonInfo {
    pub season: Season,
    pub year: i32,
}

// ── Titles ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Title {
    pub romaji: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub english: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<String>,
}

// ── Cross-database IDs ───────────────────────────────────────────────────────

/// Cross-references to external providers, populated by the Fribb/anime-lists
/// cross-reference phase.
///
/// All fields are optional — an entry may have no cross-references, or only
/// some.  Empty collections (`Vec`) are skipped in serialization.
///
/// `tmdb_tv` and `tmdb_movie` are separate because TMDB uses different ID
/// namespaces for TV shows vs movies — a single numeric ID can refer to
/// completely different content depending on which endpoint you hit.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrossIds {
    /// MyAnimeList ID (from AniList `idMal` + Fribb).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mal: Option<i32>,

    /// AniDB ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anidb: Option<i32>,

    /// Kitsu ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kitsu: Option<i32>,

    /// TVDB (TheTVDB) series ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tvdb: Option<i32>,

    /// TMDB series ID — hits `/tv/{id}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmdb_tv: Option<i32>,

    /// TMDB movie IDs — hits `/movie/{id}`.  A single anime movie can map
    /// to multiple TMDB movie IDs (e.g. a trilogy released as one entry).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tmdb_movie: Vec<i32>,

    /// IMDB ID(s).  Usually a single string like `"tt0102847"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imdb: Option<String>,

    /// Anime-Planet slug (string ID).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anime_planet: Option<String>,

    /// AniSearch ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anisearch: Option<i32>,

    /// LiveChart.me ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub livechart: Option<i32>,

    /// Simkl ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub simkl: Option<i32>,

    /// AnimeCountdown ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub animecountdown: Option<i32>,

    /// Anime News Network ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub animenewsnetwork: Option<i32>,
}

// ── Credits ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Studio {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Author {
    pub id: i32,
    pub name: String,
    pub role: AuthorRole,
}

/// Explicit-rename allowlist for author roles.
///
/// Only the five roles below are preserved. Any role returned by AniList that
/// does not match one of these exact strings is silently dropped (and logged
/// during generation so we can catch new authorial-sounding roles).
///
/// Note: `#[serde(rename_all = ...)]` is NOT used here because the output
/// strings contain spaces and ampersands that no case convention can produce.
/// Each variant uses an explicit `#[serde(rename = "...")]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthorRole {
    #[serde(rename = "Story & Art")]
    StoryArt,
    #[serde(rename = "Story")]
    Story,
    #[serde(rename = "Art")]
    Art,
    #[serde(rename = "Original Creator")]
    OriginalCreator,
    #[serde(rename = "Original Story")]
    OriginalStory,
}

// ── Scores ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Score {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub average: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub popularity: Option<i32>,
}

// ── Visuals ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artwork {
    pub r#type: ArtworkType,
    pub provider: ArtworkProvider,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArtworkType {
    #[serde(rename = "POSTER")]
    Poster,
    #[serde(rename = "BANNER")]
    Banner,
    #[serde(rename = "FANART")]
    Fanart,
    #[serde(rename = "CLEARLOGO")]
    Clearlogo,
    #[serde(rename = "BACKDROP")]
    Backdrop,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArtworkProvider {
    #[serde(rename = "anilist")]
    Anilist,
    #[serde(rename = "tmdb")]
    Tmdb,
    #[serde(rename = "tvdb")]
    Tvdb,
    #[serde(other)]
    Unknown,
}

// ── Relations ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Relation {
    pub r#type: RelationType,
    pub target_type: EntryType,
    pub target: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationType {
    #[serde(rename = "SEQUEL")]
    Sequel,
    #[serde(rename = "PREQUEL")]
    Prequel,
    #[serde(rename = "SIDE_STORY")]
    SideStory,
    #[serde(rename = "ADAPTATION")]
    Adaptation,
    #[serde(rename = "SPIN_OFF")]
    SpinOff,
    #[serde(rename = "CHARACTER")]
    Character,
    #[serde(rename = "OTHER")]
    Other,
    #[serde(rename = "SUMMARY")]
    Summary,
    #[serde(rename = "ALTERNATIVE")]
    Alternative,
    #[serde(rename = "PARENT")]
    Parent,
    #[serde(rename = "CONTAINS")]
    Contains,
    #[serde(other)]
    Unknown,
}

// ── Tags ─────────────────────────────────────────────────────────────────────

/// AniList returns tags as objects with `name` and `rank`.
/// We flatten to just the name string in the output schema, but keep the
/// full structure internally in case consumers want to use rank for filtering.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tag {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<i32>,
}

// ── Enums shared by Anime + Manga ────────────────────────────────────────────

/// Media format. AniList-sourced — catch-all for resilience to new values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaFormat {
    #[serde(rename = "TV")]
    Tv,
    #[serde(rename = "MOVIE")]
    Movie,
    #[serde(rename = "OVA")]
    Ova,
    #[serde(rename = "ONA")]
    Ona,
    #[serde(rename = "SPECIAL")]
    Special,
    #[serde(rename = "MUSIC")]
    Music,
    #[serde(rename = "TV_SHORT")]
    TvShort,
    #[serde(other)]
    Unknown,
}

/// Anime-specific formats. Same as MediaFormat but restricted for anime.
pub type AnimeFormat = MediaFormat;

/// Manga-specific formats.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MangaFormat {
    #[serde(rename = "MANGA")]
    Manga,
    #[serde(rename = "NOVEL")]
    Novel,
    #[serde(rename = "ONE_SHOT")]
    OneShot,
    #[serde(other)]
    Unknown,
}

/// Release status. AniList-sourced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaStatus {
    #[serde(rename = "FINISHED")]
    Finished,
    #[serde(rename = "RELEASING")]
    Releasing,
    #[serde(rename = "NOT_YET_RELEASED")]
    NotYetReleased,
    #[serde(rename = "CANCELLED")]
    Cancelled,
    #[serde(rename = "HIATUS")]
    Hiatus,
    #[serde(other)]
    Unknown,
}

/// Original source material. AniList-sourced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaSource {
    #[serde(rename = "ORIGINAL")]
    Original,
    #[serde(rename = "MANGA")]
    Manga,
    #[serde(rename = "LIGHT_NOVEL")]
    LightNovel,
    #[serde(rename = "VISUAL_NOVEL")]
    VisualNovel,
    #[serde(rename = "VIDEO_GAME")]
    VideoGame,
    #[serde(rename = "OTHER")]
    Other,
    #[serde(rename = "NOVEL")]
    Novel,
    #[serde(rename = "DOUJIN")]
    Doujin,
    #[serde(rename = "WEB_MANGA")]
    WebManga,
    #[serde(rename = "PRINT")]
    Print,
    #[serde(rename = "COMIC")]
    Comic,
    #[serde(rename = "BOOK")]
    Book,
    #[serde(rename = "CARD_GAME")]
    CardGame,
    #[serde(rename = "MIXED_MEDIA")]
    MixedMedia,
    #[serde(rename = "RADIO")]
    Radio,
    #[serde(rename = "PICTURE_BOOK")]
    PictureBook,
    #[serde(other)]
    Unknown,
}

/// Content rating.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgeRating {
    #[serde(rename = "G")]
    G,
    #[serde(rename = "PG")]
    Pg,
    #[serde(rename = "R")]
    R,
    #[serde(rename = "R18")]
    R18,
    #[serde(rename = "R18+")]
    R18Plus,
    #[serde(other)]
    Unknown,
}

/// Airing season.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Season {
    #[serde(rename = "WINTER")]
    Winter,
    #[serde(rename = "SPRING")]
    Spring,
    #[serde(rename = "SUMMER")]
    Summer,
    #[serde(rename = "FALL")]
    Fall,
    #[serde(other)]
    Unknown,
}
