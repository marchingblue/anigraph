# anigraph

*Open-source anime & manga metadata dataset generator*

> [!IMPORTANT]
> heavy use of ai assistance was involved during development

anigraph produces a complete, cross-referenced dataset of anime and manga metadata by pulling from multiple upstream sources and merging them into a unified schema. 

It's the spiritual successor of [anime-offline-database](https://github.com/manami-project/anime-offline-database) which has been archived on July 4th this year.

---

## Dataset

The final output is two [JSON Lines](https://jsonlines.org/) files, compressed with [zstd](https://facebook.github.io/zstd/):

| File | Contents |
|------|----------|
| `anigraph-anime.jsonl.zst` | ~20K anime entries with episodes, artwork, cross-references |
| `anigraph-manga.jsonl.zst` | ~130K manga entries with artwork                            |

### Additional output files

| File | Contents |
|------|----------|
| `checksums.txt` | **blake3** and **sha256** hashes of compressed files |
| `manifest.json` | Machine-readable version metadata (entry counts, file sizes, hashes) |

The manifest is useful for programmatic consumers — check its `version` field (set to the generation date) to detect whether the dataset has been updated.

---

## Quick start

### Prerequisites

- rust 2024 edition (`rustup update`)
- API keys (tmdb and tvdb)

### Setup

```bash
git clone https://github.com/marchingblue/anigraph
cd anigraph

# Configure API keys
cp .env.example .env
# Edit .env with your keys, or set env vars directly

# Build & run the full pipeline
cargo run --release -- --work-dir ./data --output-dir ./data
```

### Environment variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `TMDB_READ_KEY` | For TMDB phase | TMDB API read access token (JWT) |
| `THETVDB_KEY` | For TVDB phase | TheTVDB v4 API key |

---

## Schema

### Anime entry

```json
{
  "id": 1,
  "type": "ANIME",
  "sources": ["anilist", "fribb", "tmdb", "tvdb"],

  "ids": {
    "mal": 1,
    "anidb": 5,
    "tvdb": 2337,
    "tmdbTv": 105558,
    "tmdbMovie": [],
    "imdb": "tt0110008",
    "kitsu": 1,
    "animePlanet": "cowboy-bebop",
    "anisearch": 1,
    "livechart": 1,
    "simkl": 315,
    "animenewsnetwork": 440
  },

  "titles": {
    "romaji": "Cowboy Bebop",
    "english": "Cowboy Bebop",
    "native": "カウボーイビバップ"
  },
  "synonyms": ["Cowboy Bebop (1998)", "COWBOY BEBOP"],
  "description": "In the year 2071...",
  "format": "TV",
  "episodesCount": 26,
  "duration": 24,
  "status": "FINISHED",
  "source": "ORIGINAL",
  "ageRating": "PG",

  "season": { "season": "SPRING", "year": 1998 },
  "dates": {
    "start": { "year": 1998, "month": 4, "day": 3 },
    "end": { "year": 1999, "month": 4, "day": 24 }
  },

  "genres": ["Action", "Adventure", "Drama", "Sci-Fi"],
  "tags": ["Space", "Bounty Hunter", "Noir", "Adult Cast"],

  "studios": [{ "id": 3, "name": "Sunrise" }],
  "authors": [
    { "id": 444, "name": "Hajime Yatate", "role": "Original Creator" }
  ],

  "score": { "average": 86, "mean": 86, "popularity": 283249 },

  "artwork": [
    {
      "type": "POSTER",
      "provider": "anilist",
      "url": "https://img.anili.st/media/1",
      "width": 230,
      "height": 320
    },
    {
      "type": "POSTER",
      "provider": "tmdb",
      "url": "https://image.tmdb.org/t/p/original/...jpg",
      "width": 1000,
      "height": 1500
    }
  ],

  "relations": [
    { "type": "SEQUEL", "targetType": "ANIME", "target": 5 },
    { "type": "PREQUEL", "targetType": "ANIME", "target": 102650 }
  ],

  "episodes": [
    {
      "number": 1,
      "absolute": 1,
      "seasonNumber": 1,
      "titles": { "english": "Asteroid Blues" },
      "airDate": { "year": 1998, "month": 4, "day": 3 },
      "runtime": 24,
      "overview": "Spike and Jet chase a bounty...",
      "ids": { "tvdb": 2337001 }
    }
  ]
}
```

### Manga entry

```json
{
  "id": 30000,
  "type": "MANGA",
  "sources": ["anilist"],

  "ids": {
    "mal": 23390,
    "anidb": 13204
  },

  "titles": {
    "romaji": "Shingeki no Kyojin",
    "english": "Attack on Titan",
    "native": "進撃の巨人"
  },
  "synonyms": ["AOT", "Shingeki no Kyojin"],

  "description": "Centuries ago, mankind was destroyed...",
  "format": "MANGA",
  "chaptersCount": 139,
  "volumesCount": 34,
  "status": "FINISHED",
  "source": "ORIGINAL",
  "ageRating": "R",

  "dates": {
    "start": { "year": 2009, "month": 9, "day": 9 },
    "end": { "year": 2021, "month": 4, "day": 9 }
  },

  "genres": ["Action", "Drama", "Fantasy", "Mystery"],
  "tags": ["Military", "Shounen", "Gore", "Tragedy"],

  "authors": [
    { "id": 109139, "name": "Hajime Isayama", "role": "Story & Art" }
  ],

  "score": { "average": 83, "mean": 85, "popularity": 497810 },

  "artwork": [
    {
      "type": "POSTER",
      "provider": "anilist",
      "url": "https://img.anili.st/media/30000",
      "width": 230,
      "height": 320
    }
  ],

  "relations": [
    { "type": "ADAPTATION", "targetType": "ANIME", "target": 16498 }
  ]
}
```

---

## Data attribution

This dataset is built from multiple upstream sources. Please respect their terms:

- **[AniList](https://anilist.co)** — Primary source for basic metadata (titles, descriptions, genres, scores, dates). Data retrieved via the [AniList API v2](https://anilist.gitbook.io/anilist-apiv2-docs/).
- **[TMDB](https://www.themoviedb.org)** — Artwork images (posters, backdrops, logos). This product uses the TMDB API but is not endorsed or certified by TMDB.
- **[TheTVDB](https://thetvdb.com)** — Episode metadata and additional artwork. Data provided by TheTVDB.com.
- **[Fribb/anime-lists](https://github.com/Fribb/anime-lists)** — Cross-database ID mappings (MAL, AniDB, Kitsu, etc.).

If you redistribute or build on this dataset, include the appropriate attribution above.

---

## License

The anigraph **source code** is licensed under the [MIT License](LICENSE).

The **output dataset** incorporates data from third-party sources (see [Data attribution](#data-attribution)). Redistribution of the dataset is subject to the terms of each upstream provider.
