# Lichess FEN frequency service

A standalone tool that builds a local **FEN → move frequency** database from the [Lichess database](https://database.lichess.org/), so you can query human move statistics without hitting the rate-limited Lichess Explorer API.

## Purpose

- **Input**: Lichess standard rated PGNs (monthly `.pgn.zst` files from https://database.lichess.org/standard/list.txt).
- **Output**: A queryable store (SQLite or PostgreSQL): for each `(FEN, rating bucket)` you get a list of moves with **games** and **winrate**.
- **Why**: The Explorer API is heavily rate-limited. Preprocessing the Lichess DB gives the same style of data with no rate limits and fast local lookups.

Data is stored by **rating band** (e.g. per 100 Elo); at query time you specify a **bucket** (e.g. `1600-1800`) and the service aggregates the matching bands.

## Prerequisites

- **Rust** toolchain (1.70+): install from https://rustup.rs/
- **Disk**: Enough space for downloaded `.pgn.zst` files (tens of GB per month) and the SQLite DB (grows with positions and bands).
- No separate zstd CLI required; the Rust `zstd` crate is used for streaming decompression.

## Install

```bash
cargo build --release
# or
cargo install --path .
```

## Config

Copy the example config and edit paths and filters:

```bash
cp config.example.yaml config.yaml
```

`config.yaml` is gitignored so you can keep local paths and optional `database_url` out of version control.

Main fields:

| Field | Description |
|-------|-------------|
| `lichess_list_url` | URL that lists monthly PGN.zst files (default: Lichess standard list). |
| `data_dir` | Directory for downloaded `.pgn.zst` files. |
| `output_db` | Path to the SQLite database (e.g. `./data/fen_move.db`). Ignored when `database_url` is set. |
| `database_url` | Optional. When set (e.g. `postgresql://user:password@localhost:5432/fen_move`), **run** writes to PostgreSQL and **query** / **serve** use the same URL if you pass it as `--db`. |
| `rating_band_width` | Elo band width (e.g. 100 → bands 1200, 1300, …). |
| `rating_min`, `rating_max` | Only positions where the side-to-move rating is in this range (keeps DB smaller). |
| `time_controls` | List of Event substrings; only games whose `Event` tag contains one of these are included. Lichess standard DB uses e.g. `"Rated Blitz game"`, `"Rated Rapid game"`, `"Rated Classical game"`. |
| `months` | `"all"` or `"since YYYY-MM"` to limit which months are considered (CLI `--since` overrides this). |
| `download` | Whether to run the download step when using `run`. |
| `concurrency` | Number of PGN files to process in parallel (default 1). |

Bucket bounds are grid-aligned to the band width (e.g. bucket `1600-1800` with width 100 uses bands 1600, 1700, 1800).

## Run (build the DB)

**Full run** (download if enabled, then process months from config):

```bash
cargo run --release -- run --config config.yaml
```

**Incremental** (only months not yet in the manifest):

- Use `--incremental` or rely on default: already-processed months are skipped.
- Or restrict to a start month: `--since 2025-01`.

**Re-download / re-process**:

- If a month is already in the manifest, it is skipped unless you pass `--force` (re-processing the same month can double-count; avoid unless you know what you’re doing).

Process each month **at most once** in normal use; the manifest table records which months are done.

### Test / sample mode

To estimate how long a full month would take without processing everything, use `--sample N`: process at most **N games** from the **most recent month** only (downloads that month if needed). The month is **not** marked as processed, so you can run a full process later.

**If that month’s PGN file is already in `data_dir`, it is not downloaded again** — the tool skips download and uses the existing file.

After the run, the tool prints timing and an extrapolated duration for a full month (~90M games):

```bash
cargo run --release -- run --config config.yaml --sample 50000
```

Example output:

```
Sample mode: processing at most 50000 games from most recent month (.../lichess_db_standard_rated_2025-01.pgn.zst)
Processing .../lichess_db_standard_rated_2025-01.pgn.zst ...
  Processed 50000 games in 2m 15.3s
  At this rate, a full month (~90M games) would take approximately 67 minutes (1.1 hours).
  (Month not marked as processed; run without --sample to process fully.)
```

Use a value that runs for a couple of minutes (e.g. 30k–100k games depending on your machine) to get a stable estimate.

## Query (CLI)

Get move stats for a FEN and rating bucket (output is JSON to stdout):

```bash
cargo run --release -- query --db ./data/fen_move.db \
  --fen "r1bqkbnr/pppp1ppp/2n5/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R w KQkq -" \
  --bucket 1600-1800
```

Output shape:

```json
{
  "moves": [
    { "move": "f1c4", "games": 12345, "winrate": 0.52 },
    { "move": "f1b5", "games": 8000, "winrate": 0.48 }
  ]
}
```

- `move`: UCI (e.g. `e2e4`).
- `games`: number of games (summed over bands in the bucket).
- `winrate`: (wins + draws/2) / games for the side to move, in [0, 1].

Use the same **FEN normalization** as your client (6-field FEN, trimmed); this tool uses the representation from the `shakmaty` crate’s FEN output.

## Optional HTTP server

Run a small API server for querying over HTTP:

```bash
cargo run --release -- serve --db ./data/fen_move.db --bind 127.0.0.1:8080
```

- **GET** `/?fen=...&bucket=1600-1800` or **GET** `/query?fen=...&bucket=1600-1800`: returns the same JSON as the CLI.
- CORS is permissive so browser apps on other origins can call it.

## Where the DB lives

By default the config uses `output_db: "./data/fen_move.db"`. You can set it to any path; the parent directory is created if needed.

### Using PostgreSQL

Set `database_url` in `config.yaml` (e.g. `postgresql://user:password@localhost:5432/fen_move`). Then:

- **Run**: Uses PostgreSQL for the store and manifest; tables `fen_move_stats` and `processed_months` are created if missing.
- **Query / Serve**: Pass the same URL as `--db` so the CLI and HTTP server read from Postgres:
  - `query --db "postgresql://user:pass@localhost:5432/fen_move" --fen "..." --bucket 1600-1800`
  - `serve --db "postgresql://user:pass@localhost:5432/fen_move" --bind 127.0.0.1:8080`

If `--db` contains `://`, it is treated as a database URL (PostgreSQL); otherwise it is a file path (SQLite).

## Using the data

- **Same process**: Open the SQLite file (or Postgres connection) and call the same query logic (e.g. `store::query_moves`).
- **HTTP**: Run `serve` and call `/query?fen=...&bucket=...`; the response is `{ "moves": [ { "move", "games", "winrate" }, ... ] }`.

## Performance

Full-month runs are I/O and CPU heavy. The pipeline does the following to improve throughput:

- **Bulk writes**: Stats are flushed in batches (default 500k keys) using multi-row `INSERT ... ON CONFLICT` in chunks (SQLite: 100 rows/statement; Postgres: 500 rows/statement) instead of one insert per row.
- **SQLite**: WAL mode, `synchronous=NORMAL`, and a 256 MB cache during ingest to speed up bulk load.
- **PostgreSQL**: Often faster for large ingest due to better concurrent write and indexing; use `database_url` in config.

To get higher games/second:

- Use an **SSD** for both the PGN files and the database.
- **Narrow the dataset**: fewer `time_controls`, smaller `rating_min`–`rating_max`, or a single `months` slice to reduce volume.
- Run **sample mode** first (`--sample 50000`) to estimate time on your machine; throughput depends on CPU (parsing/replay) and disk (decompression, DB writes).

## Design notes

- **Rating**: Uses the **rating of the side to move** (WhiteElo when White to move, BlackElo when Black to move), mapped to a band (e.g. `floor(rating/100)*100`). Buckets aggregate bands (e.g. 1600–1800 → 1600, 1700, 1800).
- **Games with missing/invalid Elo**: Skipped entirely.
- **Time control**: Filter by `Event` tag (configurable substrings). Use the same Lichess tag values you care about (e.g. `"Rated Blitz game"`).
- **Re-processing**: Use the manifest; do not re-process the same month without `--force` to avoid double-counting.
