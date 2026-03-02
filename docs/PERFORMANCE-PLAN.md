# Performance plan: full month in “some hours”

**Goal:** Get a full Lichess month (~90M games) down to a few hours instead of ~17h, so running the pipeline is not a blocker.

**Current state (after deferred index + bulk-load):** ~34s for 50k games → ~1,475 games/s → ~17h extrapolated. Bottleneck: **~83% DB writes**, ~17% parse+decompress.

---

## 1. Staging table (append-only ingest, then merge) — **implemented**

**Implementation:** SQLite and Postgres use `fen_move_staging`; each flush (every 1M keys) does `insert_batch_staging` then `merge_staging_into_fen_move_stats` (see `store.rs`, `store_postgres.rs`, `ingest.rs`).

**Idea:** Stop upserting into `fen_move_stats` during parse. Instead:

- **During parse:** Insert rows into a **staging table** (append-only, no unique constraint, no index). Same batch size (e.g. 100 rows per INSERT), but plain `INSERT` only — no `ON CONFLICT`, so no lookups.
- **When staging is large enough (e.g. every 1–2M rows) or at end of file:** Run a single merge step:
  - `INSERT INTO fen_move_stats (fen, rating_band, move, games, wins, draws)  
     SELECT fen, rating_band, move, SUM(games), SUM(wins), SUM(draws)  
     FROM fen_move_staging GROUP BY fen, rating_band, move  
     ON CONFLICT(fen, rating_band, move) DO UPDATE SET games=games+excluded.games, ...`
  - Then `DELETE FROM fen_move_staging` (or truncate).

**Why it helps:** Appends are much cheaper than upserts (no PK lookup + update). We trade many small upserts for many appends + fewer big merges. SQLite is very good at bulk INSERT and at GROUP BY over a temp table; the merge can be done in one or a few transactions per file.

**Risks:** Staging table size (same row count as current flush, so disk/memory is similar). Merge step must be tuned (batch size / when to run) so it doesn’t dominate.

**Target:** Aim for a clear drop in “DB write” time in the time breakdown (e.g. from ~83% toward ~30–50%), and full-month extrapolation down toward single-digit hours.

---

## 2. Process multiple months in parallel — **for multi-month runs**

**Idea:** When you have several months to process (e.g. “since 2024-01”):

- Run **N workers** (e.g. N = 2–4 or `config.concurrency`), each processing **one month** into its **own** SQLite file (e.g. `fen_move_2024_01.db`, `fen_move_2024_02.db`, …).
- After all months are done, **merge** into the main DB: for each per-month file, run  
  `INSERT INTO main.fen_move_stats SELECT ... FROM attached_db.fen_move_stats ON CONFLICT DO UPDATE ...`.

**Why it helps:** Parse + per-month DB work is parallelized; only the merge is single-threaded. With 4 months and 4 workers you get up to ~4× shorter wall-clock for the heavy part.

**Caveats:** More disk (one DB per month until merge). Merge step is I/O bound but one-time. Best when you have many months and spare cores.

---

## 3. Put DB on RAM disk during ingest (optional)

**Idea:** During ingest, write to a DB on a **tmpfs / RAM disk**, then copy the final DB to disk when done.

**Why it helps:** Removes disk as bottleneck for the writer; can give a large speedup if the current bottleneck is disk I/O.

**Caveats:** Need enough RAM (or a large tmpfs). After copy-back, you have a normal on-disk DB for serve/query.

---

## 4. Within-file parallelism (later, only if parse dominates)

**Idea:** After Phase 1–2, if “parse+decompress” becomes the main cost:

- One thread: decompress `.pgn.zst` and split the stream by **game boundaries** into N chunks (e.g. by byte ranges + seek to next `[Event`).
- N worker threads: each parses one chunk, emits (fen, band, move, stats) into a channel.
- One writer (or staging inserter): consumes channel and does staging inserts (or merge).

**Why it helps:** Uses multiple cores for parsing. Only worth it if parse time is a large fraction of total.

---

## Recommended order

| Phase | What | Goal |
|-------|------|------|
| **1** | Staging table: append-only ingest + periodic merge into `fen_move_stats` | Big drop in DB write time; target full month in single-digit hours |
| **2** | Multi-month parallelism: one DB per month, then merge into main | Shorter wall-clock when processing many months |
| **3** | (Optional) RAM disk for `output_db` during ingest | Extra speed if disk is still limiting |
| **4** | (Only if needed) Within-file parse parallelism | If parse becomes dominant after 1–2 |

Implementing **Phase 1** first is the main architectural change that can get a full month into “some hours” without adding multiple processes or merge logic; we can then measure and decide on Phase 2–4.
