// Ingest: run pipeline (download -> process -> store)
use crate::config::Config;
use crate::download::{self, month_from_url};
use crate::cli::RunArgs;
use crate::manifest;
use crate::pgn::{self, MoveRecord};
use crate::store;
use crate::store_postgres;
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

const FLUSH_THRESHOLD: usize = 500_000;
/// Typical games per Lichess standard month (for extrapolation in sample mode).
const TYPICAL_GAMES_PER_MONTH: u64 = 90_000_000;

enum DbBackend {
    Sqlite(rusqlite::Connection),
    Postgres(sqlx::PgPool),
}

pub async fn run_pipeline(config: &Config, args: &RunArgs) -> Result<()> {
    eprintln!("Starting pipeline...");

    let (backend, skip_months) = if let Some(ref url) = config.database_url {
        eprintln!("Using PostgreSQL.");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(url)
            .await?;
        store_postgres::ensure_schema(&pool).await?;
        store_postgres::ensure_manifest_table(&pool).await?;
        let skip = if args.force {
            vec![]
        } else {
            store_postgres::processed_months(&pool).await?
        };
        (DbBackend::Postgres(pool), skip)
    } else {
        std::fs::create_dir_all(config.output_db.parent().unwrap_or(Path::new(".")))?;
        let conn = store::open_connection(&config.output_db)?;
        store::ensure_schema(&conn)?;
        manifest::ensure_table(&conn)?;
        let skip = if args.force {
            vec![]
        } else {
            manifest::processed_months(&conn)?
        };
        (DbBackend::Sqlite(conn), skip)
    };

    let since_ym = args.since.as_deref().or_else(|| {
        config.months.as_deref().and_then(|m| {
            let m = m.trim();
            if m.eq_ignore_ascii_case("all") {
                None
            } else if m.starts_with("since ") {
                Some(m.trim_start_matches("since ").trim())
            } else {
                Some(m)
            }
        })
    });

    let sample_mode = args.sample.is_some();

    let mut paths = if config.download {
        let list_url = config.lichess_list_url.as_str();
        let skip = if sample_mode { &[][..] } else { skip_months.as_slice() };
        let max_downloads = if sample_mode { Some(1) } else { None };
        download::ensure_downloaded(list_url, &config.data_dir, since_ym, skip, max_downloads).await?
    } else {
        let urls = download::fetch_list(&config.lichess_list_url).await?;
        let urls = download::filter_by_since(&urls, since_ym);
        let mut paths = Vec::new();
        for u in &urls {
            let month = month_from_url(u).unwrap_or_default();
            if !sample_mode && skip_months.iter().any(|m| m == &month) && !args.force {
                continue;
            }
            let name = u.split('/').next_back().unwrap_or("unknown.pgn.zst");
            let path = config.data_dir.join(name);
            if path.exists() {
                paths.push(path);
            }
        }
        paths
    };

    if sample_mode {
        paths.sort_by(|a, b| b.cmp(a));
        paths.truncate(1);
        if let Some(p) = paths.first() {
            eprintln!(
                "Sample mode: processing at most {} games from most recent month ({})",
                args.sample.unwrap(),
                p.display()
            );
        }
    } else {
        paths.sort();
    }

    if paths.is_empty() {
        eprintln!("No PGN files to process.");
        return Ok(());
    }

    for path in paths {
        let month = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.strip_suffix(".pgn"))
            .and_then(|s| s.rsplit('_').next())
            .map(String::from)
            .unwrap_or_default();

        if !sample_mode && skip_months.contains(&month) && !args.force {
            continue;
        }

        eprintln!("Processing {} ...", path.display());
        let start = Instant::now();
        let mut agg: HashMap<(String, u32, String), (u64, u64, u64)> = HashMap::new();

        let games_processed = {
            let mut on_record = |record: MoveRecord| {
                let key = (
                    record.fen,
                    record.rating_band,
                    record.move_uci,
                );
                let e = agg.entry(key).or_insert((0, 0, 0));
                e.0 += record.games;
                e.1 += record.wins;
                e.2 += record.draws;

                if agg.len() >= FLUSH_THRESHOLD {
                    flush_agg(&backend, &mut agg);
                }
            };
            pgn::process_file(
                &path,
                &config.time_controls,
                config.rating_min,
                config.rating_max,
                config.rating_band_width,
                args.sample,
                &mut on_record,
            )?
        };

        flush_agg(&backend, &mut agg);
        let elapsed = start.elapsed();

        if sample_mode {
            eprintln!("  Processed {} games in {:.1?}", games_processed, elapsed);
            if games_processed > 0 && elapsed.as_secs_f64() > 0.0 {
                let games_per_sec = games_processed as f64 / elapsed.as_secs_f64();
                let extrapolated_secs = TYPICAL_GAMES_PER_MONTH as f64 / games_per_sec;
                let extrapolated_mins = extrapolated_secs / 60.0;
                let extrapolated_hrs = extrapolated_mins / 60.0;
                eprintln!(
                    "  At this rate, a full month (~{}M games) would take approximately {:.0} minutes ({:.1} hours).",
                    TYPICAL_GAMES_PER_MONTH / 1_000_000,
                    extrapolated_mins,
                    extrapolated_hrs
                );
            }
            eprintln!("  (Month not marked as processed; run without --sample to process fully.)");
        } else {
            mark_processed(&backend, &month).await?;
            eprintln!("  Done. Processed {} games. Marked {} as processed.", games_processed, month);
        }
    }

    Ok(())
}

fn flush_agg(
    backend: &DbBackend,
    agg: &mut HashMap<(String, u32, String), (u64, u64, u64)>,
) {
    if agg.is_empty() {
        return;
    }
    let rows: Vec<_> = agg
        .drain()
        .map(|((fen, band, move_uci), (games, wins, draws))| (fen, band, move_uci, games, wins, draws))
        .collect();
    match backend {
        DbBackend::Sqlite(conn) => {
            if let Err(e) = store::upsert_batch(conn, &rows) {
                eprintln!("Warning: flush failed: {}", e);
            }
        }
        DbBackend::Postgres(pool) => {
            let pool = pool.clone();
            let rows = rows;
            if let Err(e) = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(store_postgres::upsert_batch(&pool, &rows))
            }) {
                eprintln!("Warning: flush failed: {}", e);
            }
        }
    }
}

async fn mark_processed(backend: &DbBackend, month: &str) -> Result<()> {
    match backend {
        DbBackend::Sqlite(conn) => manifest::mark_processed(conn, month),
        DbBackend::Postgres(pool) => store_postgres::mark_processed(pool, month).await,
    }
}
