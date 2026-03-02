// PostgreSQL store and manifest (when database_url is set)
use anyhow::Result;
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::store::{bucket_to_bands, MoveStat, QueryResult};

pub async fn ensure_schema(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS fen_move_stats (
            fen TEXT NOT NULL,
            rating_band INTEGER NOT NULL,
            move TEXT NOT NULL,
            games BIGINT NOT NULL,
            wins BIGINT NOT NULL,
            draws BIGINT NOT NULL,
            PRIMARY KEY (fen, rating_band, move)
        )
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_fen_rating ON fen_move_stats(fen, rating_band)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Append-only staging table for Phase 1 ingest.
pub async fn ensure_staging_table(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS fen_move_staging (
            fen TEXT NOT NULL,
            rating_band INTEGER NOT NULL,
            move TEXT NOT NULL,
            games BIGINT NOT NULL,
            wins BIGINT NOT NULL,
            draws BIGINT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Chunk size for multi-row INSERT.
const PG_CHUNK: usize = 500;

/// Append-only insert into staging. Call merge_staging_into_fen_move_stats after.
pub async fn insert_batch_staging(
    pool: &PgPool,
    rows: &[(String, u32, String, u64, u64, u64)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    for chunk in rows.chunks(PG_CHUNK) {
        let n = chunk.len();
        let mut placeholders = Vec::with_capacity(n);
        let mut param = 1i32;
        for _ in 0..n {
            placeholders.push(format!(
                "(${},{},{},{},{},{})",
                param, param + 1, param + 2, param + 3, param + 4, param + 5
            ));
            param += 6;
        }
        let sql = format!(
            "INSERT INTO fen_move_staging (fen, rating_band, move, games, wins, draws) VALUES {}",
            placeholders.join(", ")
        );
        let mut query = sqlx::query(&sql);
        for (fen, band, move_, games, wins, draws) in chunk {
            query = query
                .bind(fen)
                .bind(*band as i32)
                .bind(move_)
                .bind(*games as i64)
                .bind(*wins as i64)
                .bind(*draws as i64);
        }
        query.execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Merge staging into fen_move_stats, then truncate staging.
pub async fn merge_staging_into_fen_move_stats(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO fen_move_stats (fen, rating_band, move, games, wins, draws)
        SELECT fen, rating_band, move, SUM(games), SUM(wins), SUM(draws)
        FROM fen_move_staging
        GROUP BY fen, rating_band, move
        ON CONFLICT (fen, rating_band, move) DO UPDATE SET
            games = fen_move_stats.games + excluded.games,
            wins = fen_move_stats.wins + excluded.wins,
            draws = fen_move_stats.draws + excluded.draws
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query("TRUNCATE fen_move_staging").execute(pool).await?;
    Ok(())
}

pub async fn ensure_manifest_table(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS processed_months (month TEXT PRIMARY KEY)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn processed_months(pool: &PgPool) -> Result<Vec<String>> {
    let rows = sqlx::query_scalar::<sqlx::Postgres, String>("SELECT month FROM processed_months")
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn mark_processed(pool: &PgPool, month: &str) -> Result<()> {
    sqlx::query("INSERT INTO processed_months (month) VALUES ($1) ON CONFLICT (month) DO NOTHING")
        .bind(month)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn query_moves(
    pool: &PgPool,
    fen: &str,
    bucket: &str,
    band_width: u32,
) -> Result<QueryResult> {
    let bands = bucket_to_bands(bucket, band_width)?;
    if bands.is_empty() {
        return Ok(QueryResult { moves: vec![] });
    }
    // Build IN list: $2, $3, $4, ...
    let placeholders: Vec<String> = (2..=bands.len() + 1).map(|i| format!("${i}")).collect();
    let in_clause = placeholders.join(", ");
    let sql = format!(
        "SELECT move, SUM(games)::BIGINT AS games, SUM(wins)::BIGINT AS wins, SUM(draws)::BIGINT AS draws
         FROM fen_move_stats
         WHERE fen = $1 AND rating_band IN ({})
         GROUP BY move
         ORDER BY games DESC",
        in_clause
    );
    let mut query = sqlx::query(&sql).bind(fen);
    for b in &bands {
        query = query.bind(*b as i32);
    }
    let rows = query.fetch_all(pool).await?;
    let moves: Vec<MoveStat> = rows
        .into_iter()
        .map(|row| {
            let move_: String = row.get(0);
            let games: i64 = row.get(1);
            let wins: i64 = row.get(2);
            let draws: i64 = row.get(3);
            let games = games as u64;
            let winrate = if games > 0 {
                (wins as f64 + 0.5 * draws as f64) / games as f64
            } else {
                0.0
            };
            MoveStat {
                move_uci: move_,
                games,
                winrate,
            }
        })
        .collect();
    Ok(QueryResult { moves })
}
