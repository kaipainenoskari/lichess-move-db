// SQLite schema, upsert, query by (fen, bucket)
use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;

#[derive(serde::Serialize)]
pub struct MoveStat {
    #[serde(rename = "move")]
    pub move_uci: String,
    pub games: u64,
    pub winrate: f64,
}

#[derive(serde::Serialize)]
pub struct QueryResult {
    pub moves: Vec<MoveStat>,
}

fn open(db_path: &Path) -> Result<Connection> {
    open_connection(db_path)
}

pub fn open_connection(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-256000; PRAGMA temp_store=MEMORY;")?;
    ensure_fen_rating_index(&conn)?;
    Ok(conn)
}

/// Opens a connection tuned for bulk ingest (faster writes, no index until finalize).
/// Use only for the ingest pipeline; for query/serve use open_connection.
pub fn open_connection_for_bulk_load(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; PRAGMA cache_size=-256000; PRAGMA temp_store=MEMORY;")?;
    Ok(conn)
}

/// Creates the fen+rating index (call after bulk load so writes are faster).
pub fn ensure_fen_rating_index(conn: &Connection) -> Result<()> {
    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_fen_rating ON fen_move_stats(fen, rating_band);")?;
    Ok(())
}

/// Drops the fen+rating index so bulk ingest doesn't pay index update cost. Call at start of ingest.
pub fn drop_fen_rating_index_if_exists(conn: &Connection) -> Result<()> {
    conn.execute_batch("DROP INDEX IF EXISTS idx_fen_rating;")?;
    Ok(())
}

pub fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS fen_move_stats (
            fen TEXT NOT NULL,
            rating_band INTEGER NOT NULL,
            move TEXT NOT NULL,
            games INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            draws INTEGER NOT NULL,
            PRIMARY KEY (fen, rating_band, move)
        );
        "#,
    )?;
    Ok(())
}

/// Append-only staging table for Phase 1 ingest (no PK, no index).
pub fn ensure_staging_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS fen_move_staging (
            fen TEXT NOT NULL,
            rating_band INTEGER NOT NULL,
            move TEXT NOT NULL,
            games INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            draws INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

/// Append-only insert into staging (no ON CONFLICT). Call merge_staging_into_fen_move_stats after.
pub fn insert_batch_staging(conn: &Connection, rows: &[(String, u32, String, u64, u64, u64)]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    for chunk in rows.chunks(SQLITE_CHUNK) {
        let n = chunk.len();
        let placeholders: Vec<String> = (0..n)
            .map(|i| {
                let b = i * 6;
                format!("(?{},?{},?{},?{},?{},?{})", b + 1, b + 2, b + 3, b + 4, b + 5, b + 6)
            })
            .collect();
        let sql = format!(
            "INSERT INTO fen_move_staging (fen, rating_band, move, games, wins, draws) VALUES {}",
            placeholders.join(", ")
        );
        let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(n * 6);
        for (fen, band, move_, games, wins, draws) in chunk {
            params.push(rusqlite::types::Value::Text(fen.clone()));
            params.push(rusqlite::types::Value::Integer(*band as i64));
            params.push(rusqlite::types::Value::Text(move_.clone()));
            params.push(rusqlite::types::Value::Integer(*games as i64));
            params.push(rusqlite::types::Value::Integer(*wins as i64));
            params.push(rusqlite::types::Value::Integer(*draws as i64));
        }
        tx.execute(&sql, rusqlite::params_from_iter(params))?;
    }
    tx.commit()?;
    Ok(())
}

/// Merge staging into fen_move_stats (GROUP BY + ON CONFLICT), then clear staging.
pub fn merge_staging_into_fen_move_stats(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        INSERT INTO fen_move_stats (fen, rating_band, move, games, wins, draws)
        SELECT fen, rating_band, move, SUM(games), SUM(wins), SUM(draws)
        FROM fen_move_staging
        GROUP BY fen, rating_band, move
        ON CONFLICT(fen, rating_band, move) DO UPDATE SET
            games = fen_move_stats.games + excluded.games,
            wins = fen_move_stats.wins + excluded.wins,
            draws = fen_move_stats.draws + excluded.draws;
        DELETE FROM fen_move_staging;
        "#,
    )?;
    Ok(())
}

/// SQLite default limit is 999 bound parameters per statement; 100 rows × 6 cols = 600.
const SQLITE_CHUNK: usize = 100;

/// Resolve bucket "1600-1800" to bands [1600, 1700, 1800] (grid-aligned).
pub fn bucket_to_bands(bucket: &str, band_width: u32) -> Result<Vec<u32>> {
    let (low, high) = parse_bucket(bucket)?;
    let low = (low / band_width) * band_width;
    let high = (high / band_width) * band_width;
    let mut bands = Vec::new();
    let mut b = low;
    while b <= high {
        bands.push(b);
        b += band_width;
    }
    Ok(bands)
}

fn parse_bucket(s: &str) -> Result<(u32, u32)> {
    let s = s.trim();
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 2 {
        anyhow::bail!("bucket must be like 1600-1800");
    }
    let low: u32 = parts[0].trim().parse()?;
    let high: u32 = parts[1].trim().parse()?;
    if low > high {
        anyhow::bail!("bucket low must be <= high");
    }
    Ok((low, high))
}

pub fn query_moves(
    db_path: &Path,
    fen: &str,
    bucket: &str,
    band_width: u32,
) -> Result<QueryResult> {
    let conn = open(db_path)?;
    let bands = bucket_to_bands(bucket, band_width)?;
    if bands.is_empty() {
        return Ok(QueryResult { moves: vec![] });
    }
    let placeholders = (2..2 + bands.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT move, SUM(games) AS games, SUM(wins) AS wins, SUM(draws) AS draws
         FROM fen_move_stats
         WHERE fen = ?1 AND rating_band IN ({})
         GROUP BY move
         ORDER BY games DESC",
        placeholders
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut params = vec![rusqlite::types::Value::Text(fen.to_string())];
    for b in &bands {
        params.push(rusqlite::types::Value::Integer(*b as i64));
    }
    let mut rows = stmt.query(rusqlite::params_from_iter(params))?;
    let mut moves = Vec::new();
    loop {
        let row = match rows.next()? {
            Some(r) => r,
            None => break,
        };
        let move_: String = row.get(0)?;
        let games: i64 = row.get(1)?;
        let wins: i64 = row.get(2)?;
        let draws: i64 = row.get(3)?;
        let games = games as u64;
        let winrate = if games > 0 {
            (wins as f64 + 0.5 * draws as f64) / games as f64
        } else {
            0.0
        };
        moves.push(MoveStat {
            move_uci: move_,
            games,
            winrate,
        });
    }
    Ok(QueryResult { moves })
}
