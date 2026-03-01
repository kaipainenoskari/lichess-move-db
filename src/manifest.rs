// Processed months manifest (skip already-processed)
use anyhow::Result;
use rusqlite::Connection;

pub fn ensure_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS processed_months (month TEXT PRIMARY KEY);",
    )?;
    Ok(())
}

pub fn processed_months(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT month FROM processed_months")?;
    let rows = stmt.query_map([], |r| r.get(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn mark_processed(conn: &Connection, month: &str) -> Result<()> {
    conn.execute("INSERT OR IGNORE INTO processed_months (month) VALUES (?1)", [month])?;
    Ok(())
}
