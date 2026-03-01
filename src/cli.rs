use crate::config::Config;
use anyhow::Result;
use std::path::PathBuf;

#[derive(clap::Subcommand)]
pub enum Commands {
    /// Download (optional) and process PGN files into the FEN move stats DB
    Run(RunArgs),
    /// Query move stats for a FEN and rating bucket (prints JSON to stdout)
    Query(QueryArgs),
    /// Run HTTP server for query API (for chess-prep app integration)
    Serve(ServeArgs),
}

#[derive(clap::Args)]
pub struct RunArgs {
    /// Path to config file (YAML)
    #[arg(long, short, default_value = "config.yaml")]
    pub config: PathBuf,

    /// Only process months from this date (YYYY-MM). Overrides config.
    #[arg(long)]
    pub since: Option<String>,

    /// Process only months not yet in the manifest (default when manifest exists)
    #[arg(long)]
    pub incremental: bool,

    /// Re-process even if month is already in manifest (can cause double-counting)
    #[arg(long)]
    pub force: bool,

    /// Test/sample mode: process at most N games from the most recent month only.
    /// Does not mark the month as processed. Prints timing and extrapolated duration for a full month.
    #[arg(long)]
    pub sample: Option<u64>,
}

#[derive(clap::Args)]
pub struct QueryArgs {
    /// Path to SQLite database, or PostgreSQL URL (e.g. postgresql://user:pass@localhost:5432/db)
    #[arg(long, short)]
    pub db: String,

    /// FEN position (6-field, normalized)
    #[arg(long, short)]
    pub fen: String,

    /// Rating bucket e.g. "1600-1800" (grid-aligned to band width)
    #[arg(long, short)]
    pub bucket: String,

    /// Band width used when building the DB (default 100)
    #[arg(long, default_value = "100")]
    pub band_width: u32,
}

#[derive(clap::Args)]
pub struct ServeArgs {
    /// Path to SQLite database, or PostgreSQL URL (e.g. postgresql://user:pass@localhost:5432/db)
    #[arg(long, short)]
    pub db: String,

    /// Bind address
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub bind: String,

    /// Band width used when building the DB (default 100)
    #[arg(long, default_value = "100")]
    pub band_width: u32,
}

pub async fn run(args: RunArgs) -> Result<()> {
    let config = Config::load(&args.config)?;
    crate::ingest::run_pipeline(&config, &args).await
}

pub async fn query(args: QueryArgs) -> Result<()> {
    let moves = if args.db.contains("://") {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&args.db)
            .await?;
        crate::store_postgres::query_moves(&pool, &args.fen, &args.bucket, args.band_width).await?
    } else {
        crate::store::query_moves(std::path::Path::new(&args.db), &args.fen, &args.bucket, args.band_width)?
    };
    println!("{}", serde_json::to_string_pretty(&moves)?);
    Ok(())
}

pub async fn serve(args: ServeArgs) -> Result<()> {
    crate::server::serve(&args.db, &args.bind, args.band_width).await
}
