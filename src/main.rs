mod cli;
mod config;
mod download;
mod ingest;
mod manifest;
mod pgn;
mod server;
mod store;
mod store_postgres;

use anyhow::Result;
use clap::Parser;
use cli::Commands;

#[derive(Parser)]
#[command(name = "lichess-fen-service")]
#[command(about = "Local FEN → move frequency store from Lichess database")]
struct App {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<()> {
    let app = App::parse();
    match app.command {
        Commands::Run(args) => cli::run(args).await,
        Commands::Query(args) => cli::query(args).await,
        Commands::Serve(args) => cli::serve(args).await,
    }
}
