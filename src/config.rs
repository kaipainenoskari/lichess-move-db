use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_lichess_list_url")]
    pub lichess_list_url: String,

    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    #[serde(default = "default_output_db")]
    pub output_db: PathBuf,

    /// PostgreSQL connection URL. When set, use PostgreSQL instead of SQLite (output_db is ignored for run).
    /// Example: postgresql://user:password@localhost:5432/fen_move
    pub database_url: Option<String>,

    #[serde(default = "default_rating_band_width")]
    pub rating_band_width: u32,

    #[serde(default = "default_rating_min")]
    pub rating_min: u32,

    #[serde(default = "default_rating_max")]
    pub rating_max: u32,

    #[serde(default = "default_time_controls")]
    pub time_controls: Vec<String>,

    /// "all" | "since YYYY-MM" | unused if CLI provides --since or only unprocessed
    pub months: Option<String>,

    #[serde(default = "default_true")]
    pub download: bool,

    #[serde(default = "default_concurrency")]
    #[allow(dead_code)] // reserved for future parallel file processing
    pub concurrency: usize,
}

fn default_lichess_list_url() -> String {
    "https://database.lichess.org/standard/list.txt".to_string()
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data/pgn")
}

fn default_output_db() -> PathBuf {
    PathBuf::from("./data/fen_move.db")
}

fn default_rating_band_width() -> u32 {
    100
}

fn default_rating_min() -> u32 {
    1200
}

fn default_rating_max() -> u32 {
    2500
}

fn default_time_controls() -> Vec<String> {
    vec![
        "Rated Blitz game".to_string(),
        "Rated Rapid game".to_string(),
    ]
}

fn default_true() -> bool {
    true
}

fn default_concurrency() -> usize {
    1
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        if !path.exists() {
            anyhow::bail!(
                "Config file not found: {}\n  Create it e.g. by copying config.example.yaml to config.yaml",
                path.display()
            );
        }
        let s = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&s)?;
        Ok(config)
    }
}
