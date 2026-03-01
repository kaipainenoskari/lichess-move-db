// Download list.txt and .pgn.zst files to data_dir
use anyhow::Result;
use futures::stream::StreamExt;
use std::path::Path;
use tokio::io::AsyncWriteExt;

pub async fn fetch_list(url: &str) -> Result<Vec<String>> {
    eprintln!("Fetching list of PGN files...");
    let body = reqwest::get(url).await?.text().await?;
    let urls: Vec<String> = body
        .lines()
        .map(str::trim)
        .filter(|s| s.starts_with("http") && s.ends_with(".pgn.zst"))
        .map(String::from)
        .collect();
    eprintln!("  Found {} monthly files.", urls.len());
    Ok(urls)
}

/// Filter URLs by month: only those >= since_ym (YYYY-MM), or all if None.
pub fn filter_by_since(urls: &[String], since_ym: Option<&str>) -> Vec<String> {
    let Some(since) = since_ym else {
        return urls.to_vec();
    };
    urls.iter()
        .filter(|u| {
            let month = u
                .trim_end_matches(".pgn.zst")
                .rsplit('_')
                .next()
                .unwrap_or("");
            month >= since
        })
        .cloned()
        .collect()
}

/// Extract month (YYYY-MM) from URL like .../lichess_db_standard_rated_2025-01.pgn.zst
pub fn month_from_url(url: &str) -> Option<String> {
    let name = url.rsplit('/').next()?;
    let month = name.trim_end_matches(".pgn.zst").rsplit('_').next()?;
    if month.len() == 7 && month.as_bytes().get(4) == Some(&b'-') {
        Some(month.to_string())
    } else {
        None
    }
}

/// Download a single file to data_dir if not already present. Streams to disk (no full buffering).
pub async fn download_file(url: &str, data_dir: &Path) -> Result<std::path::PathBuf> {
    let filename = url.split('/').next_back().unwrap_or("unknown.pgn.zst");
    let path = data_dir.join(filename);
    if path.exists() {
        eprintln!("  {} already present, skipping download.", filename);
        return Ok(path);
    }
    std::fs::create_dir_all(data_dir)?;
    eprintln!("  Downloading {} (streaming to disk; large files take a long time)...", filename);
    let resp = reqwest::get(url).await?;
    let mut stream = resp.bytes_stream();
    let mut file = tokio::fs::File::create(&path).await?;
    let mut written: u64 = 0;
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        let len = chunk.len() as u64;
        written += len;
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
        // Progress every ~100 MB
        if written % (100 * 1024 * 1024) < len {
            eprintln!("    ... {} MB written", written / (1024 * 1024));
        }
    }
    file.flush().await?;
    eprintln!("  Done. {} MB total.", written / (1024 * 1024));
    Ok(path)
}

/// Get list of URLs, filter by since/manifest, download missing, return local paths to process.
/// If max_downloads is Some(n), only download the first n files (e.g. 1 for sample mode).
pub async fn ensure_downloaded(
    list_url: &str,
    data_dir: &Path,
    since_ym: Option<&str>,
    skip_months: &[String],
    max_downloads: Option<usize>,
) -> Result<Vec<std::path::PathBuf>> {
    let urls = fetch_list(list_url).await?;
    let urls = filter_by_since(&urls, since_ym);
    let mut paths = Vec::new();
    for u in &urls {
        if max_downloads.map_or(false, |max| paths.len() >= max) {
            break;
        }
        let month = month_from_url(u).unwrap_or_default();
        if skip_months.iter().any(|m| m == &month) {
            continue;
        }
        let path = download_file(u, data_dir).await?;
        paths.push(path);
        if max_downloads == Some(1) {
            break;
        }
    }
    Ok(paths)
}
