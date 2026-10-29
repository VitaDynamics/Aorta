use anyhow::Result;
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

pub async fn shm(stale_timeout: u64) -> Result<()> {
    let candidates = ["/dev/shm", "/run/shm", "/var/run/shm"];
    let stale = Duration::from_secs(stale_timeout);
    let now = SystemTime::now();

    let mut cleaned = 0usize;
    let mut scanned = 0usize;
    let mut found_root = false;

    for root in candidates {
        let root_path = Path::new(root);
        if !root_path.exists() {
            continue;
        }
        found_root = true;
        let entries = match fs::read_dir(root_path) {
            Ok(entries) => entries,
            Err(err) => {
                eprintln!("WARN: Failed to scan '{}': {}", root, err);
                continue;
            }
        };

        for entry in entries.flatten() {
            scanned += 1;
            let path = entry.path();
            let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if !file_name.contains("zenoh") {
                continue;
            }

            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            let modified = metadata.modified().unwrap_or(now);
            if now.duration_since(modified).unwrap_or_default() < stale {
                continue;
            }

            match fs::remove_file(&path) {
                Ok(_) => {
                    cleaned += 1;
                    println!("Removed stale SHM file: {}", path.display());
                }
                Err(err) => {
                    eprintln!("WARN: Failed to remove '{}': {}", path.display(), err);
                }
            }
        }
    }

    if !found_root {
        println!("No SHM directory found on this platform. Nothing to clean.");
        return Ok(());
    }

    println!(
        "Scanned {} entries, removed {} stale files.",
        scanned, cleaned
    );
    Ok(())
}
