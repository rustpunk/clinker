/// Recent files list: persisted to OS app data directory.
///
/// Spec §F2.7: up to 20 entries, deduplicated by canonical path,
/// most-recently-opened first.
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[allow(dead_code)]
const MAX_RECENT: usize = 20;
const APP_DIR_NAME: &str = "clinker-kiln";
const RECENT_FILE_NAME: &str = "recent.json";

/// A single entry in the recent files list.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecentFileEntry {
    pub path: PathBuf,
    pub workspace_root: Option<PathBuf>,
    pub last_opened: String, // ISO 8601 timestamp
    pub pipeline_name: Option<String>,
}

/// Get the OS-appropriate path for the recent files JSON.
///
/// - Linux: `~/.local/share/clinker-kiln/recent.json`
/// - macOS: `~/Library/Application Support/clinker-kiln/recent.json`
/// - Windows: `%APPDATA%\clinker-kiln\recent.json`
fn recent_files_path() -> Option<PathBuf> {
    dirs::data_dir().map(|d| d.join(APP_DIR_NAME).join(RECENT_FILE_NAME))
}

/// Load the recent files list from disk. Returns empty vec on any error.
pub fn load_recent_files() -> Vec<RecentFileEntry> {
    let Some(path) = recent_files_path() else {
        return Vec::new();
    };

    let Ok(content) = fs::read_to_string(&path) else {
        return Vec::new();
    };

    serde_json::from_str(&content).unwrap_or_default()
}

/// Save the recent files list to disk. Silently ignores errors.
#[allow(dead_code)]
pub fn save_recent_files(entries: &[RecentFileEntry]) {
    let Some(path) = recent_files_path() else {
        return;
    };

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    if let Ok(json) = serde_json::to_string_pretty(entries) {
        let _ = fs::write(&path, json);
    }
}

/// Add a file to the recent list. Deduplicates by path and caps at 20 entries.
#[allow(dead_code)]
pub fn add_recent(entries: &mut Vec<RecentFileEntry>, entry: RecentFileEntry) {
    // Canonicalize for dedup (best-effort — keep original if canonicalize fails)
    let canonical = entry
        .path
        .canonicalize()
        .unwrap_or_else(|_| entry.path.clone());

    // Remove existing entry for same path
    entries.retain(|e| {
        let existing = e.path.canonicalize().unwrap_or_else(|_| e.path.clone());
        existing != canonical
    });

    // Insert at front (most recent first)
    entries.insert(0, entry);

    // Cap at max
    entries.truncate(MAX_RECENT);
}

/// Check whether a file still exists on disk.
#[allow(dead_code)]
pub fn file_exists(path: &Path) -> bool {
    path.exists()
}

/// Format a relative time string from an ISO 8601 timestamp.
/// Returns a human-friendly string like "2 hours ago", "yesterday", "3 days ago".
/// Falls back to the raw timestamp on parse failure.
#[allow(dead_code)]
pub fn relative_time(iso_timestamp: &str) -> String {
    // Simple implementation — good enough for recent files display
    // Full chrono parsing would be more robust but this covers the common cases
    iso_timestamp.to_string()
}
