//! Filesystem watcher for auto-refreshing git status and schema index.
//!
//! Watches the workspace root for file changes and triggers git status
//! recomputation after a 500ms debounce. Also refreshes the schema index
//! when .schema.yaml files change.
//! Spec: clinker-kiln-git-addendum.md §G2.4.

use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};

/// Start a filesystem watcher on the given directory.
///
/// Returns a receiver that emits debounced change events. The watcher
/// runs on a background thread. Drop the watcher to stop it.
pub fn start_watcher(root: &Path) -> Option<(RecommendedWatcher, mpsc::Receiver<Vec<PathBuf>>)> {
    let (raw_tx, raw_rx) = mpsc::channel::<Vec<PathBuf>>();
    let root = root.to_path_buf();

    let watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        if let Ok(event) = res {
            let paths: Vec<PathBuf> = event.paths;
            if !paths.is_empty() {
                let _ = raw_tx.send(paths);
            }
        }
    });

    let mut watcher = watcher.ok()?;
    watcher.watch(&root, RecursiveMode::Recursive).ok()?;

    // Spawn a debounce thread that collects events for 500ms
    let (debounced_tx, debounced_rx) = mpsc::channel::<Vec<PathBuf>>();

    std::thread::spawn(move || {
        let mut pending = Vec::new();
        loop {
            match raw_rx.recv_timeout(Duration::from_millis(500)) {
                Ok(paths) => {
                    pending.extend(paths);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if !pending.is_empty() {
                        let batch = std::mem::take(&mut pending);
                        if debounced_tx.send(batch).is_err() {
                            break; // Receiver dropped
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    });

    Some((watcher, debounced_rx))
}

/// Check if any of the changed paths are relevant for git status refresh.
pub fn has_git_relevant_changes(paths: &[PathBuf]) -> bool {
    paths.iter().any(|p| {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        // Skip .git internal files (too noisy), IDE state files
        !p.components().any(|c| c.as_os_str() == ".git") && name != ".kiln-state.json"
    })
}

/// Check if any of the changed paths are schema files.
#[allow(dead_code)]
pub fn has_schema_changes(paths: &[PathBuf]) -> bool {
    paths.iter().any(|p| {
        p.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|name| name.ends_with(".schema.yaml") || name.ends_with(".schema.yml"))
    })
}
