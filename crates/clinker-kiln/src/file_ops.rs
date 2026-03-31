/// File I/O operations: open/save dialogs, read/write pipeline YAML.
///
/// Uses `rfd` for native file pickers and `blake3` for content hashing
/// (dirty-state detection). All file operations are synchronous on the
/// calling thread — Dioxus desktop runs on a single UI thread so async
/// file dialogs block the event loop, which is acceptable for brief
/// picker interactions.
use std::fs;
use std::path::{Path, PathBuf};

/// Compute a blake3 hash of YAML content for dirty-state comparison.
pub fn compute_hash(yaml: &str) -> [u8; 32] {
    *blake3::hash(yaml.as_bytes()).as_bytes()
}

/// Show a native "Open File" dialog filtered to YAML files.
///
/// Returns `None` if the user cancels.
pub fn open_file_dialog(starting_dir: Option<&Path>) -> Option<PathBuf> {
    let mut dialog = rfd::FileDialog::new()
        .add_filter("Pipeline YAML", &["yaml", "yml"])
        .set_title("Open Pipeline");

    if let Some(dir) = starting_dir {
        dialog = dialog.set_directory(dir);
    }

    dialog.pick_file()
}

/// Show a native "Save As" dialog filtered to YAML files.
///
/// Returns `None` if the user cancels.
pub fn save_file_dialog(suggested_name: &str, starting_dir: Option<&Path>) -> Option<PathBuf> {
    let mut dialog = rfd::FileDialog::new()
        .add_filter("Pipeline YAML", &["yaml", "yml"])
        .set_title("Save Pipeline As")
        .set_file_name(suggested_name);

    if let Some(dir) = starting_dir {
        dialog = dialog.set_directory(dir);
    }

    dialog.save_file()
}

/// Read a pipeline YAML file from disk.
///
/// Returns the file contents as a UTF-8 string, or an error message.
pub fn read_pipeline_file(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|e| format!("Failed to read {}: {e}", path.display()))
}

/// Write pipeline YAML to disk.
///
/// Uses a simple write (not atomic — Phase 2.75 adds tempfile + rename
/// for `.kiln-state.json`; pipeline YAML is small enough that truncation
/// risk is acceptable in v1).
pub fn write_pipeline_file(path: &Path, content: &str) -> Result<(), String> {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create directory {}: {e}", parent.display()))?;
    }
    fs::write(path, content).map_err(|e| format!("Failed to write {}: {e}", path.display()))
}
