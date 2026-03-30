/// Workspace system: kiln.toml manifest, .kiln-state.json persistence,
/// auto-detection via ancestor walk, auto-creation on first save.
///
/// Spec §F4: kiln.toml is human-editable + version-controlled.
/// .kiln-state.json is machine-managed + gitignored.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Maximum ancestor directory levels to walk when searching for kiln.toml.
const MAX_ANCESTOR_DEPTH: usize = 10;
const APP_DIR_NAME: &str = "clinker-kiln";
const LAST_WORKSPACE_FILE: &str = "last-workspace.json";

// ── Workspace manifest (kiln.toml) ──────────────────────────────────────

/// Parsed kiln.toml workspace manifest.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WorkspaceManifest {
    #[serde(default)]
    pub workspace: WorkspaceConfig,
    #[serde(default)]
    pub pipelines: Option<PipelineDiscovery>,
    #[serde(default)]
    pub schema: Option<SchemaConfig>,
    #[serde(default)]
    pub cli: Option<CliConfig>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    pub name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineDiscovery {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaConfig {
    pub path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CliConfig {
    pub binary: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

// ── IDE state (.kiln-state.json) ────────────────────────────────────────

/// Machine-managed IDE state persisted per workspace.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WorkspaceState {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub window: Option<WindowGeometry>,
    #[serde(default)]
    pub layout: Option<LayoutState>,
    #[serde(default)]
    pub tabs: Option<TabsState>,
    #[serde(default)]
    pub pipelines: HashMap<String, PipelineEditorState>,
    #[serde(default)]
    pub last_open_directory: Option<String>,
}

fn default_version() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WindowGeometry {
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
    pub maximized: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LayoutState {
    pub preset: String,
    pub inspector_width: Option<f32>,
    pub yaml_sidebar_width: Option<f32>,
    pub run_log_expanded: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TabsState {
    pub open: Vec<String>,
    pub active: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PipelineEditorState {
    #[serde(default)]
    pub canvas_positions: HashMap<String, CanvasPosition>,
    #[serde(default)]
    pub canvas_viewport: Option<ViewportState>,
    pub selected_stage: Option<String>,
    pub active_test_profile: Option<String>,
    pub inspector_drawer: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanvasPosition {
    pub x: f64,
    pub y: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewportState {
    pub pan_x: f64,
    pub pan_y: f64,
    pub zoom: f64,
}

// ── Workspace (combined manifest + state) ───────────────────────────────

/// A loaded workspace with its root directory, manifest, and IDE state.
#[derive(Clone, Debug)]
pub struct Workspace {
    /// Directory containing kiln.toml.
    pub root: PathBuf,
    /// Parsed kiln.toml.
    pub manifest: WorkspaceManifest,
    /// Parsed .kiln-state.json (or defaults).
    pub state: WorkspaceState,
}

impl Workspace {
    /// Display name: workspace.name from manifest, or directory name.
    pub fn display_name(&self) -> String {
        self.manifest
            .workspace
            .name
            .clone()
            .unwrap_or_else(|| {
                self.root
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "workspace".to_string())
            })
    }

    /// Make a path relative to the workspace root.
    pub fn relative_path(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .unwrap_or(path)
            .display()
            .to_string()
    }
}

// ── Public API ──────────────────────────────────────────────────────────

/// Walk ancestor directories looking for kiln.toml.
///
/// Spec §F4.4: stops at first kiln.toml found, or after 10 levels.
/// Returns the workspace root (directory containing kiln.toml) if found.
pub fn detect_workspace(file_path: &Path) -> Option<PathBuf> {
    let dir = if file_path.is_file() {
        file_path.parent()?
    } else {
        file_path
    };

    let mut current = dir.to_path_buf();
    for _ in 0..MAX_ANCESTOR_DEPTH {
        if current.join("kiln.toml").exists() {
            return Some(current);
        }
        if !current.pop() {
            break;
        }
    }

    None
}

/// Load a workspace from its root directory.
///
/// Reads kiln.toml and .kiln-state.json (if present).
pub fn load_workspace(root: &Path) -> Option<Workspace> {
    let manifest_path = root.join("kiln.toml");
    let manifest_content = fs::read_to_string(&manifest_path).ok()?;
    let manifest: WorkspaceManifest =
        toml::from_str(&manifest_content).unwrap_or_default();

    let state_path = root.join(".kiln-state.json");
    let state = if state_path.exists() {
        let content = fs::read_to_string(&state_path).unwrap_or_default();
        let parsed: WorkspaceState = serde_json::from_str(&content).unwrap_or_default();
        // Ignore unknown schema versions
        if parsed.version > 1 {
            WorkspaceState::default()
        } else {
            parsed
        }
    } else {
        WorkspaceState::default()
    };

    Some(Workspace {
        root: root.to_path_buf(),
        manifest,
        state,
    })
}

/// Auto-create a minimal kiln.toml in the given directory.
///
/// Spec §F4.3: silent creation as a side effect of saving.
/// Returns true if created, false if already exists or on error.
pub fn auto_create_workspace(dir: &Path) -> bool {
    let manifest_path = dir.join("kiln.toml");
    if manifest_path.exists() {
        return false;
    }

    let content = "# kiln.toml \u{2014} Clinker Kiln workspace\n\
                   # Created automatically. Edit freely or delete to disable workspace features.\n";

    if fs::write(&manifest_path, content).is_err() {
        return false;
    }

    // Append .kiln-state.json to .gitignore if it exists
    append_gitignore(dir);

    true
}

/// Save workspace IDE state to .kiln-state.json.
///
/// Spec §F4.5: atomic write (best-effort — write then rename on supported platforms).
pub fn save_workspace_state(root: &Path, state: &WorkspaceState) {
    let state_path = root.join(".kiln-state.json");
    let Ok(json) = serde_json::to_string_pretty(state) else {
        return;
    };

    // Best-effort atomic write: write to temp then rename
    let temp_path = root.join(".kiln-state.json.tmp");
    if fs::write(&temp_path, &json).is_ok() {
        let _ = fs::rename(&temp_path, &state_path);
    } else {
        // Fallback: direct write
        let _ = fs::write(&state_path, &json);
    }
}

/// Append .kiln-state.json to .gitignore if not already covered.
///
/// Spec §F4.6: only appends if .gitignore already exists.
fn append_gitignore(dir: &Path) {
    let gitignore_path = dir.join(".gitignore");
    if !gitignore_path.exists() {
        return;
    }

    let Ok(content) = fs::read_to_string(&gitignore_path) else {
        return;
    };

    // Check if already covered
    if content.lines().any(|line| {
        let trimmed = line.trim();
        trimmed == ".kiln-state.json" || trimmed == ".kiln-state.json/"
    }) {
        return;
    }

    // Append
    let addition = "\n# Clinker Kiln IDE state (user-specific, not version-controlled)\n\
                    .kiln-state.json\n";
    let _ = fs::write(&gitignore_path, format!("{content}{addition}"));
}

// ── Last workspace tracking (OS app data dir) ───────────────────────────

/// Path to the last-workspace tracker file.
fn last_workspace_path() -> Option<PathBuf> {
    dirs::data_dir().map(|d| d.join(APP_DIR_NAME).join(LAST_WORKSPACE_FILE))
}

/// Remember which workspace was last used (so we can restore on next launch).
pub fn save_last_workspace(root: &Path) {
    let Some(path) = last_workspace_path() else { return };
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let json = serde_json::json!({ "root": root.display().to_string() });
    let _ = fs::write(&path, json.to_string());
}

/// Load the last-used workspace root path.
pub fn load_last_workspace() -> Option<PathBuf> {
    let path = last_workspace_path()?;
    let content = fs::read_to_string(&path).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&content).ok()?;
    let root_str = parsed.get("root")?.as_str()?;
    let root = PathBuf::from(root_str);
    // Only return if the workspace still exists
    if root.join("kiln.toml").exists() {
        Some(root)
    } else {
        None
    }
}

// ── Session save/restore helpers ────────────────────────────────────────

use crate::file_ops;
use crate::state::LayoutPreset;
use crate::tab::TabEntry;

/// Build a WorkspaceState from current app state for persistence.
pub fn build_state_snapshot(
    tabs: &[TabEntry],
    active_file: Option<&str>,
    layout: LayoutPreset,
    run_log_expanded: bool,
) -> WorkspaceState {
    let open_paths: Vec<String> = tabs
        .iter()
        .filter_map(|t| t.file_path.as_ref())
        .map(|p| p.display().to_string())
        .collect();

    WorkspaceState {
        version: 1,
        window: None, // TODO: save window geometry
        layout: Some(LayoutState {
            preset: layout.as_data_attr().to_string(),
            inspector_width: None,
            yaml_sidebar_width: None,
            run_log_expanded: Some(run_log_expanded),
        }),
        tabs: Some(TabsState {
            open: open_paths,
            active: active_file.map(|s| s.to_string()),
        }),
        pipelines: HashMap::new(),
        last_open_directory: None,
    }
}

/// Restore tabs from a WorkspaceState. Returns the tabs and which should be active.
pub fn restore_tabs(state: &WorkspaceState) -> (Vec<TabEntry>, Option<String>) {
    let Some(ref tabs_state) = state.tabs else {
        return (Vec::new(), None);
    };

    let mut tabs = Vec::new();
    for path_str in &tabs_state.open {
        let path = PathBuf::from(path_str);
        if let Ok(yaml) = file_ops::read_pipeline_file(&path) {
            tabs.push(TabEntry::from_file(path, yaml));
        }
    }

    (tabs, tabs_state.active.clone())
}

/// Resolve a LayoutPreset from a string (for state restore).
pub fn parse_layout_preset(s: &str) -> LayoutPreset {
    match s {
        "canvas-focus" => LayoutPreset::CanvasFocus,
        "hybrid" => LayoutPreset::Hybrid,
        "editor-focus" => LayoutPreset::EditorFocus,
        "schematics" => LayoutPreset::Schematics,
        _ => LayoutPreset::Hybrid,
    }
}

/// Show a native directory picker and try to open it as a workspace.
pub fn open_workspace_dialog() -> Option<Workspace> {
    let dialog = rfd::FileDialog::new()
        .set_title("Open Workspace");

    let dir = dialog.pick_folder()?;

    // Check if it has a kiln.toml
    if dir.join("kiln.toml").exists() {
        load_workspace(&dir)
    } else {
        // Try to find kiln.toml in the selected directory's children
        // (user might have picked the parent)
        None
    }
}
