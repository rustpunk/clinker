// Hide the console window on Windows release builds
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;
use std::sync::OnceLock;

use dioxus::desktop::{Config, LogicalSize, WindowBuilder};

mod app;
mod autodoc;
mod commands;
mod components;
mod cxl_bridge;
mod debug_state;
mod file_ops;
mod fs_watcher;
mod keyboard;
mod notes;
mod pipeline_view;
mod search;
mod state;
mod sync;
mod tab;
mod template;
mod workspace;

/// Workspace path passed via `--workspace <path>` CLI arg.
///
/// Highest priority in session restore — overrides last-used workspace and CWD detection.
static CLI_WORKSPACE: OnceLock<PathBuf> = OnceLock::new();

/// Get the CLI-specified workspace path, if any.
pub fn cli_workspace() -> Option<&'static PathBuf> {
    CLI_WORKSPACE.get()
}

fn main() {
    // Parse --workspace <path> from CLI args
    let args: Vec<String> = std::env::args().collect();
    if let Some(idx) = args.iter().position(|a| a == "--workspace")
        && let Some(path_str) = args.get(idx + 1)
    {
        let path = PathBuf::from(path_str);
        let resolved = if path.is_relative() {
            std::env::current_dir()
                .map(|cwd| cwd.join(&path))
                .unwrap_or(path)
        } else {
            path
        };
        let _ = CLI_WORKSPACE.set(resolved);
    }

    // Inlined into <head> via with_custom_head to bypass the wry custom-protocol
    // asset path on Windows (WebView2 IPC marshaling is ~10s for a 173KB file)
    // and to work around DioxusLabs/dioxus#2847 (asset!()-loaded CSS paints late).
    const KILN_CSS: &str = include_str!("../assets/kiln.css");

    #[cfg_attr(not(target_os = "windows"), allow(unused_mut))]
    let mut cfg = Config::new()
        .with_window(
            WindowBuilder::new()
                .with_title("clinker kiln")
                .with_decorations(false)
                .with_inner_size(LogicalSize::new(1400, 900))
                .with_min_inner_size(LogicalSize::new(800, 600)),
        )
        .with_disable_context_menu(true)
        .with_custom_head(format!("<style>{KILN_CSS}</style>"));

    // Workaround for DioxusLabs/dioxus#2304: keep WebView2's user-data folder
    // in %LOCALAPPDATA% instead of next to the .exe, where ACLs / OneDrive
    // sync can add seconds of cold-start retries.
    #[cfg(target_os = "windows")]
    if let Ok(local) = std::env::var("LOCALAPPDATA") {
        cfg = cfg.with_data_directory(PathBuf::from(local).join("Kiln").join("WebView2"));
    }

    dioxus::LaunchBuilder::new()
        .with_cfg(cfg)
        .launch(app::AppShell);
}
