// Hide the console window on Windows release builds
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;
use std::sync::OnceLock;

use dioxus::desktop::{Config, LogicalSize, WindowBuilder};

mod app;
mod autodoc;
mod channel_resolve;
mod commands;
mod components;
mod composition_index;
mod composition_ops;
mod cxl_bridge;
mod demo;
mod file_ops;
mod fs_watcher;
mod keyboard;
mod notes;
mod pipeline_view;
mod recent_files;
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

    dioxus::LaunchBuilder::new()
        .with_cfg(
            Config::new()
                .with_window(
                    WindowBuilder::new()
                        .with_title("clinker kiln")
                        .with_decorations(false)
                        .with_inner_size(LogicalSize::new(1400, 900))
                        .with_min_inner_size(LogicalSize::new(800, 600)),
                )
                .with_disable_context_menu(true),
        )
        .launch(app::AppShell);
}
