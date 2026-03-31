use dioxus::prelude::*;

use clinker_git::{BlameLine, GitOps};

use crate::state::{ChannelViewMode, TabManagerState, use_app_state};
use crate::sync::EditSource;

use super::tokenizer::tokenize;

/// Full-height YAML sidebar with syntax-highlighted overlay and editable textarea.
///
/// Architecture: a transparent `<textarea>` sits on top of a syntax-coloured
/// `<pre>` that shows the same text with tokenized `<span>` elements. The
/// textarea captures keystrokes; the pre shows the colours. Both scroll
/// together via a shared container.
///
/// Blame gutter: toggleable column showing per-line git blame (author, time, hash).
/// Spec §G9.
#[component]
pub fn YamlSidebar() -> Element {
    let state = use_app_state();
    let tab_mgr = use_context::<TabManagerState>();
    let raw_text = (state.yaml_text)();
    let errors = (state.parse_errors)();
    let raw_lines = tokenize(&raw_text);
    let raw_line_count = raw_lines.len().max(1);
    let mut blame_visible = use_signal(|| false);
    let mut blame_data = use_signal(Vec::<BlameLine>::new);

    let show_blame = (blame_visible)();

    // Compute selected stage's YAML line range for highlighting.
    let selected_range: Option<(usize, usize)> = {
        let selected = (state.selected_stage)();
        let pipeline_guard = (state.pipeline).read();
        match (selected.as_deref(), pipeline_guard.as_ref()) {
            (Some(stage_id), Some(config)) => {
                let ranges = crate::sync::compute_yaml_ranges(&raw_text, config);
                ranges.get(stage_id).copied()
            }
            _ => None,
        }
    };

    // Schema warnings for YAML squiggles
    let _warnings = (state.schema_warnings)();

    // Channel view mode — Base / Resolved / Channel
    let channel_resolution = (state.channel_pipeline)();
    let has_channel = channel_resolution.is_some()
        && (tab_mgr.channel_state)()
            .as_ref()
            .and_then(|cs| cs.active_channel.as_ref())
            .is_some();
    let view_mode = if has_channel {
        (state.channel_view_mode)()
    } else {
        ChannelViewMode::Base
    };

    // Load channel override text for Channel mode
    let channel_override_text = if view_mode == ChannelViewMode::Channel {
        load_channel_override_text(&tab_mgr)
    } else {
        None
    };

    // Determine displayed text and editability
    let (text, lines, line_count, is_editable) = match view_mode {
        ChannelViewMode::Resolved => {
            let resolved_text = if let Some(ref cr) = channel_resolution {
                crate::sync::serialize_yaml(&cr.resolved_config)
            } else {
                raw_text.clone()
            };
            let resolved_lines = tokenize(&resolved_text);
            let count = resolved_lines.len().max(1);
            (resolved_text, resolved_lines, count, false)
        }
        ChannelViewMode::Channel => {
            let chan_text = channel_override_text.unwrap_or_default();
            let chan_lines = tokenize(&chan_text);
            let count = chan_lines.len().max(1);
            (chan_text, chan_lines, count, true)
        }
        ChannelViewMode::Base => (raw_text.clone(), raw_lines, raw_line_count, true),
    };

    let section_title = match view_mode {
        ChannelViewMode::Base => "PIPELINE YAML",
        ChannelViewMode::Resolved => "RESOLVED YAML (read-only)",
        ChannelViewMode::Channel => "CHANNEL OVERRIDE",
    };

    rsx! {
        div {
            class: "kiln-yaml-sidebar",

            // Section header with blame toggle
            div {
                class: "kiln-section-header",
                span { class: "kiln-diamond", "\u{25C6}" }
                span { class: "kiln-section-title", "{section_title}" }
                span { class: "kiln-section-rule" }

                // Channel view mode toggle (when channel is active)
                if has_channel {
                    div {
                        class: "kiln-yaml-channel-toggle",
                        for mode in ChannelViewMode::ALL {
                            {
                                let is_active = view_mode == mode;
                                let mut view_sig = state.channel_view_mode;
                                rsx! {
                                    button {
                                        key: "{mode.label()}",
                                        class: if is_active {
                                            "kiln-yaml-toggle-btn kiln-yaml-toggle-btn--active"
                                        } else {
                                            "kiln-yaml-toggle-btn"
                                        },
                                        onclick: move |_| view_sig.set(mode),
                                        "{mode.label()}"
                                    }
                                }
                            }
                        }
                    }
                }

                // Blame toggle button (only when git repo detected)
                if (tab_mgr.git_state)().is_some() {
                    button {
                        class: if show_blame {
                            "kiln-blame-toggle kiln-blame-toggle--active"
                        } else {
                            "kiln-blame-toggle"
                        },
                        onclick: move |_| {
                            let new_val = !show_blame;
                            blame_visible.set(new_val);
                            if new_val && (blame_data)().is_empty() {
                                // Load blame data
                                load_blame(&tab_mgr, &mut blame_data);
                            }
                        },
                        "⑂ BLAME"
                    }
                }
            }

            // Code area: blame gutter + line numbers + editor
            div {
                class: "kiln-yaml-code-area",

                // Blame gutter (toggleable, 130px)
                if show_blame {
                    div {
                        class: "kiln-blame-gutter",
                        for i in 0..line_count {
                            {
                                let bl = (blame_data)();
                                let blame = bl.iter().find(|b| b.line == i + 1).cloned();
                                let prev_blame = if i > 0 {
                                    bl.iter().find(|b| b.line == i).cloned()
                                } else {
                                    None
                                };
                                let is_group_start = blame.as_ref().map(|b| {
                                    prev_blame.as_ref().map(|pb| pb.commit_id != b.commit_id).unwrap_or(true)
                                }).unwrap_or(false);

                                if let Some(ref b) = blame {
                                    let author = if b.author.len() > 8 { b.author[..8].to_string() } else { b.author.clone() };
                                    let time = relative_time_short(b.timestamp);
                                    let hash = b.commit_id.clone();

                                    rsx! {
                                        div {
                                            key: "blame-{i}",
                                            class: "kiln-blame-line",
                                            if is_group_start {
                                                span { class: "kiln-blame-author", "{author}" }
                                                span { class: "kiln-blame-time", "{time}" }
                                                span { class: "kiln-blame-hash", "{hash}" }
                                            } else {
                                                span { class: "kiln-blame-continuation", "│" }
                                            }
                                        }
                                    }
                                } else {
                                    rsx! {
                                        div {
                                            key: "blame-{i}",
                                            class: "kiln-blame-line",
                                            span { class: "kiln-blame-uncommitted", "uncommitted" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Line-number gutter
                div {
                    class: "kiln-yaml-gutter",
                    for i in 0..line_count {
                        {
                            let line_num = i + 1;
                            let in_range = selected_range
                                .is_some_and(|(s, e)| line_num >= s && line_num <= e);
                            // Check for schema warnings on this line
                            let _has_warning = false; // TODO: map warnings to line numbers
                            rsx! {
                                div {
                                    key: "gutter-{i}",
                                    class: "kiln-yaml-line-num",
                                    "data-selected": if in_range { "true" },
                                    "{line_num}"
                                }
                            }
                        }
                    }
                }

                // Editor container: syntax overlay + textarea stacked
                div {
                    class: "kiln-yaml-editor",

                    // Syntax-highlighted overlay (read-only visual layer)
                    div {
                        class: "kiln-yaml-highlight",
                        for (i, line_tokens) in lines.iter().enumerate() {
                            {
                                let line_num = i + 1;
                                let in_range = selected_range
                                    .is_some_and(|(s, e)| line_num >= s && line_num <= e);
                                rsx! {
                                    div {
                                        key: "line-{i}",
                                        class: "kiln-yaml-line",
                                        "data-selected": if in_range { "true" },
                                        for (j, token) in line_tokens.iter().enumerate() {
                                            span {
                                                key: "tok-{i}-{j}",
                                                "data-token": token.kind.as_data_attr(),
                                                "{token.text}"
                                            }
                                        }
                                        if line_tokens.iter().all(|t| t.text.is_empty()) {
                                            "\u{00A0}"
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Transparent textarea (captures input)
                    if is_editable {
                        textarea {
                            class: "kiln-yaml-textarea",
                            spellcheck: "false",
                            value: "{text}",
                            oninput: move |e: FormEvent| {
                                if view_mode == ChannelViewMode::Channel {
                                    // Channel override editing — save to file
                                    save_channel_override_text(&tab_mgr, &e.value());
                                } else {
                                    // Base pipeline editing
                                    let mut src = state.edit_source;
                                    src.set(EditSource::Yaml);
                                    let mut yaml = state.yaml_text;
                                    yaml.set(e.value());
                                }
                            },
                        }
                    } else {
                        textarea {
                            class: "kiln-yaml-textarea kiln-yaml-textarea--readonly",
                            spellcheck: "false",
                            readonly: true,
                            value: "{text}",
                        }
                    }
                }
            }

            // Parse error bar
            if !errors.is_empty() {
                div {
                    class: "kiln-yaml-errors",
                    for (i, err) in errors.iter().enumerate() {
                        {
                            let err_text = err.clone();
                            let err_display = err.clone();
                            rsx! {
                                div {
                                    key: "err-{i}",
                                    class: "kiln-yaml-error",
                                    span {
                                        class: "kiln-yaml-error-text",
                                        "{err_display}"
                                    }
                                    button {
                                        class: "kiln-yaml-error-copy",
                                        title: "Copy error to clipboard",
                                        onclick: move |_| {
                                            let text = err_text.clone();
                                            let js = format!(
                                                "navigator.clipboard.writeText({})",
                                                serde_json::to_string(&text).unwrap_or_default()
                                            );
                                            document::eval(&js);
                                        },
                                        "\u{2398}"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Load blame data for the current file.
fn load_blame(tab_mgr: &TabManagerState, blame_data: &mut Signal<Vec<BlameLine>>) {
    let ws = (tab_mgr.workspace)();
    let Some(ws) = ws else { return };

    // Get the active tab's file path
    let active_id = (tab_mgr.active_tab_id)();
    let tabs = tab_mgr.tabs.read();
    let active_tab = active_id.and_then(|id| tabs.iter().find(|t| t.id == id));
    let Some(tab) = active_tab else { return };
    let Some(ref file_path) = tab.file_path else {
        return;
    };

    // Make path relative to repo root
    let relative = file_path.strip_prefix(&ws.root).unwrap_or(file_path);

    if let Ok(ops) = clinker_git::GitCliOps::discover(&ws.root)
        && let Ok(lines) = ops.blame(relative)
    {
        blame_data.set(lines);
    }
}

/// Derive the channel override file path for the active tab + active channel.
fn channel_override_path(tab_mgr: &TabManagerState) -> Option<std::path::PathBuf> {
    let cs = (tab_mgr.channel_state)();
    let channel_state = cs.as_ref()?;
    let channel_id = channel_state.active_channel.as_ref()?;
    let channel_dir = channel_state
        .workspace_root
        .join(&channel_state.channels_dir)
        .join(channel_id);

    let active_id = (tab_mgr.active_tab_id)();
    let tabs = tab_mgr.tabs.read();
    let tab = active_id.and_then(|id| tabs.iter().find(|t| t.id == id))?;
    let file_path = tab.file_path.as_ref()?;

    Some(clinker_channel::channel_override::ChannelOverride::path_for(file_path, &channel_dir))
}

/// Load channel override YAML text from disk.
fn load_channel_override_text(tab_mgr: &TabManagerState) -> Option<String> {
    let path = channel_override_path(tab_mgr)?;
    if path.exists() {
        std::fs::read_to_string(&path).ok()
    } else {
        // Return template for new override file
        Some("# Channel override for this pipeline\n# Add transform overrides here\n\n_override:\n  transforms: []\n".to_string())
    }
}

/// Save channel override YAML text to disk.
fn save_channel_override_text(tab_mgr: &TabManagerState, content: &str) {
    if let Some(path) = channel_override_path(tab_mgr) {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(&path, content);
    }
}

/// Short relative time for blame gutter (2h, 3d, 1w, 2mo).
fn relative_time_short(timestamp: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let diff = now - timestamp;

    if diff < 3600 {
        format!("{}m", diff / 60)
    } else if diff < 86400 {
        format!("{}h", diff / 3600)
    } else if diff < 604800 {
        format!("{}d", diff / 86400)
    } else if diff < 2592000 {
        format!("{}w", diff / 604800)
    } else {
        format!("{}mo", diff / 2592000)
    }
}
