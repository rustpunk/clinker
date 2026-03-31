use dioxus::prelude::*;

use crate::components::yaml_sidebar::tokenizer::tokenize;
use crate::state::{use_app_state, ChannelViewMode, TabManagerState};
use crate::sync::compute_yaml_ranges;

/// Bottom section of the inspector showing the selected stage's YAML block
/// with absolute line numbers from the full YAML.
#[component]
pub fn ScopedYaml(stage_id: String, accent: &'static str) -> Element {
    let state = use_app_state();
    let tab_mgr = use_context::<TabManagerState>();
    let raw_text = (state.yaml_text)();
    let pipeline_guard = (state.pipeline).read();

    let Some(config) = pipeline_guard.as_ref() else {
        return rsx! {};
    };

    // Channel view mode support
    let channel_resolution = (state.channel_pipeline)();
    let has_channel = channel_resolution.is_some()
        && (tab_mgr.channel_state)()
            .as_ref()
            .and_then(|cs| cs.active_channel.as_ref())
            .is_some();
    let view_mode = if has_channel { (state.channel_view_mode)() } else { ChannelViewMode::Base };

    // Determine which text/config to scope from
    let (text, scoped_config) = match view_mode {
        ChannelViewMode::Resolved => {
            if let Some(ref cr) = channel_resolution {
                let resolved_text = crate::sync::serialize_yaml(&cr.resolved_config);
                (resolved_text, Some(&cr.resolved_config))
            } else {
                (raw_text.clone(), Some(config))
            }
        }
        _ => (raw_text.clone(), Some(config)),
    };

    let resolve_config = scoped_config.unwrap_or(config);
    let ranges = compute_yaml_ranges(&text, resolve_config);
    let Some(&(start, end)) = ranges.get(&stage_id) else {
        return rsx! {};
    };

    let all_lines = tokenize(&text);
    let start_idx = start.saturating_sub(1); // 0-indexed
    let end_idx = end.min(all_lines.len());
    let scoped_lines = &all_lines[start_idx..end_idx];

    let section_title = match view_mode {
        ChannelViewMode::Base => "STAGE YAML",
        ChannelViewMode::Resolved => "STAGE YAML (RESOLVED)",
        ChannelViewMode::Channel => "STAGE YAML (CHANNEL)",
    };

    rsx! {
        div {
            class: "kiln-inspector-yaml",

            div {
                class: "kiln-section-header",
                span { class: "kiln-diamond", "\u{25C6}" }
                span { class: "kiln-section-title", "{section_title}" }

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

                span { class: "kiln-section-rule" }
            }

            div {
                class: "kiln-yaml-code-area kiln-inspector-yaml-area",

                // Gutter — absolute line numbers
                div {
                    class: "kiln-yaml-gutter",
                    for (i, _) in scoped_lines.iter().enumerate() {
                        {
                            let line_num = start + i;
                            rsx! {
                                div {
                                    key: "gutter-{i}",
                                    class: "kiln-yaml-line-num",
                                    "{line_num}"
                                }
                            }
                        }
                    }
                }

                // Code column
                div {
                    class: "kiln-yaml-code",
                    for (i, line_tokens) in scoped_lines.iter().enumerate() {
                        div {
                            key: "scoped-{i}",
                            class: "kiln-yaml-line",
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
    }
}
