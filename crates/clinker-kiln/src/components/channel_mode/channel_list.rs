//! Channel list sidebar — 280px left region within Channel Mode.
//!
//! Search input at top, tier filter tabs, channel entries with badges,
//! groups section at bottom, "New Channel" button.

use dioxus::prelude::*;

use crate::state::{ChannelSummary, GroupSummary, TabManagerState};
use crate::workspace;

/// Channel list sidebar component.
#[component]
pub fn ChannelList() -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();
    let mut search_text = use_signal(String::new);
    let mut tier_filter = use_signal(|| "all".to_string());
    let mut show_groups = use_signal(|| false);
    let mut new_channel_input = use_signal(|| None::<String>);

    let cs = (tab_mgr.channel_state)();
    let Some(channel_state) = cs else {
        return rsx! {};
    };

    let query = (search_text)().to_lowercase();
    let active_tier = (tier_filter)();
    let active_channel = channel_state.active_channel.clone();
    let tiers = channel_state.tier_names();
    let groups_expanded = (show_groups)();

    // Filter channels by search + tier
    let filtered: Vec<&ChannelSummary> = channel_state
        .channels
        .iter()
        .filter(|c| {
            if active_tier != "all" {
                if c.tier.as_deref() != Some(active_tier.as_str()) {
                    return false;
                }
            }
            if query.is_empty() {
                return true;
            }
            c.id.to_lowercase().contains(&query)
                || c.name.to_lowercase().contains(&query)
                || c.description
                    .as_deref()
                    .unwrap_or("")
                    .to_lowercase()
                    .contains(&query)
                || c.tags.iter().any(|t| t.to_lowercase().contains(&query))
                || c.tier
                    .as_deref()
                    .unwrap_or("")
                    .to_lowercase()
                    .contains(&query)
        })
        .collect();

    let total = channel_state.channels.len();
    let groups = &channel_state.groups;
    let workspace_root = channel_state.workspace_root.clone();
    let channels_dir_name = channel_state.channels_dir.clone();
    let is_creating = (new_channel_input)().is_some();

    rsx! {
        div { class: "kiln-channel-list",
            // Search + New Channel header
            div { class: "kiln-channel-list__header",
                input {
                    class: "kiln-channel-list__search-input",
                    r#type: "text",
                    placeholder: "Search channels...",
                    value: "{search_text}",
                    oninput: move |e| search_text.set(e.value()),
                }
                button {
                    class: "kiln-channel-list__new-btn",
                    title: "New Channel",
                    onclick: move |_| {
                        new_channel_input.set(Some(String::new()));
                    },
                    "+"
                }
            }

            // Inline new channel input
            if is_creating {
                {
                    let ws_root = workspace_root.clone();
                    let chan_dir = channels_dir_name.clone();
                    rsx! {
                        div { class: "kiln-channel-list__new-row",
                            input {
                                class: "kiln-channel-list__new-input",
                                r#type: "text",
                                placeholder: "channel-id",
                                autofocus: true,
                                value: (new_channel_input)().unwrap_or_default(),
                                oninput: move |e| {
                                    new_channel_input.set(Some(e.value()));
                                },
                                onkeydown: move |e: KeyboardEvent| {
                                    match e.key() {
                                        Key::Enter => {
                                            let id = (new_channel_input)().unwrap_or_default();
                                            let id = id.trim().to_string();
                                            if !id.is_empty() {
                                                let ws_root = ws_root.clone();
                                                let chan_dir = chan_dir.clone();
                                                create_channel(&ws_root, &chan_dir, &id);
                                                new_channel_input.set(None);
                                                // Refresh channel state
                                                if let Some(ref ws) = (tab_mgr.workspace)() {
                                                    let state = workspace::discover_channels(ws);
                                                    if let Some(s) = state {
                                                        tab_mgr.channel_state.write().replace(s);
                                                    }
                                                }
                                            }
                                        }
                                        Key::Escape => {
                                            new_channel_input.set(None);
                                        }
                                        _ => {}
                                    }
                                },
                            }
                            button {
                                class: "kiln-channel-list__new-cancel",
                                onclick: move |_| new_channel_input.set(None),
                                "\u{2715}"
                            }
                        }
                    }
                }
            }

            // Tier filter tabs
            div { class: "kiln-channel-list__tiers",
                button {
                    class: if active_tier == "all" {
                        "kiln-channel-tier kiln-channel-tier--active"
                    } else {
                        "kiln-channel-tier"
                    },
                    onclick: move |_| tier_filter.set("all".to_string()),
                    "All ({total})"
                }
                for tier in &tiers {
                    {
                        let tier_val = tier.clone();
                        let is_active = active_tier == *tier;
                        let count = channel_state
                            .channels
                            .iter()
                            .filter(|c| c.tier.as_deref() == Some(tier.as_str()))
                            .count();
                        rsx! {
                            button {
                                key: "{tier}",
                                class: if is_active {
                                    "kiln-channel-tier kiln-channel-tier--active"
                                } else {
                                    "kiln-channel-tier"
                                },
                                onclick: move |_| tier_filter.set(tier_val.clone()),
                                "{tier} ({count})"
                            }
                        }
                    }
                }
            }

            // Channel entries
            div { class: "kiln-channel-list__entries",
                for channel in &filtered {
                    {
                        let is_active = active_channel.as_deref() == Some(channel.id.as_str());
                        let channel_id = channel.id.clone();
                        let tier_class = channel
                            .tier
                            .as_deref()
                            .map(|t| format!("kiln-channel-entry__tier--{t}"))
                            .unwrap_or_default();

                        rsx! {
                            div {
                                key: "{channel.id}",
                                class: if is_active {
                                    "kiln-channel-entry kiln-channel-entry--active"
                                } else if !channel.active {
                                    "kiln-channel-entry kiln-channel-entry--inactive"
                                } else {
                                    "kiln-channel-entry"
                                },
                                onclick: move |_| {
                                    let mut cs = tab_mgr.channel_state.write();
                                    if let Some(ref mut state) = *cs {
                                        state.select_channel(Some(channel_id.clone()));
                                    }
                                },

                                // Tier pip
                                if let Some(ref tier) = channel.tier {
                                    span {
                                        class: "kiln-channel-entry__pip {tier_class}",
                                        title: "{tier}",
                                    }
                                }

                                div { class: "kiln-channel-entry__info",
                                    span { class: "kiln-channel-entry__name",
                                        "{channel.name}"
                                    }
                                    span { class: "kiln-channel-entry__id",
                                        "{channel.id}"
                                    }
                                }

                                div { class: "kiln-channel-entry__meta",
                                    if channel.override_count > 0 {
                                        span { class: "kiln-channel-entry__badge",
                                            "{channel.override_count} overrides"
                                        }
                                    }
                                    if !channel.active {
                                        span { class: "kiln-channel-entry__badge kiln-channel-entry__badge--inactive",
                                            "inactive"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Groups section
            if !groups.is_empty() {
                div { class: "kiln-channel-list__groups-header",
                    button {
                        class: "kiln-channel-list__groups-toggle",
                        onclick: move |_| show_groups.set(!groups_expanded),
                        if groups_expanded { "▾ " } else { "▸ " }
                        "Groups ({groups.len()})"
                    }
                }

                if groups_expanded {
                    div { class: "kiln-channel-list__groups",
                        for group in groups {
                            {
                                let inheritor_count = group.inheritor_ids.len();
                                rsx! {
                                    div {
                                        key: "{group.id}",
                                        class: "kiln-channel-entry kiln-channel-entry--group",

                                        span { class: "kiln-channel-entry__pip kiln-channel-entry__pip--group" }

                                        div { class: "kiln-channel-entry__info",
                                            span { class: "kiln-channel-entry__name",
                                                "{group.id}"
                                            }
                                            span { class: "kiln-channel-entry__id",
                                                "{inheritor_count} channels inherit"
                                            }
                                        }

                                        div { class: "kiln-channel-entry__meta",
                                            if group.override_count > 0 {
                                                span { class: "kiln-channel-entry__badge",
                                                    "{group.override_count} overrides"
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
        }
    }
}

/// Create a new channel directory with a template `channel.yaml` manifest.
fn create_channel(workspace_root: &std::path::Path, channels_dir: &str, id: &str) {
    let channels_abs = workspace_root.join(channels_dir);

    // Create channels/ directory if it doesn't exist
    if !channels_abs.is_dir() {
        let _ = std::fs::create_dir_all(&channels_abs);
    }

    let channel_dir = channels_abs.join(id);
    if channel_dir.exists() {
        return; // Channel already exists
    }
    let _ = std::fs::create_dir_all(&channel_dir);

    // Write template channel.yaml
    let display_name = id
        .replace('-', " ")
        .replace('_', " ")
        .split_whitespace()
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().to_string() + c.as_str(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ");

    let manifest = format!(
        "_channel:\n  id: {id}\n  name: \"{display_name}\"\n  active: true\n\nvariables: {{}}\n"
    );

    let _ = std::fs::write(channel_dir.join("channel.yaml"), manifest);
}
