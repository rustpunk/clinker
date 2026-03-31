//! Channel Mode — full-viewport channel management layout.
//!
//! Follows the VersionMode pattern: accent indicator, sub-tab bar, content dispatch.
//! Reads channel state from `TabManagerState.channel_state`.

use dioxus::prelude::*;

use crate::state::TabManagerState;

use super::channel_list::ChannelList;
use super::health_tab::HealthTab;
use super::identity_tab::IdentityTab;
use super::overrides_tab::OverridesTab;

/// Active sub-tab within Channel Mode.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum ChannelTab {
    #[default]
    Identity,
    Overrides,
    Health,
}

impl ChannelTab {
    fn label(self) -> &'static str {
        match self {
            Self::Identity => "Identity",
            Self::Overrides => "Overrides",
            Self::Health => "Health",
        }
    }
}

const CHANNEL_TABS: [ChannelTab; 3] = [
    ChannelTab::Identity,
    ChannelTab::Overrides,
    ChannelTab::Health,
];

/// Channel Mode root component.
#[component]
pub fn ChannelMode() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let mut active_tab = use_signal(|| ChannelTab::Identity);
    let current_tab = (active_tab)();

    let cs = (tab_mgr.channel_state)();
    let channel_state = cs.unwrap_or_else(|| crate::state::ChannelState {
        workspace_root: std::path::PathBuf::new(),
        channels_dir: String::new(),
        groups_dir: String::new(),
        default_channel: None,
        dir_exists: false,
        channels: Vec::new(),
        groups: Vec::new(),
        active_channel: None,
        recent_channels: Vec::new(),
    });
    let has_selected = channel_state.active_channel.is_some();
    let has_channels = !channel_state.channels.is_empty();

    rsx! {
        div { class: "kiln-channel-mode",
            // 2px accent indicator (verdigris)
            div { class: "kiln-channel-mode__indicator" }

            div { class: "kiln-channel-mode__layout",
                // Left: channel list (280px)
                ChannelList {}

                // Right: tab content
                div { class: "kiln-channel-mode__main",
                    if has_channels {
                        // Sub-tab bar
                        div { class: "kiln-channel-tabs",
                            for tab in CHANNEL_TABS {
                                button {
                                    class: if current_tab == tab {
                                        "kiln-channel-tab kiln-channel-tab--active"
                                    } else {
                                        "kiln-channel-tab"
                                    },
                                    onclick: move |_| active_tab.set(tab),
                                    "{tab.label()}"
                                }
                            }
                        }

                        // Content area
                        div { class: "kiln-channel-content",
                            if has_selected {
                                match current_tab {
                                    ChannelTab::Identity => rsx! { IdentityTab {} },
                                    ChannelTab::Overrides => rsx! { OverridesTab {} },
                                    ChannelTab::Health => rsx! { HealthTab {} },
                                }
                            } else {
                                div { class: "kiln-channel-content__empty",
                                    div { class: "kiln-channel-content__empty-icon", "\u{25C8}" }
                                    div { class: "kiln-channel-content__empty-text",
                                        "Select a channel from the list to view details."
                                    }
                                }
                            }
                        }
                    } else {
                        // Empty state — no channels exist
                        div { class: "kiln-channel-empty-state",
                            div { class: "kiln-channel-empty-state__icon", "\u{25C8}" }
                            div { class: "kiln-channel-empty-state__title",
                                "No channels configured"
                            }
                            div { class: "kiln-channel-empty-state__desc",
                                "Channels let you run the same pipeline with different overrides "
                                "per customer, region, or environment."
                            }
                            div { class: "kiln-channel-empty-state__hint",
                                "Create a "
                                span { class: "kiln-channel-empty-state__code", "channels/" }
                                " directory with channel subdirectories to get started."
                            }
                        }
                    }
                }
            }
        }
    }
}
