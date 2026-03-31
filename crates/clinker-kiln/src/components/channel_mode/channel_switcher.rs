//! Channel switcher dropdown — anchored to the title bar badge.
//!
//! Built on `dioxus-nox-select` for proper dropdown positioning,
//! keyboard navigation, and fuzzy search.

use dioxus::prelude::*;
use dioxus_nox_select::*;

use crate::state::TabManagerState;

/// Channel switcher — a select-only dropdown anchored to a badge trigger.
///
/// Renders the `select::Root` container that wraps both the trigger badge
/// and the dropdown content. The caller places this component where the
/// trigger should appear in the DOM.
#[component]
pub fn ChannelSwitcher(
    /// Label text shown on the trigger badge (e.g. "NO CHANNEL" or channel id).
    badge_label: String,
    /// Optional tier class for badge styling.
    #[props(default)]
    tier_class: String,
    /// Whether a channel is active.
    #[props(default)]
    has_channel: bool,
) -> Element {
    let mut tab_mgr = use_context::<TabManagerState>();

    let cs = (tab_mgr.channel_state)();
    let Some(ref channel_state) = cs else {
        return rsx! {};
    };

    let active_value = channel_state
        .active_channel
        .clone()
        .unwrap_or_default();

    let channels = channel_state.channels.clone();

    rsx! {
        select::Root {
            class: "kiln-channel-select",
            default_value: "{active_value}",
            on_value_change: move |val: String| {
                let new_id = if val.is_empty() { None } else { Some(val) };
                let mut cs = tab_mgr.channel_state.write();
                if let Some(ref mut state) = *cs {
                    state.select_channel(new_id);
                }
            },

            select::Trigger {
                class: "kiln-channel-badge {tier_class}",
                "data-active": if has_channel { "true" } else { "false" },
                "[{badge_label} \u{25be}]"
            }

            select::Content {
                class: "kiln-channel-dropdown",

                select::Item { value: "", label: "No Channel",
                    select::ItemIndicator { "(\u{25cf})" }
                    select::ItemText { "No Channel" }
                    span { class: "kiln-channel-dropdown__desc", "(run base pipeline)" }
                }

                for ch in channels {
                    {
                        let override_count = ch.override_count;
                        let tier = ch.tier.clone();
                        rsx! {
                            select::Item {
                                key: "{ch.id}",
                                value: "{ch.id}",
                                label: "{ch.id}",
                                select::ItemIndicator { "(\u{25cf})" }
                                select::ItemText { "{ch.id}" }
                                if let Some(ref t) = tier {
                                    span { class: "kiln-channel-dropdown__tier", "{t}" }
                                }
                                if override_count > 0 {
                                    span { class: "kiln-channel-dropdown__count",
                                        "{override_count} overrides"
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
