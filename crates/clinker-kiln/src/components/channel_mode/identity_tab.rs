//! Identity tab — channel metadata card.
//!
//! Displays id, name, description, contact, tier, tags, active/inactive,
//! inherits chain, variables table, and override summary.

use dioxus::prelude::*;

use crate::state::TabManagerState;

/// Channel identity card component.
#[component]
pub fn IdentityTab() -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let cs = (tab_mgr.channel_state)();
    let Some(ref channel_state) = cs else {
        return rsx! {};
    };

    let summary = channel_state.active_summary();
    let Some(channel) = summary else {
        return rsx! {
            div { class: "kiln-identity-tab__empty",
                "No channel selected."
            }
        };
    };

    let tier_class = channel
        .tier
        .as_deref()
        .map(|t| format!("kiln-identity__tier--{t}"))
        .unwrap_or_default();

    rsx! {
        div { class: "kiln-identity-tab",
            // Identity card
            div { class: "kiln-identity-card",
                // Header
                div { class: "kiln-identity-card__header",
                    h2 { class: "kiln-identity-card__name", "{channel.name}" }
                    span { class: "kiln-identity-card__id", "{channel.id}" }
                    if !channel.active {
                        span { class: "kiln-identity-card__inactive", "INACTIVE" }
                    }
                }

                // Tier badge
                if let Some(ref tier) = channel.tier {
                    div { class: "kiln-identity-card__tier {tier_class}",
                        span { class: "kiln-identity-card__tier-label", "Tier" }
                        span { class: "kiln-identity-card__tier-value", "{tier}" }
                    }
                }

                // Description
                if let Some(ref desc) = channel.description {
                    p { class: "kiln-identity-card__desc", "{desc}" }
                }

                // Contact
                if let Some(ref contact) = channel.contact {
                    div { class: "kiln-identity-card__field",
                        span { class: "kiln-identity-card__label", "Contact" }
                        span { class: "kiln-identity-card__value", "{contact}" }
                    }
                }

                // Tags
                if !channel.tags.is_empty() {
                    div { class: "kiln-identity-card__tags",
                        span { class: "kiln-identity-card__label", "Tags" }
                        div { class: "kiln-identity-card__tag-list",
                            for tag in &channel.tags {
                                span {
                                    key: "{tag}",
                                    class: "kiln-identity-card__tag",
                                    "{tag}"
                                }
                            }
                        }
                    }
                }

                // Inherits
                if !channel.inherits.is_empty() {
                    div { class: "kiln-identity-card__inherits",
                        span { class: "kiln-identity-card__label", "Inherits" }
                        div { class: "kiln-identity-card__inherit-chain",
                            for group_id in &channel.inherits {
                                span {
                                    key: "{group_id}",
                                    class: "kiln-identity-card__inherit-badge",
                                    "{group_id}"
                                }
                            }
                        }
                    }
                }
            }

            // Variables section
            if channel.variable_count > 0 {
                div { class: "kiln-identity-card__section",
                    h3 { class: "kiln-identity-card__section-title",
                        "Variables ({channel.variable_count})"
                    }
                    p { class: "kiln-identity-card__section-note",
                        "Variable values are resolved from channel.yaml."
                    }
                }
            }

            // Override summary
            if channel.override_count > 0 {
                div { class: "kiln-identity-card__section",
                    h3 { class: "kiln-identity-card__section-title",
                        "Overrides ({channel.override_count})"
                    }
                    p { class: "kiln-identity-card__section-note",
                        "Pipeline override files (.channel.yaml) in this channel's directory."
                    }
                }
            }
        }
    }
}
