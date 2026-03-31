use dioxus::prelude::*;

/// Which drawer is currently expanded in the inspector.
#[derive(Clone, Copy, PartialEq, Default, Debug)]
pub enum ActiveDrawer {
    #[default]
    None,
    Run,
    Docs,
    Notes,
}

/// Toggle bar with Run / Docs / Notes buttons.
///
/// Spec §A2.3: 32px height, `char-raised` background, three equal-width
/// buttons separated by 1px vertical dividers. Each button has an icon,
/// label, and badge. Active button gets accent-tinted background and
/// 2px accent bottom border.
#[component]
pub fn DrawerToggleBar(active: ActiveDrawer, on_toggle: EventHandler<ActiveDrawer>) -> Element {
    rsx! {
        div {
            class: "kiln-drawer-bar",

            // Run button — phosphor accent
            DrawerButton {
                icon: "\u{25B8}",  // ▸
                label: "Run",
                badge: "\u{2014}", // —
                accent: "var(--kiln-phosphor)",
                is_active: active == ActiveDrawer::Run,
                onclick: move |_| {
                    if active == ActiveDrawer::Run {
                        on_toggle.call(ActiveDrawer::None);
                    } else {
                        on_toggle.call(ActiveDrawer::Run);
                    }
                },
            }

            // Divider
            span { class: "kiln-drawer-divider" }

            // Docs button — verdigris/bpAccent
            DrawerButton {
                icon: "\u{25C7}",  // ◇
                label: "Docs",
                badge: "\u{2014}",
                accent: "var(--kiln-verdigris)",
                is_active: active == ActiveDrawer::Docs,
                onclick: move |_| {
                    if active == ActiveDrawer::Docs {
                        on_toggle.call(ActiveDrawer::None);
                    } else {
                        on_toggle.call(ActiveDrawer::Docs);
                    }
                },
            }

            // Divider
            span { class: "kiln-drawer-divider" }

            // Notes button — iron accent
            DrawerButton {
                icon: "\u{270E}",  // ✎
                label: "Notes",
                badge: "0",
                accent: "var(--kiln-iron)",
                is_active: active == ActiveDrawer::Notes,
                onclick: move |_| {
                    if active == ActiveDrawer::Notes {
                        on_toggle.call(ActiveDrawer::None);
                    } else {
                        on_toggle.call(ActiveDrawer::Notes);
                    }
                },
            }
        }
    }
}

/// A single drawer toggle button.
#[component]
fn DrawerButton(
    icon: &'static str,
    label: &'static str,
    badge: &'static str,
    accent: &'static str,
    is_active: bool,
    onclick: EventHandler<MouseEvent>,
) -> Element {
    rsx! {
        button {
            class: "kiln-drawer-btn",
            "data-active": if is_active { "true" } else { "false" },
            style: if is_active {
                format!("color: {accent}; border-bottom-color: {accent}; \
                         background: color-mix(in srgb, {accent} 10%, transparent);")
            } else {
                String::new()
            },
            onclick: move |e| onclick.call(e),

            span { class: "kiln-drawer-btn-icon", "{icon}" }
            span { class: "kiln-drawer-btn-label", "{label}" }
            span { class: "kiln-drawer-btn-badge", "{badge}" }
        }
    }
}
