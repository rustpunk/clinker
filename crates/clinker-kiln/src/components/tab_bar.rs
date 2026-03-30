/// Tab bar: horizontal strip below the title bar showing open pipeline tabs.
///
/// Spec §F3.1: 28px height, active/inactive styling, dirty dot, close button, [+] new tab.

use dioxus::prelude::*;

use crate::keyboard;
use crate::state::TabManagerState;
use crate::tab::TabEntry;

/// Tab bar component spanning the full viewport width.
#[component]
pub fn TabBar() -> Element {
    let mut tab_mgr: TabManagerState = use_context();
    let mut tabs = tab_mgr.tabs;
    let active_id = tab_mgr.active_tab_id;

    let current_active = (active_id)();

    rsx! {
        div {
            class: "kiln-tab-bar",

            for tab in tabs.read().iter() {
                {
                    let tab_id = tab.id;
                    let is_active = current_active == Some(tab_id);
                    let is_dirty = tab.is_dirty();
                    let name = tab.display_name();
                    let class = if is_active {
                        "kiln-tab kiln-tab--active"
                    } else {
                        "kiln-tab"
                    };

                    rsx! {
                        div {
                            key: "{tab_id}",
                            class: "{class}",
                            onclick: move |_| {
                                let mut active = tab_mgr.active_tab_id;
                                active.set(Some(tab_id));
                            },
                            title: "{tab.full_path().unwrap_or_default()}",

                            if is_dirty {
                                span { class: "kiln-tab-dirty", "\u{25CF} " }
                            }
                            span { class: "kiln-tab-name", "{name}" }

                            button {
                                class: "kiln-tab-close",
                                onclick: move |e: MouseEvent| {
                                    e.stop_propagation();
                                    keyboard::request_close_tab(&mut tab_mgr, tab_id);
                                },
                                "\u{00D7}"
                            }
                        }
                    }
                }
            }

            // [+] New tab button
            button {
                class: "kiln-tab-new",
                onclick: move |_| {
                    let new_tab = TabEntry::new_untitled(&tabs.read());
                    let new_id = new_tab.id;
                    tabs.write().push(new_tab);
                    tab_mgr.active_tab_id.set(Some(new_id));
                },
                "+"
            }
        }
    }
}
