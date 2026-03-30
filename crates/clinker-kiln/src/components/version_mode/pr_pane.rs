//! PR creation pane — create pull requests from Version Mode.
//! Spec: clinker-kiln-git-addendum.md §G7.

use dioxus::prelude::*;

use clinker_git::{GitOps, ProviderKind};

use crate::components::toast::{toast_error, toast_success, ToastState};
use crate::state::TabManagerState;

/// PR creation pane component.
#[component]
pub fn PrPane(on_close: EventHandler<()>) -> Element {
    let tab_mgr = use_context::<TabManagerState>();
    let mut title = use_signal(String::new);
    let mut body = use_signal(String::new);
    let mut draft = use_signal(|| false);
    let mut target_branch = use_signal(|| "main".to_string());
    let mut creating = use_signal(|| false);
    let mut created_url = use_signal(|| None::<String>);

    let git = (tab_mgr.git_state)();
    let source_branch = git.as_ref().map(|g| g.branch.clone()).unwrap_or_default();

    // Detect provider from remote
    let ws = (tab_mgr.workspace)();
    let provider = ws.as_ref()
        .and_then(|ws| clinker_git::get_remote_url(&ws.root).ok())
        .map(|url| clinker_git::detect_provider(&url))
        .unwrap_or(ProviderKind::Unknown);

    // Auto-suggest title from branch name
    if (title)().is_empty() && !source_branch.is_empty() {
        let suggested = humanize_branch(&source_branch);
        title.set(suggested);
    }

    // Auto-detect target branch
    if let Some(ref ws) = ws {
        let default = clinker_git::get_default_branch(&ws.root).unwrap_or_else(|_| "main".to_string());
        if (target_branch)() == "main" && default != "main" {
            target_branch.set(default);
        }
    }

    let created = (created_url)();

    // Show success screen if PR was created
    if let Some(ref url) = created {
        return rsx! {
            div { class: "kiln-pr-success",
                div { class: "kiln-pr-success__icon", "✓" }
                div { class: "kiln-pr-success__title", "PULL REQUEST CREATED" }
                div { class: "kiln-pr-success__meta",
                    "{provider.label()} · {source_branch} → {target_branch}"
                }
                a {
                    class: "kiln-pr-success__url",
                    href: "{url}",
                    "{url}"
                }
                button {
                    class: "kiln-pr-success__close",
                    onclick: move |_| on_close.call(()),
                    "Done"
                }
            }
        };
    }

    rsx! {
        div { class: "kiln-pr-pane",
            div { class: "kiln-pr-pane__header",
                span { class: "kiln-pr-pane__title", "CREATE PULL REQUEST" }
                button {
                    class: "kiln-pr-pane__close",
                    onclick: move |_| on_close.call(()),
                    "×"
                }
            }

            // Provider indicator
            div { class: "kiln-pr-pane__provider",
                span { class: "kiln-pr-pane__provider-label",
                    "Platform: {provider.label()}"
                }
            }

            // Source → Target
            div { class: "kiln-pr-pane__branches",
                div { class: "kiln-pr-pane__branch",
                    span { class: "kiln-pr-pane__branch-icon", "⑂" }
                    span { class: "kiln-pr-pane__branch-name", "{source_branch}" }
                }
                span { class: "kiln-pr-pane__arrow", "→" }
                div { class: "kiln-pr-pane__branch",
                    input {
                        class: "kiln-pr-pane__branch-input",
                        r#type: "text",
                        value: "{target_branch}",
                        oninput: move |e: FormEvent| target_branch.set(e.value()),
                    }
                }
            }

            // Title
            div { class: "kiln-pr-pane__field",
                label { class: "kiln-pr-pane__label", "TITLE" }
                input {
                    class: "kiln-pr-pane__input",
                    r#type: "text",
                    value: "{title}",
                    oninput: move |e: FormEvent| title.set(e.value()),
                }
            }

            // Description
            div { class: "kiln-pr-pane__field",
                label { class: "kiln-pr-pane__label", "DESCRIPTION" }
                textarea {
                    class: "kiln-pr-pane__textarea",
                    placeholder: "Describe your changes...",
                    value: "{body}",
                    oninput: move |e: FormEvent| body.set(e.value()),
                }
            }

            // Options
            div { class: "kiln-pr-pane__options",
                label {
                    class: "kiln-pr-pane__checkbox",
                    input {
                        r#type: "checkbox",
                        checked: (draft)(),
                        onchange: move |e: FormEvent| draft.set(e.value() == "true"),
                    }
                    "Draft PR"
                }
            }

            // Actions
            div { class: "kiln-pr-pane__actions",
                button {
                    class: "kiln-pr-pane__create-btn",
                    disabled: (title)().is_empty() || (creating)(),
                    onclick: {
                        let ws_root = ws.as_ref().map(|w| w.root.clone());
                        move |_| {
                            let Some(ref root) = ws_root else { return };
                            creating.set(true);

                            let params = clinker_git::PrParams {
                                source_branch: source_branch.clone(),
                                target_branch: (target_branch)(),
                                title: (title)(),
                                body: (body)(),
                                draft: (draft)(),
                            };

                            match clinker_git::create_pr(root, &params, provider) {
                                Ok(result) => {
                                    let mut toast: Signal<Option<ToastState>> = use_context();
                                    toast_success(&mut toast, &format!("PR #{} created", result.number));
                                    created_url.set(Some(result.url));
                                }
                                Err(e) => {
                                    let mut toast: Signal<Option<ToastState>> = use_context();
                                    toast_error(&mut toast, format!("PR creation failed: {e}"));
                                }
                            }
                            creating.set(false);
                        }
                    },
                    if (draft)() { "Create Draft PR" } else { "Create Pull Request" }
                }
                button {
                    class: "kiln-pr-pane__cancel-btn",
                    onclick: move |_| on_close.call(()),
                    "Cancel"
                }
            }
        }
    }
}

/// Convert a branch name to a human-readable PR title.
fn humanize_branch(branch: &str) -> String {
    // Strip common prefixes
    let name = branch
        .strip_prefix("feat/")
        .or_else(|| branch.strip_prefix("fix/"))
        .or_else(|| branch.strip_prefix("chore/"))
        .or_else(|| branch.strip_prefix("refactor/"))
        .unwrap_or(branch);

    // Replace separators with spaces, capitalize first letter
    let words: String = name
        .replace('-', " ")
        .replace('_', " ");

    let mut chars = words.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_humanize_branch() {
        assert_eq!(humanize_branch("feat/normalize-stage"), "Normalize stage");
        assert_eq!(humanize_branch("fix/csv-quoting"), "Csv quoting");
        assert_eq!(humanize_branch("main"), "Main");
        assert_eq!(humanize_branch("chore/update_deps"), "Update deps");
    }
}
