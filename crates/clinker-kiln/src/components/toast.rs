/// Toast notification overlay with auto-dismiss.
///
/// Spec §F2.2: anchored bottom-right, 16px from edges, auto-dismiss after 5s.
/// Verdigris accent for success, oxide-red for errors. Click to dismiss early.
use dioxus::prelude::*;

/// Toast state: message text + CSS accent color + auto-dismiss duration.
#[derive(Clone, Debug)]
pub struct ToastState {
    pub message: String,
    pub accent: &'static str,
    pub dismiss_ms: u64,
}

/// Show a success toast (verdigris, 3s).
pub fn toast_success(signal: &mut Signal<Option<ToastState>>, message: impl Into<String>) {
    signal.set(Some(ToastState {
        message: message.into(),
        accent: "#43B3AE",
        dismiss_ms: 3000,
    }));
}

/// Show an error toast (oxide-red, 5s).
pub fn toast_error(signal: &mut Signal<Option<ToastState>>, message: impl Into<String>) {
    signal.set(Some(ToastState {
        message: message.into(),
        accent: "#B7410E",
        dismiss_ms: 5000,
    }));
}

/// Toast overlay — renders the current toast message if any, with auto-dismiss.
#[component]
pub fn ToastOverlay() -> Element {
    let mut toast: Signal<Option<ToastState>> = use_context();
    let current = (toast)();

    // Auto-dismiss timer: spawn a task whenever a toast appears
    use_effect(move || {
        let state = (toast)();
        if let Some(ref t) = state {
            let ms = t.dismiss_ms;
            spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                toast.set(None);
            });
        }
    });

    if let Some(t) = current {
        rsx! {
            div {
                class: "kiln-toast",
                style: "border-left-color: {t.accent};",
                onclick: move |_| {
                    toast.set(None);
                },
                "{t.message}"
            }
        }
    } else {
        rsx! {}
    }
}
