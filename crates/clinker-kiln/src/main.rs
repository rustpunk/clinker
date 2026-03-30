use dioxus::desktop::{Config, LogicalSize, WindowBuilder};

mod app;
mod autodoc;
mod components;
mod cxl_bridge;
mod demo;
mod notes;
mod pipeline_view;
mod state;
mod sync;

fn main() {
    dioxus::LaunchBuilder::new()
        .with_cfg(
            Config::new()
                .with_window(
                    WindowBuilder::new()
                        .with_title("clinker kiln")
                        .with_decorations(false)
                        .with_inner_size(LogicalSize::new(1400, 900))
                        .with_min_inner_size(LogicalSize::new(800, 600)),
                )
                .with_disable_context_menu(true),
        )
        .launch(app::App);
}
