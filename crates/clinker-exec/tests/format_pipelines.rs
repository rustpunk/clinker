//! End-to-end format integration suite.
//!
//! Keeping related format cases in one harness avoids relinking the complete
//! executor dependency graph for every individual case while preserving each
//! case as its own Rust module.

#[path = "common/mod.rs"]
mod common;

macro_rules! format_pipeline_cases {
    ($($module:ident => $path:literal),+ $(,)?) => {
        $(
            #[path = $path]
            mod $module;
        )+

        const DECLARED_FORMAT_PIPELINE_CASES: &[&str] = &[
            $(concat!(stringify!($module), ".rs")),+
        ];
    };
}

format_pipeline_cases! {
    csv_charset => "format_pipelines/csv_charset.rs",
    csv_join_values_e2e => "format_pipelines/csv_join_values_e2e.rs",
    csv_split_values_e2e => "format_pipelines/csv_split_values_e2e.rs",
    edifact_pipeline => "format_pipelines/edifact_pipeline.rs",
    format_dispatch => "format_pipelines/format_dispatch.rs",
    hl7_pipeline => "format_pipelines/hl7_pipeline.rs",
    json_nested_write_e2e => "format_pipelines/json_nested_write_e2e.rs",
    nested_cxl_write_e2e => "format_pipelines/nested_cxl_write_e2e.rs",
    numeric_guess_parity => "format_pipelines/numeric_guess_parity.rs",
    swift_pipeline => "format_pipelines/swift_pipeline.rs",
    x12_pipeline => "format_pipelines/x12_pipeline.rs",
    xml_multi_value_e2e => "format_pipelines/xml_multi_value_e2e.rs",
    xml_nested_write_e2e => "format_pipelines/xml_nested_write_e2e.rs",
}

#[test]
fn suite_declares_every_format_pipeline_case() {
    let case_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("format_pipelines");

    let mut discovered = std::fs::read_dir(case_dir)
        .expect("format pipeline case directory exists")
        .map(|entry| {
            entry
                .expect("format pipeline case entry is readable")
                .file_name()
                .into_string()
                .expect("format pipeline case names are UTF-8")
        })
        .filter(|name| name.ends_with(".rs"))
        .collect::<Vec<_>>();
    discovered.sort_unstable();

    let mut declared = DECLARED_FORMAT_PIPELINE_CASES
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    declared.sort_unstable();

    assert_eq!(declared, discovered, "format pipeline suite drifted");
}
