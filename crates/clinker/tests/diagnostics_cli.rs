//! End-to-end contract for registry-derived diagnostic discovery.

use std::collections::BTreeMap;
use std::process::{Command, Output};

use clinker_core_types::diagnostic::{DiagnosticCategory, DiagnosticLifecycle, REGISTRY};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .args(args)
        .output()
        .expect("run clinker")
}

fn success(args: &[&str]) -> String {
    let output = run(args);
    assert!(
        output.status.success(),
        "{args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("utf8 stdout")
}

fn descriptor(block: &str) -> BTreeMap<&str, &str> {
    block
        .lines()
        .take_while(|line| !line.is_empty())
        .map(|line| line.split_once(": ").expect("descriptor key/value"))
        .collect()
}

fn listed_descriptors(output: &str) -> Vec<BTreeMap<&str, &str>> {
    output.split("\n\n").map(descriptor).collect()
}

#[test]
fn list_and_code_share_every_registry_descriptor_in_stable_order() {
    let list = success(&["explain", "--list"]);
    let listed = listed_descriptors(&list);
    let mut expected: Vec<_> = REGISTRY.iter().collect();
    expected.sort_unstable_by_key(|entry| entry.code);

    assert_eq!(listed.len(), expected.len());
    for (actual, entry) in listed.iter().zip(expected) {
        assert_eq!(actual["Code"], entry.code);
        let severity = match entry.severity {
            clinker_core_types::Severity::Error => "error",
            clinker_core_types::Severity::Warning => "warning",
            clinker_core_types::Severity::Note => "note",
        };
        assert_eq!(actual["Severity"], severity);
        assert_eq!(actual["Status"], entry.lifecycle.as_str());
        assert_eq!(actual["Category"], entry.category.as_str());
        assert_eq!(
            actual["Retryability"],
            entry.retry_advice.as_str().replace('_', "-")
        );
        assert!(
            ["Status", "Category", "Retryability"]
                .iter()
                .all(|field| !actual[*field].contains('_')),
            "descriptor enum values must use one kebab-case convention"
        );
        assert_eq!(actual["Meaning"], entry.meaning);
        assert_eq!(actual["Correction"], entry.correction);

        let code = success(&["explain", "--code", entry.code]);
        assert_eq!(
            descriptor(&code),
            *actual,
            "{} descriptor drifted",
            entry.code
        );
    }
    assert!(
        listed
            .windows(2)
            .all(|pair| pair[0]["Code"] < pair[1]["Code"]),
        "list order must be stable by code"
    );
}

#[test]
fn filters_are_exact_and_include_the_retired_reservation() {
    let retired = success(&["explain", "--list", "--status", "retired-reserved"]);
    let entries = listed_descriptors(&retired);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["Code"], "E376");
    assert_eq!(entries[0]["Category"], "terminal-authoring");
    assert_eq!(entries[0]["Correction"], "type: sink");

    let warnings = success(&["explain", "--list", "--category", "advisory"]);
    assert!(
        listed_descriptors(&warnings)
            .iter()
            .all(|entry| entry["Severity"] == "warning")
    );

    let unknown_status = run(&["explain", "--list", "--status", "unknown"]);
    let status_error = String::from_utf8_lossy(&unknown_status.stderr);
    let statuses = DiagnosticLifecycle::ALL
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    assert!(status_error.contains(&format!("Valid statuses: {statuses}")));

    let unknown_category = run(&["explain", "--list", "--category", "unknown"]);
    let category_error = String::from_utf8_lossy(&unknown_category.stderr);
    let categories = DiagnosticCategory::ALL
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    assert!(category_error.contains(&format!("Valid categories: {categories}")));

    for args in [
        &["explain", "--list", "--status", ""] as &[&str],
        &["explain", "--list", "--category", "unknown"],
        &[
            "explain",
            "--list",
            "--status",
            "retired-reserved",
            "--category",
            "configuration",
        ],
    ] {
        let output = run(args);
        assert!(!output.status.success(), "{args:?} must fail");
    }
}

#[test]
fn unknown_codes_and_conflicting_or_absent_modes_fail() {
    let unknown = run(&["explain", "--code", "E999"]);
    assert!(!unknown.status.success());
    let error = String::from_utf8_lossy(&unknown.stderr);
    assert!(error.contains("unknown diagnostic code 'E999'"));
    assert!(
        error.contains("E376"),
        "valid set comes from the leaf registry"
    );

    for args in [
        &["explain"] as &[&str],
        &["explain", "pipeline.yaml"],
        &["explain", "--list", "--code", "E010"],
        &["explain", "pipeline.yaml", "--list"],
        &["explain", "pipeline.yaml", "--code", "E010"],
        &["explain", "--status", "active", "--code", "E010"],
        &["explain", "--field", "node.param"],
    ] {
        let output = run(args);
        assert!(!output.status.success(), "{args:?} must fail");
    }
}

#[test]
fn registered_code_without_detail_page_still_succeeds() {
    let output = success(&["explain", "--code", "E376"]);
    assert!(output.contains("Code: E376"));
    assert!(output.contains("Detail page: none"));
    assert!(!output.contains("unknown diagnostic"));
}

#[test]
fn e377_list_code_and_detail_page_are_in_parity() {
    let list = success(&["explain", "--list", "--category", "configuration"]);
    let listed = listed_descriptors(&list);
    let entry = listed
        .iter()
        .find(|entry| entry["Code"] == "E377")
        .expect("E377 appears in configuration list");
    assert_eq!(entry["Status"], "active");
    assert_eq!(entry["Severity"], "error");
    assert_eq!(entry["Retryability"], "do-not-retry");

    let code = success(&["explain", "--code", "E377"]);
    assert_eq!(descriptor(&code), *entry);
    assert!(code.contains("Detail page:"));
    assert!(code.contains("ordinary composition call"));
    assert!(code.contains("_compose.outputs"));
}
