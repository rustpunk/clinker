use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{Value, json};
use tempfile::TempDir;

const MARKER: &str = "<!-- clinker-phase3-recovery-receipt:v1 -->";

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("detached manifest must be beneath repository root")
        .to_path_buf()
}

fn commands() -> Value {
    json!([
        {"id":"CMD-01","argv":["cargo","test","--locked","--offline","-p","clinker-plan","semantic_fingerprint"],"status":"PASS"},
        {"id":"CMD-02","argv":["cargo","test","--locked","--offline","-p","clinker-core-types","--test","failure_classification"],"status":"PASS"},
        {"id":"CMD-03","argv":["cargo","test","--locked","--offline","-p","clinker","--test","machine_protocol_cli"],"status":"PASS"},
        {"id":"CMD-04","argv":["cargo","test","--locked","--offline","-p","clinker","--test","machine_supervision"],"status":"PASS"},
        {"id":"CMD-05","argv":["cargo","test","--locked","--offline","-p","clinker","--test","attempt_publication"],"status":"PASS"},
        {"id":"CMD-06","argv":["cargo","run","--locked","--offline","-p","clinker","--","run","--help"],"status":"PASS"},
        {"id":"CMD-07","argv":["cargo","test","--locked","-p","clinker-exec","--features","test-utils","--test","invariant_errors","--","--nocapture"],"status":"PASS"},
        {"id":"CMD-08","argv":["cargo","test","--locked","-p","clinker-plan","--test","observability_config"],"status":"PASS"},
        {"id":"CMD-09","argv":["cargo","test","--locked","-p","clinker-plan","--test","transform_observability"],"status":"PASS"},
        {"id":"CMD-10","argv":["cargo","test","--locked","-p","clinker-net","--test","otlp_http"],"status":"PASS"},
        {"id":"CMD-11","argv":["cargo","test","--locked","-p","clinker-lineage","--test","logical_identity"],"status":"PASS"},
        {"id":"CMD-12","argv":["cargo","test","--locked","-p","clinker-lineage","--test","lifecycle_delivery"],"status":"PASS"},
        {"id":"CMD-13","argv":["cargo","test","--locked","-p","clinker-exec","--test","observability_isolation"],"status":"PASS"},
        {"id":"CMD-14","argv":["cargo","test","--locked","-p","clinker","--test","lineage_cli"],"status":"PASS"},
        {"id":"CMD-15","argv":["cargo","test","--locked","-p","clinker","--test","observability_isolation"],"status":"PASS"},
        {"id":"CMD-16","argv":["cargo","run","--manifest-path","tools/dependency-policy/Cargo.toml","--locked","--offline","--","--scope","final","--root","."],"status":"PASS"},
        {"id":"CMD-17","argv":["bash","scripts/check-ai-docs.sh"],"status":"PASS"},
        {"id":"CMD-18","argv":["mdbook","build","docs/user"],"status":"PASS"},
        {"id":"CMD-19","argv":["mdbook","build","docs/engine"],"status":"PASS"},
        {"id":"CMD-20","argv":["cargo","fmt","--all","--check"],"status":"PASS"},
        {"id":"CMD-21","argv":["git","diff","--check"],"status":"PASS"}
    ])
}

fn checks() -> Value {
    json!([
        {"id":"CHECK-01","evidence_ids":["CMD-07"],"status":"PASS"},
        {"id":"CHECK-02","evidence_ids":["VALIDATION-D22"],"status":"PASS"},
        {"id":"CHECK-03","evidence_ids":["CMD-04","VALIDATION-D26"],"status":"PASS"},
        {"id":"CHECK-04","evidence_ids":["CMD-05","VALIDATION-D33"],"status":"PASS"},
        {"id":"CHECK-05","evidence_ids":["CMD-08","CMD-09","CMD-10","CMD-13","CMD-15"],"status":"PASS"},
        {"id":"CHECK-06","evidence_ids":["CMD-12","CMD-13","CMD-15"],"status":"PASS"},
        {"id":"CHECK-07","evidence_ids":["CMD-11","CMD-14"],"status":"PASS"},
        {"id":"CHECK-08","evidence_ids":["CMD-05","CMD-07"],"status":"PASS"},
        {"id":"CHECK-09","evidence_ids":["CMD-02","CMD-07","CMD-16"],"status":"PASS"},
        {"id":"CHECK-10","evidence_ids":["VALIDATION-OWNERSHIP","VALIDATION-WAVES"],"status":"PASS"}
    ])
}

fn prohibitions() -> Value {
    json!([
        {"id":"PROHIB-01","source_ids":["MACHINE-PROHIB-01"],"tier":"judgment","command_ids":["CMD-16"],"reviewed_paths":["Cargo.toml","crates/clinker/Cargo.toml","crates/clinker/src/main.rs","docs/ai/10_ARCHITECTURE.md"],"disposition":"no-product-orchestrator-runtime","status":"PASS"},
        {"id":"PROHIB-02","source_ids":["MACHINE-PROHIB-02"],"tier":"test","command_ids":["CMD-03"],"reviewed_paths":[],"disposition":"","status":"PASS"},
        {"id":"PROHIB-03","source_ids":["MACHINE-PROHIB-03","SUPERVISION-PROHIB-02"],"tier":"test","command_ids":["CMD-02","CMD-03","CMD-04"],"reviewed_paths":[],"disposition":"","status":"PASS"},
        {"id":"PROHIB-04","source_ids":["MACHINE-PROHIB-04"],"tier":"test","command_ids":["CMD-03"],"reviewed_paths":[],"disposition":"","status":"PASS"},
        {"id":"PROHIB-05","source_ids":["MACHINE-PROHIB-05"],"tier":"test","command_ids":["CMD-03","CMD-05"],"reviewed_paths":[],"disposition":"","status":"PASS"},
        {"id":"PROHIB-06","source_ids":["SUPERVISION-PROHIB-01"],"tier":"judgment","command_ids":[],"reviewed_paths":["crates/clinker/src/main.rs","crates/clinker/tests/support/process.rs","crates/clinker/tests/machine_supervision.rs"],"disposition":"process-launch-test-only","status":"PASS"},
        {"id":"PROHIB-07","source_ids":["SUPERVISION-PROHIB-03"],"tier":"test","command_ids":["CMD-03","CMD-06"],"reviewed_paths":[],"disposition":"","status":"PASS"},
        {"id":"PROHIB-08","source_ids":["SUPERVISION-PROHIB-04"],"tier":"test","command_ids":["CMD-04","CMD-05"],"reviewed_paths":[],"disposition":"","status":"PASS"}
    ])
}

fn complete_receipt() -> Value {
    json!({
        "schema": "clinker.phase3-recovery-receipt/v1",
        "plan": "03-51",
        "status": "PASS",
        "commands": commands(),
        "checks": checks(),
        "prohibitions": prohibitions(),
        "dependency_closure": {
            "plans": (36..=50).map(|plan| format!("03-{plan}")).collect::<Vec<_>>(),
            "status": "PASS"
        }
    })
}

fn summary(receipt: &Value) -> String {
    format!(
        "# Recovery summary\n\n{MARKER}\n```json\n{}\n```\n",
        serde_json::to_string_pretty(receipt).expect("serialize receipt")
    )
}

fn run_contents(contents: &str) -> Output {
    let directory = tempfile::tempdir().expect("temporary receipt directory");
    let path = directory.path().join("03-51-SUMMARY.md");
    fs::write(&path, contents).expect("write receipt summary");
    run_path(&path, Some(&directory))
}

fn run_path(path: &Path, _directory: Option<&TempDir>) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
        .current_dir(repository_root())
        .args(["recovery", "validate-receipt", "--summary"])
        .arg(path)
        .output()
        .expect("clinker-release-policy must execute")
}

fn assert_rejected(contents: &str) {
    let output = run_contents(contents);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.len() <= 576, "diagnostic must remain bounded");
}

#[test]
fn exact_complete_receipt_is_accepted() {
    let output = run_contents(&summary(&complete_receipt()));
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        output.stdout,
        b"Phase 3 recovery receipt validation passed\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn missing_summary_and_missing_receipt_are_rejected_without_path_disclosure() {
    let directory = tempfile::tempdir().expect("temporary receipt directory");
    let missing = directory.path().join("private-summary-name.md");
    let output = run_path(&missing, Some(&directory));
    assert!(!output.status.success());
    let diagnostic = String::from_utf8(output.stderr).expect("UTF-8 diagnostic");
    assert!(!diagnostic.contains("private-summary-name"));
    assert_rejected("# Summary without a receipt\n");
}

#[test]
fn duplicate_nested_trailing_and_ambiguous_receipt_blocks_are_rejected() {
    let valid = summary(&complete_receipt());
    assert_rejected(&format!("{valid}\n{valid}"));
    assert_rejected(&valid.replace("```json\n", "```json\n```json\n"));
    assert_rejected(&valid.replace("\n```\n", "\n{}\n```\n"));
    assert_rejected(&valid.replace(MARKER, &format!("{MARKER}\nprose")));
}

#[test]
fn missing_duplicate_extra_and_reordered_command_rows_are_rejected() {
    let mut missing = complete_receipt();
    missing["commands"]
        .as_array_mut()
        .expect("commands")
        .remove(0);
    assert_rejected(&summary(&missing));

    let mut duplicate = complete_receipt();
    let first = duplicate["commands"][0].clone();
    duplicate["commands"]
        .as_array_mut()
        .expect("commands")
        .push(first);
    assert_rejected(&summary(&duplicate));

    let mut extra = complete_receipt();
    extra["commands"]
        .as_array_mut()
        .expect("commands")
        .push(json!({
            "id":"CMD-22","argv":["cargo","test"],"status":"PASS"
        }));
    assert_rejected(&summary(&extra));

    let mut reordered = complete_receipt();
    reordered["commands"]
        .as_array_mut()
        .expect("commands")
        .swap(0, 1);
    assert_rejected(&summary(&reordered));
}

#[test]
fn changed_argv_empty_token_and_non_pass_status_are_rejected() {
    let mut changed = complete_receipt();
    changed["commands"][0]["argv"][1] = json!("check");
    assert_rejected(&summary(&changed));

    let mut empty = complete_receipt();
    empty["commands"][0]["argv"][0] = json!("");
    assert_rejected(&summary(&empty));

    let mut failed = complete_receipt();
    failed["checks"][0]["status"] = json!("SKIPPED");
    assert_rejected(&summary(&failed));
}

#[test]
fn duplicate_json_keys_and_unknown_fields_at_each_level_are_rejected() {
    let encoded = serde_json::to_string(&complete_receipt()).expect("serialize receipt");
    let duplicated = encoded.replacen(
        '{',
        "{\"schema\":\"clinker.phase3-recovery-receipt/v1\",",
        1,
    );
    assert_rejected(&format!("{MARKER}\n```json\n{duplicated}\n```\n"));

    for pointer in [
        "/",
        "/commands/0",
        "/checks/0",
        "/prohibitions/0",
        "/dependency_closure",
    ] {
        let mut receipt = complete_receipt();
        let object = if pointer == "/" {
            receipt.as_object_mut().expect("top-level object")
        } else {
            receipt
                .pointer_mut(pointer)
                .and_then(Value::as_object_mut)
                .expect("nested object")
        };
        object.insert("unknown".to_owned(), json!(true));
        assert_rejected(&summary(&receipt));
    }
}

#[test]
fn wrong_check_and_prohibition_evidence_tuples_are_rejected() {
    let mut check = complete_receipt();
    check["checks"][4]["evidence_ids"] = json!(["CMD-08"]);
    assert_rejected(&summary(&check));

    let mut tier = complete_receipt();
    tier["prohibitions"][0]["tier"] = json!("test");
    assert_rejected(&summary(&tier));

    let mut command = complete_receipt();
    command["prohibitions"][2]["command_ids"] = json!(["CMD-03"]);
    assert_rejected(&summary(&command));
}

#[test]
fn empty_or_invalid_judgment_paths_and_dispositions_are_rejected() {
    let mut empty_paths = complete_receipt();
    empty_paths["prohibitions"][0]["reviewed_paths"] = json!([]);
    assert_rejected(&summary(&empty_paths));

    let mut empty_disposition = complete_receipt();
    empty_disposition["prohibitions"][0]["disposition"] = json!("");
    assert_rejected(&summary(&empty_disposition));

    let mut absolute = complete_receipt();
    absolute["prohibitions"][0]["reviewed_paths"][0] = json!("/private/source.rs");
    assert_rejected(&summary(&absolute));

    let mut missing = complete_receipt();
    missing["prohibitions"][0]["reviewed_paths"][0] = json!("missing/source.rs");
    assert_rejected(&summary(&missing));
}

#[test]
fn missing_or_duplicated_source_prohibition_ids_are_rejected() {
    let mut missing = complete_receipt();
    missing["prohibitions"][2]["source_ids"] = json!(["MACHINE-PROHIB-03"]);
    assert_rejected(&summary(&missing));

    let mut duplicate = complete_receipt();
    duplicate["prohibitions"][3]["source_ids"] = json!(["MACHINE-PROHIB-02"]);
    assert_rejected(&summary(&duplicate));
}

#[test]
fn incomplete_reordered_duplicated_and_extra_dependency_closure_is_rejected() {
    let mut incomplete = complete_receipt();
    incomplete["dependency_closure"]["plans"]
        .as_array_mut()
        .expect("plans")
        .pop();
    assert_rejected(&summary(&incomplete));

    let mut reordered = complete_receipt();
    reordered["dependency_closure"]["plans"]
        .as_array_mut()
        .expect("plans")
        .swap(0, 1);
    assert_rejected(&summary(&reordered));

    let mut duplicate = complete_receipt();
    duplicate["dependency_closure"]["plans"]
        .as_array_mut()
        .expect("plans")
        .push(json!("03-50"));
    assert_rejected(&summary(&duplicate));

    let mut extra = complete_receipt();
    extra["dependency_closure"]["plans"]
        .as_array_mut()
        .expect("plans")
        .push(json!("03-51"));
    assert_rejected(&summary(&extra));
}

#[test]
fn empty_oversized_and_overlong_receipt_values_are_rejected() {
    let mut non_string = complete_receipt();
    non_string["plan"] = json!(51);
    assert_rejected(&summary(&non_string));

    let mut empty = complete_receipt();
    empty["checks"][0]["evidence_ids"] = json!([]);
    assert_rejected(&summary(&empty));

    let mut long = complete_receipt();
    long["commands"][0]["id"] = json!("X".repeat(1025));
    assert_rejected(&summary(&long));

    let oversized = format!(
        "# Summary\n\n{MARKER}\n```json\n{}\n```\n",
        " ".repeat(1_048_576)
    );
    assert_rejected(&oversized);
}
