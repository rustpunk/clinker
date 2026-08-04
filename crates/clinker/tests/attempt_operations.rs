use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use clinker_exec::output::attempt::{AttemptFault, AttemptPublication};
use clinker_exec::output::containment::PromotionDisposition;
use clinker_exec::output::staging::OutputStagingRegistry;
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_plan::security::{ValidatedPath, validate_path};

const EXECUTION_ID: &str = "018f47a2-9a41-7a27-b4d6-4f7137e3c159";

const PIPELINE: &str = r#"pipeline:
  name: attempt_operations
nodes:
  - type: source
    name: source
    config:
      name: source
      type: csv
      path: input.csv
      schema:
        - { name: value, type: string }
  - type: output
    name: result
    input: source
    config:
      name: result
      type: csv
      path: output/result.csv
"#;

fn clinker(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .args(args)
        .output()
        .expect("clinker process should start")
}

fn clinker_in(workspace: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .args(args)
        .current_dir(workspace)
        .output()
        .expect("clinker process should start")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn validated(root: &Path, relative: &str) -> ValidatedPath {
    validate_path(Path::new(relative), root, false).expect("fixture path should validate")
}

fn write_workspace(publication: &str) -> (tempfile::TempDir, PathBuf) {
    let workspace = tempfile::tempdir().expect("temporary workspace");
    std::fs::create_dir(workspace.path().join("output")).expect("output directory");
    std::fs::write(workspace.path().join("pipeline.yaml"), PIPELINE).expect("pipeline fixture");
    std::fs::write(workspace.path().join("input.csv"), "value\nexample\n").expect("input fixture");
    std::fs::write(
        workspace.path().join("clinker.toml"),
        format!(
            "[storage.publication]\nfailed_retention_seconds = 0\ncreation_grace_seconds = 1\nmin_free_bytes = \"1B\"\n{publication}"
        ),
    )
    .expect("workspace config");
    let output_root = workspace.path().join("output");
    (workspace, output_root)
}

fn seed_incomplete_attempt(output_root: &Path, execution_id: &str) {
    let registry = OutputStagingRegistry::default();
    let mut attempt =
        AttemptPublication::create(validated(output_root, "."), execution_id, 1_000, 2_000)
            .expect("create retained attempt");
    let (artifact_id, mut writer) = attempt
        .stage_direct(
            &registry,
            validated(output_root, "result.csv"),
            "result",
            "result.csv",
            PromotionDisposition::Replace,
        )
        .expect("stage retained artifact");
    writer
        .write_all(b"value\nexample\n")
        .expect("write artifact");
    drop(writer);
    attempt
        .mark_ready(&artifact_id)
        .expect("mark artifact ready");
    attempt.set_fault_for_testing(AttemptFault::BeforeRename);
    let outcome = attempt
        .publish(&registry, &ShutdownToken::new())
        .expect("publish attempt")
        .expect("publication should reach its gate");
    assert!(!outcome.is_complete(), "fixture must remain retained");
}

#[test]
fn attempts_help_exposes_path_safe_operator_commands() {
    let output = clinker(&["attempts", "--help"]);

    assert!(output.status.success(), "{}", stderr(&output));
    let help = stdout(&output);
    assert!(help.contains("list"), "{help}");
    assert!(help.contains("inspect"), "{help}");
    assert!(help.contains("purge"), "{help}");
}

#[test]
fn purge_requires_exactly_one_selector() {
    let missing = clinker(&["attempts", "purge", "pipeline.yaml"]);
    assert_eq!(missing.status.code(), Some(1), "{}", stderr(&missing));

    let ambiguous = clinker(&[
        "attempts",
        "purge",
        "pipeline.yaml",
        "--execution-id",
        "00000000-0000-4000-8000-000000000001",
        "--expired",
    ]);
    assert_eq!(ambiguous.status.code(), Some(1), "{}", stderr(&ambiguous));
}

#[test]
fn purge_help_makes_preview_the_default_and_execution_explicit() {
    let output = clinker(&["attempts", "purge", "--help"]);

    assert!(output.status.success(), "{}", stderr(&output));
    let help = stdout(&output);
    assert!(help.contains("--execute"), "{help}");
    assert!(!help.contains("--root"), "{help}");
    assert!(!help.contains("--path"), "{help}");
    assert!(!help.contains("--force"), "{help}");
}

#[test]
fn list_and_inspect_compile_the_pipeline_and_hide_physical_paths_by_default() {
    let (workspace, output_root) = write_workspace("");
    seed_incomplete_attempt(&output_root, EXECUTION_ID);

    let list = clinker_in(
        workspace.path(),
        &["attempts", "list", "pipeline.yaml", "--format", "json"],
    );
    assert!(list.status.success(), "{}", stderr(&list));
    let list_text = stdout(&list);
    let list_json: serde_json::Value = serde_json::from_str(&list_text).expect("compact JSON");
    assert_eq!(list_json["operation"], "list");
    assert_eq!(list_json["pipeline"], "pipeline.yaml");
    assert!(list_text.contains(EXECUTION_ID), "{list_text}");
    assert!(!list_text.contains(&workspace.path().display().to_string()));
    assert!(!list_text.contains(".clinker-attempts"));

    let inspect = clinker_in(
        workspace.path(),
        &[
            "attempts",
            "inspect",
            "pipeline.yaml",
            "--execution-id",
            EXECUTION_ID,
            "--format",
            "json",
        ],
    );
    assert!(inspect.status.success(), "{}", stderr(&inspect));
    let inspect_text = stdout(&inspect);
    assert!(inspect_text.contains("\"state\":\"incomplete\""));
    assert!(inspect_text.contains("artifact-00000001"));
    assert!(!inspect_text.contains(&workspace.path().display().to_string()));
    assert!(!inspect_text.contains(".clinker-attempts"));
}

#[test]
fn show_paths_is_workspace_relative_and_redacts_sensitive_components() {
    let (workspace, original_root) = write_workspace("");
    let sensitive_root = workspace.path().join("secret-token-output");
    std::fs::rename(&original_root, &sensitive_root).expect("rename output fixture");
    let pipeline = PIPELINE.replace("output/result.csv", "secret-token-output/result.csv");
    std::fs::write(workspace.path().join("pipeline.yaml"), pipeline).expect("rewrite pipeline");
    seed_incomplete_attempt(&sensitive_root, EXECUTION_ID);

    let inspect = clinker_in(
        workspace.path(),
        &[
            "attempts",
            "inspect",
            "pipeline.yaml",
            "--execution-id",
            EXECUTION_ID,
            "--show-paths",
            "--format",
            "json",
        ],
    );
    assert!(inspect.status.success(), "{}", stderr(&inspect));
    let text = stdout(&inspect);
    assert!(text.contains("<redacted>/.clinker-attempts"), "{text}");
    assert!(!text.contains("secret-token-output"), "{text}");
    assert!(!text.contains(&workspace.path().display().to_string()));
}

#[test]
fn purge_previews_without_mutation_and_executes_only_with_execute() {
    let (workspace, output_root) = write_workspace("");
    seed_incomplete_attempt(&output_root, EXECUTION_ID);
    let attempt_root = output_root.join(".clinker-attempts").join(EXECUTION_ID);

    let preview = clinker_in(
        workspace.path(),
        &[
            "attempts",
            "purge",
            "pipeline.yaml",
            "--execution-id",
            EXECUTION_ID,
            "--format",
            "json",
        ],
    );
    assert!(preview.status.success(), "{}", stderr(&preview));
    let preview_text = stdout(&preview);
    assert!(preview_text.contains("\"mode\":\"preview\""));
    assert!(preview_text.contains(EXECUTION_ID));
    assert!(
        attempt_root.exists(),
        "preview must not mutate retained state"
    );

    let execute = clinker_in(
        workspace.path(),
        &[
            "attempts",
            "purge",
            "pipeline.yaml",
            "--execution-id",
            EXECUTION_ID,
            "--execute",
            "--format",
            "json",
        ],
    );
    assert!(execute.status.success(), "{}", stderr(&execute));
    let execute_text = stdout(&execute);
    assert!(execute_text.contains("\"mode\":\"execute\""));
    assert!(execute_text.contains("\"disposition\":\"removed\""));
    assert!(
        !attempt_root.exists(),
        "--execute should remove owned state"
    );
}

#[test]
fn invalid_manifest_refusal_uses_typed_e371_recovery_data() {
    let (workspace, output_root) = write_workspace("");
    seed_incomplete_attempt(&output_root, EXECUTION_ID);
    let manifest = output_root
        .join(".clinker-attempts")
        .join(EXECUTION_ID)
        .join("manifest.json");
    std::fs::write(manifest, b"not-json").expect("corrupt manifest fixture");

    let inspect = clinker_in(
        workspace.path(),
        &[
            "attempts",
            "inspect",
            "pipeline.yaml",
            "--execution-id",
            EXECUTION_ID,
            "--format",
            "json",
        ],
    );
    assert_eq!(inspect.status.code(), Some(4), "{}", stderr(&inspect));
    let text = stdout(&inspect);
    assert!(text.contains("\"diagnostic_code\":\"E371\""), "{text}");
    assert!(
        text.contains("attempt.retention.manifest_invalid"),
        "{text}"
    );
    assert!(text.contains("\"retry_advice\":\"policy_required\""));
    assert!(text.contains(&format!(
        "clinker attempts inspect pipeline.yaml --execution-id {EXECUTION_ID}"
    )));
    assert!(!text.contains(&workspace.path().display().to_string()));
}

#[test]
fn bounded_inspection_uses_typed_e372_and_exit_four() {
    let (workspace, output_root) = write_workspace("sweep_entry_limit = 1\n");
    seed_incomplete_attempt(&output_root, EXECUTION_ID);

    let inspect = clinker_in(
        workspace.path(),
        &[
            "attempts",
            "inspect",
            "pipeline.yaml",
            "--execution-id",
            EXECUTION_ID,
            "--format",
            "json",
        ],
    );
    assert_eq!(inspect.status.code(), Some(4), "{}", stderr(&inspect));
    let text = stdout(&inspect);
    assert!(text.contains("\"diagnostic_code\":\"E372\""), "{text}");
    assert!(
        text.contains("attempt.retention.budget_exhausted"),
        "{text}"
    );
    assert!(text.contains("\"retry_advice\":\"retry_with_backoff\""));
}

#[test]
fn continuation_output_preserves_the_exact_selector_and_execution_mode() {
    let (workspace, output_root) = write_workspace("sweep_entry_limit = 1\n");
    seed_incomplete_attempt(&output_root, EXECUTION_ID);

    let purge = clinker_in(
        workspace.path(),
        &[
            "attempts",
            "purge",
            "pipeline.yaml",
            "--expired",
            "--execute",
            "--format",
            "json",
        ],
    );
    assert_eq!(purge.status.code(), Some(4), "{}", stderr(&purge));
    let value: serde_json::Value = serde_json::from_slice(&purge.stdout).expect("compact JSON");
    let root = &value["roots"][0];
    let continuation = root["continuation"]
        .as_str()
        .expect("bounded purge should emit a continuation");
    let resume = root["resume_command"]
        .as_str()
        .expect("bounded purge should emit a resume command");
    assert!(resume.contains("attempts purge pipeline.yaml --expired --execute"));
    assert!(resume.ends_with(&format!("--continuation '{continuation}'")));
}

#[test]
fn live_attempt_is_kept_with_e371_and_no_override_surface() {
    let (workspace, output_root) = write_workspace("");
    let live = AttemptPublication::create(validated(&output_root, "."), EXECUTION_ID, 1_000, 2_000)
        .expect("create live attempt");

    let purge = clinker_in(
        workspace.path(),
        &[
            "attempts",
            "purge",
            "pipeline.yaml",
            "--execution-id",
            EXECUTION_ID,
            "--execute",
            "--format",
            "json",
        ],
    );
    assert_eq!(purge.status.code(), Some(4), "{}", stderr(&purge));
    let text = stdout(&purge);
    assert!(text.contains("\"diagnostic_code\":\"E371\""), "{text}");
    assert!(text.contains("attempt.retention.live"), "{text}");
    assert!(live.attempt_root().exists(), "live attempt must be kept");
    drop(live);
}

#[test]
fn invalid_pipeline_and_continuation_arguments_exit_one() {
    let absolute = clinker(&["attempts", "list", "/tmp/not-workspace-relative.yaml"]);
    assert_eq!(absolute.status.code(), Some(1), "{}", stderr(&absolute));

    let (workspace, _output_root) = write_workspace("");
    let continuation = clinker_in(
        workspace.path(),
        &[
            "attempts",
            "list",
            "pipeline.yaml",
            "--continuation",
            "not-json",
        ],
    );
    assert_eq!(
        continuation.status.code(),
        Some(1),
        "{}",
        stderr(&continuation)
    );
}

#[test]
fn list_before_the_first_run_treats_a_missing_local_destination_as_empty() {
    let (workspace, output_root) = write_workspace("");
    std::fs::remove_dir(output_root).expect("remove unused destination root");

    let list = clinker_in(
        workspace.path(),
        &["attempts", "list", "pipeline.yaml", "--format", "json"],
    );
    assert!(list.status.success(), "{}", stderr(&list));
    let value: serde_json::Value = serde_json::from_slice(&list.stdout).expect("compact JSON");
    assert_eq!(value["roots"], serde_json::json!([]));
}
