use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("detached manifest must be beneath repository root")
        .to_path_buf()
}

fn gate(root: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
        .current_dir(root)
        .args(arguments)
        .output()
        .expect("clinker-release-policy must execute")
}

fn fixture() -> TempDir {
    let root = tempfile::tempdir().expect("temporary repository");
    fs::create_dir_all(root.path().join(".github")).expect("repository configuration directory");
    fs::write(
        root.path().join(".github/CODEOWNERS"),
        r#"/.github/dependabot.yml @rustpunk
/.github/workflows/** @rustpunk
/scripts/release/** @rustpunk
/release/** @rustpunk
/rust-toolchain.toml @rustpunk
/tools/dependency-policy/** @rustpunk
/tools/release-policy/** @rustpunk
/Cargo.toml @rustpunk
/Cargo.lock @rustpunk
/LICENSE @rustpunk
"#,
    )
    .expect("CODEOWNERS fixture");
    fs::write(
        root.path().join(".github/dependabot.yml"),
        r#"version: 2
updates:
  - package-ecosystem: github-actions
    directory: /
    schedule:
      interval: weekly
      day: monday
      time: "06:00"
      timezone: Etc/UTC
    open-pull-requests-limit: 5
"#,
    )
    .expect("Dependabot fixture");
    root
}

#[test]
fn exact_local_repository_verify_argv_accepts_governed_configuration() {
    let output = gate(
        &repository_root(),
        &["repository", "verify", "--config-only"],
    );
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        output.stdout,
        b"Release repository configuration verification passed\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn ordered_codeowners_rules_reject_shadowing_and_uncovered_critical_paths() {
    let root = fixture();
    let codeowners = root.path().join(".github/CODEOWNERS");

    let valid = fs::read_to_string(&codeowners).expect("read CODEOWNERS fixture");
    fs::write(&codeowners, format!("{valid}* @unreviewed\n")).expect("shadowed CODEOWNERS fixture");
    let shadowed = gate(root.path(), &["repository", "verify", "--config-only"]);
    assert_eq!(shadowed.status.code(), Some(1));
    assert!(shadowed.stdout.is_empty());
    assert!(!shadowed.stderr.is_empty());

    fs::write(
        &codeowners,
        valid.replace("/scripts/release/** @rustpunk\n", ""),
    )
    .expect("uncovered CODEOWNERS fixture");
    let uncovered = gate(root.path(), &["repository", "verify", "--config-only"]);
    assert_eq!(uncovered.status.code(), Some(1));
    assert!(uncovered.stdout.is_empty());
    assert!(!uncovered.stderr.is_empty());
}

#[test]
fn typed_dependabot_contract_rejects_unknown_duplicate_or_drifted_updates() {
    let root = fixture();
    let dependabot = root.path().join(".github/dependabot.yml");
    let valid = fs::read_to_string(&dependabot).expect("read Dependabot fixture");
    for mutation in [
        valid.replace("version: 2", "version: 2\nunreviewed: true"),
        valid.replace(
            "    directory: /",
            "    directory: /\n    schedule:\n      interval: daily",
        ),
        valid.replace("      day: monday", "      day: tuesday"),
        format!(
            "{valid}  - package-ecosystem: cargo\n    directory: /\n    schedule:\n      interval: weekly\n"
        ),
    ] {
        fs::write(&dependabot, mutation).expect("negative Dependabot fixture");
        let output = gate(root.path(), &["repository", "verify", "--config-only"]);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[test]
fn repository_verify_rejects_conflicting_partial_or_repeated_authority_flags() {
    let root = fixture();
    for arguments in [
        vec![
            "repository",
            "verify",
            "--config-only",
            "--repo",
            "rustpunk/clinker",
        ],
        vec![
            "repository",
            "verify",
            "--repo",
            "rustpunk/clinker",
            "--apply-approved",
            "rules.json",
        ],
        vec![
            "repository",
            "verify",
            "--repo",
            "rustpunk/clinker",
            "--repo",
            "other/repo",
        ],
    ] {
        let output = gate(root.path(), &arguments);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[test]
fn exact_authenticated_apply_argv_records_complete_immediate_readback() {
    let root = fixture();
    copy_decisions(root.path());
    install_fake_gh(root.path(), approved_rulesets(), approved_environment());
    let evidence = root.path().join("repository-controls-evidence.json");
    let output = authenticated_gate(root.path(), &evidence);
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        output.stdout,
        b"Release repository controls applied and verified\n"
    );
    assert!(output.stderr.is_empty());

    let evidence: Value = serde_json::from_slice(
        &fs::read(evidence).expect("repository controls evidence must be written"),
    )
    .expect("repository controls evidence must be JSON");
    assert_eq!(
        evidence["schema"],
        "clinker.repository-controls-evidence/v1"
    );
    assert_eq!(evidence["repository"], "rustpunk/clinker");
    assert_eq!(evidence["release_status"], "incomplete");
    assert_eq!(evidence["completion_eligible"], false);
    assert_eq!(evidence["readback"]["immutable_releases"]["enabled"], true);
    assert_eq!(evidence["readback"]["environment"]["name"], "release");
    assert_eq!(
        evidence["readback"]["rulesets"].as_array().map(Vec::len),
        Some(2)
    );
}

#[test]
fn authenticated_apply_rejects_partial_or_authority_widening_readback() {
    let root = fixture();
    copy_decisions(root.path());
    install_fake_gh(root.path(), broadened_rulesets(), approved_environment());
    let evidence = root.path().join("repository-controls-evidence.json");
    let output = authenticated_gate(root.path(), &evidence);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
    assert!(!evidence.exists());
}

#[test]
fn authenticated_apply_rejects_decisions_without_independent_policy_jobs() {
    for missing_context in ["Dependency policy", "Release policy"] {
        let root = fixture();
        copy_decisions(root.path());
        let path = root.path().join("decisions/release-rules.json");
        let mut decision: Value =
            serde_json::from_slice(&fs::read(&path).expect("read release rules fixture"))
                .expect("release rules fixture JSON");
        decision["ruleset"]["main_rule"]["required_status_checks"]
            .as_array_mut()
            .expect("required status checks array")
            .retain(|check| check["context"] != missing_context);
        fs::write(
            &path,
            serde_json::to_vec_pretty(&decision).expect("serialize release rules fixture"),
        )
        .expect("write release rules fixture");
        install_fake_gh(root.path(), approved_rulesets(), approved_environment());

        let evidence = root.path().join("repository-controls-evidence.json");
        let output = authenticated_gate(root.path(), &evidence);
        assert_eq!(
            output.status.code(),
            Some(1),
            "missing context: {missing_context}"
        );
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        assert!(!evidence.exists());
    }
}

#[test]
fn two_person_environment_mode_uses_exact_reviewers_and_prevents_self_review() {
    let root = fixture();
    copy_decisions(root.path());
    let path = root.path().join("decisions/release-environment.json");
    let mut decision: Value =
        serde_json::from_slice(&fs::read(&path).expect("read environment decision fixture"))
            .expect("environment decision fixture JSON");
    decision["selection"] = Value::String("two-person-non-self".to_owned());
    decision["environment_policy"]["approval_mode"] =
        Value::String("two-person-non-self".to_owned());
    decision["environment_policy"]["prevent_self_review"] = Value::Bool(true);
    decision["environment_policy"]["maintainer_actor_refs"] =
        json_array(&["user:rustpunk", "user:reviewer"]);
    decision["environment_policy"]["trigger_actor_ref"] = Value::String("user:trigger".to_owned());
    decision["environment_policy"]["approval_contract"]["actor_rule"] =
        Value::String("eligible-maintainer-distinct-from-trigger".to_owned());
    decision["environment_policy"]
        .as_object_mut()
        .expect("environment policy object")
        .remove("two_person_unavailable_reason");
    fs::write(
        &path,
        serde_json::to_vec_pretty(&decision).expect("serialize two-person decision fixture"),
    )
    .expect("write two-person decision fixture");
    install_fake_gh(root.path(), approved_rulesets(), two_person_environment());

    let evidence = root.path().join("repository-controls-evidence.json");
    let output = authenticated_gate(root.path(), &evidence);
    assert_eq!(output.status.code(), Some(0));
    let evidence: Value =
        serde_json::from_slice(&fs::read(evidence).expect("two-person evidence must be written"))
            .expect("two-person evidence JSON");
    assert_eq!(
        evidence["readback"]["environment"]["protection_rules"][0]["prevent_self_review"],
        true
    );
    assert_eq!(
        evidence["readback"]["environment"]["protection_rules"][0]["reviewers"]
            .as_array()
            .map(Vec::len),
        Some(2)
    );
}

fn copy_decisions(root: &Path) {
    let source = repository_root().join("release/decisions");
    let destination = root.join("decisions");
    fs::create_dir_all(&destination).expect("decision fixture directory");
    for name in [
        "release-rules.json",
        "release-environment.json",
        "publication-policy.json",
    ] {
        fs::copy(source.join(name), destination.join(name)).expect("copy decision fixture");
    }
}

fn authenticated_gate(root: &Path, evidence: &Path) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"));
    let fake_path = format!(
        "{}:{}",
        root.join("bin").display(),
        std::env::var("PATH").expect("test PATH")
    );
    command
        .current_dir(root)
        .env("PATH", fake_path)
        .args([
            "repository",
            "verify",
            "--repo",
            "rustpunk/clinker",
            "--apply-approved",
            "decisions/release-rules.json",
            "--environment-policy",
            "decisions/release-environment.json",
            "--publication-policy",
            "decisions/publication-policy.json",
            "--evidence-manifest",
            evidence.to_str().expect("evidence path UTF-8"),
        ])
        .output()
        .expect("authenticated clinker-release-policy must execute")
}

fn install_fake_gh(root: &Path, rulesets: &str, environment: &str) {
    let bin = root.join("bin");
    fs::create_dir_all(&bin).expect("fake executable directory");
    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
endpoint=""
method=GET
previous=""
for argument in "$@"; do
  if [[ "$previous" == "--method" ]]; then method="$argument"; fi
  if [[ "$argument" == repos/* || "$argument" == users/* || "$argument" == apps/* ]]; then endpoint="$argument"; fi
  previous="$argument"
done
state="$(dirname "$0")/applied"
if [[ "$method" != GET ]]; then
  : > "$state"
  printf '{{}}\n'
  exit 0
fi
case "$endpoint" in
  users/rustpunk) printf '{{"id":7,"login":"rustpunk"}}\n' ;;
  users/reviewer) printf '{{"id":8,"login":"reviewer"}}\n' ;;
  apps/github-actions) printf '{{"id":15368,"slug":"github-actions"}}\n' ;;
  repos/rustpunk/clinker/rulesets*)
    if [[ -f "$state" ]]; then
      printf '%s\n' '{rulesets}'
    else
      printf '[]\n'
    fi
    ;;
  repos/rustpunk/clinker/environments/release) printf '%s\n' '{environment}' ;;
  repos/rustpunk/clinker/environments/release/deployment-branch-policies) printf '%s\n' '{branch_policies}' ;;
  repos/rustpunk/clinker/immutable-releases) printf '{{"enabled":true,"enforced_by_owner":false}}\n' ;;
  repos/rustpunk/clinker) printf '{{"allow_squash_merge":true,"allow_merge_commit":false,"allow_rebase_merge":false,"allow_update_branch":true}}\n' ;;
  *) printf 'unexpected fake gh endpoint: %s\n' "$endpoint" >&2; exit 9 ;;
esac
"#,
        rulesets = rulesets,
        environment = environment,
        branch_policies = r#"{"total_count":1,"branch_policies":[{"name":"v*.*.*","type":"tag"}]}"#,
    );
    let path = bin.join("gh");
    fs::write(&path, script).expect("fake gh executable");
    let mut permissions = fs::metadata(&path).expect("fake gh metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("fake gh executable mode");
}

fn approved_rulesets() -> &'static str {
    r#"[{"id":11,"name":"Clinker protected main","target":"branch","enforcement":"active","bypass_actors":[{"actor_id":7,"actor_type":"User","bypass_mode":"always"}],"conditions":{"ref_name":{"include":["refs/heads/main"],"exclude":[]}},"rules":[{"type":"required_linear_history"},{"type":"non_fast_forward"},{"type":"required_status_checks","parameters":{"strict_required_status_checks_policy":true,"do_not_enforce_on_create":false,"required_status_checks":[{"context":"Dependency policy","integration_id":15368},{"context":"Release policy","integration_id":15368},{"context":"check","integration_id":15368},{"context":"cross-platform","integration_id":15368},{"context":"deny","integration_id":15368},{"context":"filesystem-matrix (linux-nfsv4.1-loopback-ci)","integration_id":15368},{"context":"filesystem-matrix (linux-smb3.1.1-loopback-ci)","integration_id":15368},{"context":"test-macos","integration_id":15368},{"context":"test-windows","integration_id":15368}]}},{"type":"pull_request","parameters":{"required_approving_review_count":1,"dismiss_stale_reviews_on_push":false,"require_code_owner_review":true,"require_last_push_approval":true,"required_review_thread_resolution":true,"allowed_merge_methods":["squash"]}}]},{"id":12,"name":"Clinker protected release tags","target":"tag","enforcement":"active","bypass_actors":[{"actor_id":7,"actor_type":"User","bypass_mode":"always"}],"conditions":{"ref_name":{"include":["refs/tags/v*.*.*"],"exclude":[]}},"rules":[{"type":"creation"},{"type":"update"},{"type":"deletion"},{"type":"non_fast_forward"}]}]"#
}

fn broadened_rulesets() -> &'static str {
    r#"[{"id":11,"name":"Clinker protected main","target":"branch","enforcement":"active","bypass_actors":[{"actor_id":7,"actor_type":"User","bypass_mode":"always"},{"actor_id":8,"actor_type":"User","bypass_mode":"always"}],"conditions":{"ref_name":{"include":["refs/heads/main"],"exclude":[]}},"rules":[]}]"#
}

fn approved_environment() -> &'static str {
    r#"{"name":"release","protection_rules":[{"type":"required_reviewers","prevent_self_review":false,"reviewers":[{"type":"User","reviewer":{"id":7,"login":"rustpunk"}}]}],"deployment_branch_policy":{"protected_branches":false,"custom_branch_policies":true}}"#
}

fn two_person_environment() -> &'static str {
    r#"{"name":"release","protection_rules":[{"type":"required_reviewers","prevent_self_review":true,"reviewers":[{"type":"User","reviewer":{"id":7,"login":"rustpunk"}},{"type":"User","reviewer":{"id":8,"login":"reviewer"}}]}],"deployment_branch_policy":{"protected_branches":false,"custom_branch_policies":true}}"#
}

fn json_array(values: &[&str]) -> Value {
    Value::Array(
        values
            .iter()
            .map(|value| Value::String((*value).to_owned()))
            .collect(),
    )
}
