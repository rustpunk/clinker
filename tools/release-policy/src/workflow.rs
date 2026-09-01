//! Typed GitHub Actions workflow trust policy.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::GateError;

const CHECKOUT: &str = "actions/checkout";
const EVIDENCE_PATH: &str = "${{ runner.temp }}/filesystem-${{ matrix.profile }}.json";
const NFS_PROFILE: &str = "linux-nfsv4.1-loopback-ci";
const SMB_PROFILE: &str = "linux-smb3.1.1-loopback-ci";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Workflow {
    name: String,
    #[serde(rename = "run-name")]
    run_name: Option<String>,
    #[serde(rename = "on")]
    triggers: BTreeMap<String, Value>,
    #[serde(default)]
    env: Option<BTreeMap<String, Value>>,
    permissions: Permissions,
    #[serde(default)]
    concurrency: Option<Concurrency>,
    jobs: BTreeMap<String, Job>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Concurrency {
    group: String,
    #[serde(rename = "cancel-in-progress")]
    cancel_in_progress: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Job {
    name: Option<String>,
    #[serde(rename = "runs-on")]
    runs_on: Option<Value>,
    permissions: Permissions,
    needs: Option<Value>,
    #[serde(rename = "if")]
    condition: Option<Value>,
    #[serde(rename = "timeout-minutes")]
    timeout_minutes: Option<u64>,
    strategy: Option<Strategy>,
    environment: Option<Value>,
    steps: Option<Vec<Step>>,
    uses: Option<String>,
    #[serde(default)]
    with: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    env: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Strategy {
    #[serde(rename = "fail-fast")]
    fail_fast: Option<bool>,
    #[serde(rename = "max-parallel")]
    max_parallel: Option<u64>,
    matrix: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Step {
    name: Option<String>,
    id: Option<String>,
    #[serde(rename = "if")]
    condition: Option<Value>,
    uses: Option<String>,
    #[serde(default)]
    with: Option<BTreeMap<String, Value>>,
    run: Option<String>,
    shell: Option<String>,
    #[serde(default)]
    env: Option<BTreeMap<String, Value>>,
    #[serde(rename = "continue-on-error")]
    continue_on_error: Option<bool>,
    #[serde(rename = "timeout-minutes")]
    timeout_minutes: Option<u64>,
    #[serde(rename = "working-directory")]
    working_directory: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Permissions {
    actions: Option<Access>,
    attestations: Option<Access>,
    checks: Option<Access>,
    contents: Option<Access>,
    deployments: Option<Access>,
    #[serde(rename = "id-token")]
    id_token: Option<Access>,
    issues: Option<Access>,
    #[serde(rename = "pull-requests")]
    pull_requests: Option<Access>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Access {
    Read,
    Write,
    None,
}

impl Permissions {
    fn entries(&self) -> BTreeMap<&'static str, Access> {
        [
            ("actions", self.actions),
            ("attestations", self.attestations),
            ("checks", self.checks),
            ("contents", self.contents),
            ("deployments", self.deployments),
            ("id-token", self.id_token),
            ("issues", self.issues),
            ("pull-requests", self.pull_requests),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.map(|access| (name, access)))
        .collect()
    }
}

pub(super) fn verify(repo_root: &Path) -> Result<(), GateError> {
    let workflow_root = repo_root.join(".github/workflows");
    let mut paths = fs::read_dir(&workflow_root)
        .map_err(|error| GateError::io("read workflow directory", &error))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("yml" | "yaml")
            )
        })
        .collect::<Vec<_>>();
    paths.sort();
    if paths.is_empty() {
        return Err(policy("repository has no workflow files"));
    }

    for path in paths {
        verify_file(&path)?;
    }
    Ok(())
}

fn verify_file(path: &Path) -> Result<(), GateError> {
    let bytes = fs::read(path).map_err(|error| GateError::io("read workflow file", &error))?;
    let source = std::str::from_utf8(&bytes).map_err(|_| policy("workflow source is not UTF-8"))?;
    let workflow: Workflow = serde_saphyr::from_str(source)
        .map_err(|error| policy(format!("workflow schema is invalid: {error}")))?;
    let name = file_name(path)?;

    if workflow.name.trim().is_empty() || workflow.jobs.is_empty() {
        return Err(policy("workflow name and jobs must be non-empty"));
    }
    if !workflow.permissions.entries().is_empty() {
        return Err(policy("workflow default permissions must be empty"));
    }
    validate_triggers(name, &workflow.triggers)?;
    validate_actions(source, &workflow.jobs)?;
    validate_permissions(name, &workflow.jobs)?;

    if name == "ci.yml" {
        validate_ci_policy_jobs(&workflow.jobs)?;
        validate_filesystem_job(&workflow.jobs)?;
    } else if name == "release.yml" {
        validate_release(&workflow)?;
    } else if name == "publish-release.yml" {
        validate_publication(&workflow)?;
    }

    consume_optional_fields(&workflow);
    Ok(())
}

fn validate_triggers(name: &str, triggers: &BTreeMap<String, Value>) -> Result<(), GateError> {
    if name == "release.yml" {
        if triggers.len() != 1 || triggers.keys().next().map(String::as_str) != Some("push") {
            return Err(policy(
                "release workflow must have only the protected tag push trigger",
            ));
        }
        let push = triggers
            .get("push")
            .and_then(Value::as_object)
            .ok_or_else(|| policy("release push trigger must be a mapping"))?;
        if push.len() != 1 || exact_string_array(push.get("tags")) != ["v*"] {
            return Err(policy("release workflow must match exactly tags: [v*]"));
        }
        return Ok(());
    }

    if name == "ci.yml"
        && !triggers
            .get("pull_request")
            .and_then(Value::as_object)
            .is_some_and(serde_json::Map::is_empty)
    {
        return Err(policy(
            "CI pull_request trigger must match every base branch",
        ));
    }

    if triggers
        .get("push")
        .and_then(Value::as_object)
        .and_then(|push| push.get("tags"))
        .is_some_and(|tags| {
            exact_string_array(Some(tags))
                .iter()
                .any(|tag| tag.contains("v*"))
        })
    {
        return Err(policy("only release.yml may match protected version tags"));
    }
    Ok(())
}

fn validate_actions(source: &str, jobs: &BTreeMap<String, Job>) -> Result<(), GateError> {
    for job in jobs.values() {
        if let Some(target) = job.uses.as_deref() {
            validate_action(source, target, None)?;
        }
        for step in job.steps.iter().flatten() {
            let Some(target) = step.uses.as_deref() else {
                continue;
            };
            validate_action(source, target, step.with.as_ref())?;
        }
    }
    Ok(())
}

fn validate_action(
    source: &str,
    target: &str,
    arguments: Option<&BTreeMap<String, Value>>,
) -> Result<(), GateError> {
    if target.starts_with("./") {
        return Ok(());
    }
    let (action, revision) = target
        .rsplit_once('@')
        .ok_or_else(|| policy("external action is missing an immutable revision"))?;
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(policy(format!(
            "external action '{action}' is not pinned to a full commit SHA"
        )));
    }
    let annotation = source
        .lines()
        .find(|line| line.contains("uses:") && line.contains(target))
        .and_then(|line| line.split_once('#').map(|(_, comment)| comment.trim()))
        .ok_or_else(|| {
            policy(format!(
                "external action '{action}' has no version annotation"
            ))
        })?;
    if !readable_annotation(annotation) {
        return Err(policy(format!(
            "external action '{action}' has an invalid version annotation"
        )));
    }
    if action == CHECKOUT
        && arguments
            .and_then(|values| values.get("persist-credentials"))
            .and_then(Value::as_bool)
            != Some(false)
    {
        return Err(policy("checkout must set persist-credentials to false"));
    }
    Ok(())
}

fn readable_annotation(annotation: &str) -> bool {
    let version = annotation.strip_prefix('v').is_some_and(|value| {
        value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_digit())
    });
    let reviewed = annotation.strip_prefix("reviewed ").is_some_and(|value| {
        value.len() >= 10
            && value.as_bytes().get(4) == Some(&b'-')
            && value.as_bytes().get(7) == Some(&b'-')
    });
    version || reviewed
}

fn validate_permissions(name: &str, jobs: &BTreeMap<String, Job>) -> Result<(), GateError> {
    let expected = expected_permissions(name);
    if let Some(expected) = expected {
        let observed = jobs.keys().map(String::as_str).collect::<BTreeSet<_>>();
        let required = expected.keys().copied().collect::<BTreeSet<_>>();
        if observed != required {
            return Err(policy(format!(
                "workflow '{name}' job inventory differs from reviewed policy"
            )));
        }
        for (job_name, permissions) in expected {
            let observed = jobs
                .get(job_name)
                .ok_or_else(|| policy("reviewed job is absent"))?
                .permissions
                .entries();
            if observed != permissions {
                return Err(policy(format!(
                    "job '{job_name}' permissions differ from reviewed policy"
                )));
            }
        }
        return Ok(());
    }

    for (job_name, job) in jobs {
        if job
            .permissions
            .entries()
            .values()
            .any(|access| *access == Access::Write)
        {
            return Err(policy(format!(
                "job '{job_name}' has unreviewed write authority"
            )));
        }
    }
    Ok(())
}

fn expected_permissions(
    name: &str,
) -> Option<BTreeMap<&'static str, BTreeMap<&'static str, Access>>> {
    let read = || BTreeMap::from([("contents", Access::Read)]);
    let entries = match name {
        "ci.yml" => vec![
            ("dependency-policy", read()),
            ("release-policy", read()),
            ("build-portability", read()),
            ("check", read()),
            ("test-windows", read()),
            ("test-macos", read()),
            ("cross-platform", read()),
            ("filesystem-matrix", read()),
            ("deny", read()),
        ],
        "agent-label-sync.yml" => vec![("sync", BTreeMap::from([("issues", Access::Write)]))],
        "agent-queue-audit.yml" => vec![("audit", BTreeMap::from([("issues", Access::Read)]))],
        "agent-issue-close-label-cleanup.yml" => {
            vec![("cleanup", BTreeMap::from([("issues", Access::Write)]))]
        }
        "agent-issue-reopened-routing.yml" => {
            vec![("reset-routing", BTreeMap::from([("issues", Access::Write)]))]
        }
        "agent-pr-merged-closeout-audit.yml" => vec![(
            "audit",
            BTreeMap::from([("issues", Access::Read), ("pull-requests", Access::Read)]),
        )],
        "agent-stale-status-reminders.yml" => vec![(
            "remind",
            BTreeMap::from([("issues", Access::Write), ("pull-requests", Access::Read)]),
        )],
        "pr-auto-update-branches.yml" => vec![(
            "update",
            BTreeMap::from([
                ("contents", Access::Write),
                ("pull-requests", Access::Write),
            ]),
        )],
        "release.yml" => vec![
            ("dependency-policy", read()),
            ("build", read()),
            (
                "assemble-draft",
                BTreeMap::from([
                    ("attestations", Access::Write),
                    ("contents", Access::Write),
                    ("id-token", Access::Write),
                ]),
            ),
        ],
        "publish-release.yml" => vec![(
            "publish-approved-release",
            BTreeMap::from([("contents", Access::Write)]),
        )],
        _ => return None,
    };
    Some(entries.into_iter().collect())
}

fn validate_filesystem_job(jobs: &BTreeMap<String, Job>) -> Result<(), GateError> {
    let job = jobs
        .get("filesystem-matrix")
        .ok_or_else(|| policy("CI filesystem-matrix job is absent"))?;
    if job.runs_on.as_ref().and_then(Value::as_str) != Some("ubuntu-24.04") {
        return Err(policy("filesystem matrix runner must be ubuntu-24.04"));
    }
    let profiles = job
        .strategy
        .as_ref()
        .and_then(|strategy| strategy.matrix.get("profile"));
    if exact_string_array(profiles) != [NFS_PROFILE, SMB_PROFILE] {
        return Err(policy(
            "filesystem matrix must contain the exact two approved profiles",
        ));
    }
    if job
        .env
        .as_ref()
        .is_some_and(|env| env.contains_key("EVIDENCE_PATH"))
    {
        return Err(policy(
            "filesystem evidence path must be bound at runner step scope",
        ));
    }
    let mut provision = 0_u8;
    let mut teardown = 0_u8;
    let mut fetch = None;
    for (index, step) in job.steps.iter().flatten().enumerate() {
        if let Some(run) = step.run.as_deref() {
            if exact_command(
                run,
                &[
                    "cargo",
                    "fetch",
                    "--manifest-path",
                    "tools/release-policy/Cargo.toml",
                    "--locked",
                ],
            ) {
                fetch = Some(index);
            }
            if run.contains("filesystem self-test") && fetch.is_none_or(|fetch| fetch >= index) {
                return Err(policy(
                    "filesystem dependencies must be fetched before offline execution",
                ));
            }
            if run.contains("filesystem provision-and-run") {
                require_direct_command(run, "provision-and-run")?;
                require_evidence_path(step)?;
                if fetch.is_none_or(|fetch| fetch >= index) {
                    return Err(policy(
                        "filesystem dependencies must be fetched before offline execution",
                    ));
                }
                provision += 1;
            }
            if run.contains("filesystem teardown") {
                require_direct_command(run, "teardown")?;
                require_evidence_path(step)?;
                if step.condition.as_ref().and_then(Value::as_str) != Some("always()") {
                    return Err(policy("filesystem teardown must use if: always()"));
                }
                teardown += 1;
            }
            if run.contains("test-filesystem-matrix.sh") {
                return Err(policy("filesystem CI must invoke the Rust gate directly"));
            }
        }
    }
    if (provision, teardown) != (1, 1) {
        return Err(policy(
            "filesystem CI requires one direct provision and one teardown",
        ));
    }
    Ok(())
}

fn validate_ci_policy_jobs(jobs: &BTreeMap<String, Job>) -> Result<(), GateError> {
    let dependency = jobs
        .get("dependency-policy")
        .ok_or_else(|| policy("CI dependency-policy job is absent"))?;
    require_ci_policy_job(dependency, "Dependency policy", "ubuntu-latest", 7)?;
    let steps = dependency
        .steps
        .as_deref()
        .expect("validated step inventory");
    require_unnamed_action_step(
        &steps[0],
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        &["persist-credentials", "false"],
    )?;
    require_unnamed_action_step(
        &steps[1],
        "dtolnay/rust-toolchain@6c977a6ca4077a0ceb28ffbe03f59d46e9ac8772",
        &["toolchain", "1.91", "components", "clippy, rustfmt"],
    )?;
    require_plain_command_step(
        &steps[2],
        "Fetch locked boundary dependencies",
        "cargo fetch --manifest-path tools/dependency-policy/Cargo.toml --locked",
    )?;
    require_plain_command_step(
        &steps[3],
        "Format the detached boundary tool",
        "cargo fmt --manifest-path tools/dependency-policy/Cargo.toml --all -- --check",
    )?;
    require_plain_command_step(
        &steps[4],
        "Lint the detached boundary tool",
        "cargo clippy --manifest-path tools/dependency-policy/Cargo.toml --all-targets --locked --offline -- -D warnings",
    )?;
    require_plain_command_step(
        &steps[5],
        "Exercise the boundary regression suite",
        "cargo test --manifest-path tools/dependency-policy/Cargo.toml --locked --offline",
    )?;
    require_plain_command_step(
        &steps[6],
        "Enforce the final repository boundary",
        "cargo run --manifest-path tools/dependency-policy/Cargo.toml --locked --offline -- --scope final --root .",
    )?;

    let release = jobs
        .get("release-policy")
        .ok_or_else(|| policy("CI release-policy job is absent"))?;
    require_ci_policy_job(release, "Release policy", "ubuntu-24.04", 8)?;
    let steps = release.steps.as_deref().expect("validated step inventory");
    require_unnamed_action_step(
        &steps[0],
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        &["persist-credentials", "false"],
    )?;
    require_unnamed_action_step(
        &steps[1],
        "dtolnay/rust-toolchain@6c977a6ca4077a0ceb28ffbe03f59d46e9ac8772",
        &["toolchain", "1.91", "components", "clippy, rustfmt"],
    )?;
    require_plain_command_step(
        &steps[2],
        "Fetch locked workspace and policy dependencies",
        "cargo fetch --locked cargo fetch --manifest-path tools/dependency-policy/Cargo.toml --locked cargo fetch --manifest-path tools/release-policy/Cargo.toml --locked",
    )?;
    require_plain_command_step(
        &steps[3],
        "Format the detached policy gate",
        "cargo fmt --manifest-path tools/release-policy/Cargo.toml --all -- --check",
    )?;
    require_plain_command_step(
        &steps[4],
        "Lint the detached policy gate",
        "cargo clippy --manifest-path tools/release-policy/Cargo.toml --all-targets --locked --offline -- -D warnings",
    )?;
    require_plain_command_step(
        &steps[5],
        "Exercise the detached policy gate",
        "cargo test --manifest-path tools/release-policy/Cargo.toml --locked --offline",
    )?;
    require_plain_command_step(
        &steps[6],
        "Validate the live release inventory",
        "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- inventory check --print-json",
    )?;
    require_plain_command_step(
        &steps[7],
        "Verify repository workflow trust",
        "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- workflow verify",
    )
}

fn require_ci_policy_job(
    job: &Job,
    name: &str,
    runner: &str,
    step_count: usize,
) -> Result<(), GateError> {
    if job.name.as_deref() != Some(name)
        || job.runs_on.as_ref().and_then(Value::as_str) != Some(runner)
        || job.needs.is_some()
        || job.condition.is_some()
        || job.timeout_minutes.is_some()
        || job.strategy.is_some()
        || job.environment.is_some()
        || job.uses.is_some()
        || job.with.is_some()
        || job.env.is_some()
        || job
            .steps
            .as_ref()
            .is_none_or(|steps| steps.len() != step_count)
    {
        return Err(policy(format!(
            "CI policy job {name:?} differs from reviewed policy"
        )));
    }
    Ok(())
}

fn require_unnamed_action_step(
    step: &Step,
    action: &str,
    arguments: &[&str],
) -> Result<(), GateError> {
    if step.name.is_some()
        || step.id.is_some()
        || step.condition.is_some()
        || step.uses.as_deref() != Some(action)
        || !exact_value_map(step.with.as_ref(), arguments)
        || step.run.is_some()
        || step.shell.is_some()
        || step.env.is_some()
        || step.continue_on_error.is_some()
        || step.timeout_minutes.is_some()
        || step.working_directory.is_some()
    {
        return Err(policy("CI policy action step differs from reviewed policy"));
    }
    Ok(())
}

fn require_plain_command_step(step: &Step, name: &str, command: &str) -> Result<(), GateError> {
    if step.name.as_deref() != Some(name)
        || step.id.is_some()
        || step.condition.is_some()
        || step.uses.is_some()
        || step.with.is_some()
        || step
            .run
            .as_deref()
            .is_none_or(|run| !exact_script(run, command))
        || step.shell.is_some()
        || step.env.is_some()
        || step.continue_on_error.is_some()
        || step.timeout_minutes.is_some()
        || step.working_directory.is_some()
    {
        return Err(policy(format!(
            "CI policy command step {name:?} differs from reviewed policy"
        )));
    }
    Ok(())
}

fn require_evidence_path(step: &Step) -> Result<(), GateError> {
    if step
        .env
        .as_ref()
        .and_then(|env| env.get("EVIDENCE_PATH"))
        .and_then(Value::as_str)
        != Some(EVIDENCE_PATH)
    {
        return Err(policy(
            "filesystem evidence path must use runner.temp at step scope",
        ));
    }
    Ok(())
}

fn exact_command(run: &str, expected: &[&str]) -> bool {
    run.split_whitespace().eq(expected.iter().copied())
}

fn require_direct_command(run: &str, operation: &str) -> Result<(), GateError> {
    let prefix = [
        "cargo",
        "run",
        "--quiet",
        "--manifest-path",
        "tools/release-policy/Cargo.toml",
        "--locked",
        "--offline",
        "--",
        "filesystem",
        operation,
    ];
    let tokens = run.split_whitespace().collect::<Vec<_>>();
    if !tokens.starts_with(&prefix)
        || !tokens.contains(&"--profile")
        || !tokens.contains(&"--evidence")
    {
        return Err(policy(format!(
            "filesystem {operation} is not the direct locked Rust command"
        )));
    }
    Ok(())
}

fn validate_release(workflow: &Workflow) -> Result<(), GateError> {
    let dependency = workflow
        .jobs
        .get("dependency-policy")
        .ok_or_else(|| policy("release dependency-policy job is absent"))?;
    let build = workflow
        .jobs
        .get("build")
        .ok_or_else(|| policy("release build job is absent"))?;
    let assemble = workflow
        .jobs
        .get("assemble-draft")
        .ok_or_else(|| policy("release assembly job is absent"))?;
    require_exact_release_dependency(dependency)?;
    require_exact_release_build(build)?;
    require_exact_release_assembly(assemble)?;
    if workflow
        .env
        .as_ref()
        .and_then(|env| env.get("CARGO_INCREMENTAL"))
        .is_none_or(|value| value != 0 && value != "0")
    {
        return Err(policy(
            "release workflow must disable incremental compilation",
        ));
    }
    Ok(())
}

fn require_exact_release_dependency(job: &Job) -> Result<(), GateError> {
    if job.name.as_deref() != Some("Verify dependency policy")
        || job.runs_on.as_ref().and_then(Value::as_str) != Some("ubuntu-24.04")
        || job.needs.is_some()
        || job.condition.is_some()
        || job.timeout_minutes.is_some()
        || job.strategy.is_some()
        || job.environment.is_some()
        || job.uses.is_some()
        || job.with.is_some()
        || job.env.is_some()
        || job.steps.as_ref().is_none_or(|steps| steps.len() != 3)
    {
        return Err(policy(
            "release dependency-policy job shape differs from reviewed policy",
        ));
    }
    let steps = job.steps.as_deref().expect("validated step inventory");
    require_action_step(
        &steps[0],
        "Check out the candidate source",
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        &["persist-credentials", "false"],
    )?;
    require_unnamed_action_step(
        &steps[1],
        "dtolnay/rust-toolchain@6c977a6ca4077a0ceb28ffbe03f59d46e9ac8772",
        &["toolchain", "1.91"],
    )?;
    require_command_step(
        &steps[2],
        "Test and enforce the locked Rust boundary tool",
        "set -euo pipefail cargo test --manifest-path tools/dependency-policy/Cargo.toml --locked cargo run --manifest-path tools/dependency-policy/Cargo.toml --locked --offline -- --scope final --root .",
        &[],
    )
}

fn require_exact_release_build(build: &Job) -> Result<(), GateError> {
    if build.name.as_deref() != Some("Build ${{ matrix.target }}")
        || build.runs_on.as_ref().and_then(Value::as_str) != Some("${{ matrix.os }}")
        || build.needs.as_ref().and_then(Value::as_str) != Some("dependency-policy")
        || build.condition.is_some()
        || build.timeout_minutes.is_some()
        || build.environment.is_some()
        || build.uses.is_some()
        || build.with.is_some()
        || build.env.is_some()
    {
        return Err(policy(
            "release build job shape differs from reviewed policy",
        ));
    }
    let strategy = build
        .strategy
        .as_ref()
        .ok_or_else(|| policy("release build strategy is absent"))?;
    if strategy.fail_fast != Some(false)
        || strategy.max_parallel.is_some()
        || strategy.matrix
            != json!({
                "include": [
                    {"target": "x86_64-unknown-linux-gnu", "os": "ubuntu-24.04", "binary_suffix": ""},
                    {"target": "x86_64-pc-windows-msvc", "os": "windows-2025", "binary_suffix": ".exe"},
                    {"target": "aarch64-apple-darwin", "os": "macos-15", "binary_suffix": ""},
                    {"target": "x86_64-apple-darwin", "os": "macos-15-intel", "binary_suffix": ""}
                ]
            })
    {
        return Err(policy(
            "release build runner matrix differs from reviewed policy",
        ));
    }
    let steps = build
        .steps
        .as_deref()
        .ok_or_else(|| policy("release build steps are absent"))?;
    if steps.len() != 6 {
        return Err(policy(
            "release build step inventory differs from reviewed policy",
        ));
    }
    require_action_step(
        &steps[0],
        "Check out the candidate source",
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        &["persist-credentials", "false"],
    )?;
    require_command_step(
        &steps[1],
        "Install the repository toolchain target",
        "set -euo pipefail test -f rust-toolchain.toml rustup show active-toolchain rustup target add \"${{ matrix.target }}\"",
        &[],
    )?;
    require_command_step(
        &steps[2],
        "Fetch the locked workspace dependencies",
        "cargo fetch --locked",
        &[],
    )?;
    require_command_step(
        &steps[3],
        "Build the governed target executables",
        "cargo build --locked --offline --release --target \"$BUILD_TARGET\" -p clinker -p cxl-cli",
        &["BUILD_TARGET", "${{ matrix.target }}"],
    )?;
    require_command_step_with_timeout(
        &steps[4],
        "Smoke-test the native target executables",
        "set -euo pipefail \"target/$BUILD_TARGET/release/clinker$BINARY_SUFFIX\" --version \"target/$BUILD_TARGET/release/cxl$BINARY_SUFFIX\" --version",
        &[
            "BUILD_TARGET",
            "${{ matrix.target }}",
            "BINARY_SUFFIX",
            "${{ matrix.binary_suffix }}",
        ],
        Some(1),
    )?;
    require_action_step(
        &steps[5],
        "Upload the native target executables",
        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
        &[
            "name",
            "release-input-${{ matrix.target }}",
            "path",
            "target/${{ matrix.target }}/release/clinker${{ matrix.binary_suffix }}\ntarget/${{ matrix.target }}/release/cxl${{ matrix.binary_suffix }}\n",
            "if-no-files-found",
            "error",
            "retention-days",
            "7",
            "compression-level",
            "0",
        ],
    )?;
    Ok(())
}

fn require_exact_release_assembly(assemble: &Job) -> Result<(), GateError> {
    if assemble.name.as_deref() != Some("Assemble and reread private draft")
        || assemble.runs_on.as_ref().and_then(Value::as_str) != Some("ubuntu-24.04")
        || assemble.needs.as_ref().and_then(Value::as_str) != Some("build")
        || assemble.condition.is_some()
        || assemble.timeout_minutes.is_some()
        || assemble.strategy.is_some()
        || assemble.environment.is_some()
        || assemble.uses.is_some()
        || assemble.with.is_some()
        || assemble.env.is_some()
    {
        return Err(policy(
            "release assembly job shape differs from reviewed policy",
        ));
    }
    let steps = assemble
        .steps
        .as_deref()
        .ok_or_else(|| policy("release assembly steps are absent"))?;
    if steps.len() != 8 {
        return Err(policy(
            "release assembly step inventory differs from reviewed policy",
        ));
    }
    require_action_step(
        &steps[0],
        "Check out verification code",
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        &["persist-credentials", "false"],
    )?;
    require_action_step(
        &steps[1],
        "Download the Linux build input",
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
        &[
            "name",
            "release-input-x86_64-unknown-linux-gnu",
            "path",
            "target/x86_64-unknown-linux-gnu/release",
        ],
    )?;
    require_action_step(
        &steps[2],
        "Download the Windows build input",
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
        &[
            "name",
            "release-input-x86_64-pc-windows-msvc",
            "path",
            "target/x86_64-pc-windows-msvc/release",
        ],
    )?;
    require_action_step(
        &steps[3],
        "Download the Apple silicon build input",
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
        &[
            "name",
            "release-input-aarch64-apple-darwin",
            "path",
            "target/aarch64-apple-darwin/release",
        ],
    )?;
    require_action_step(
        &steps[4],
        "Download the Intel macOS build input",
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
        &[
            "name",
            "release-input-x86_64-apple-darwin",
            "path",
            "target/x86_64-apple-darwin/release",
        ],
    )?;
    require_command_step(
        &steps[5],
        "Build and verify the exact release asset set with Rust policy",
        "cargo fetch --manifest-path tools/release-policy/Cargo.toml --locked cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- inventory check --print-json cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- release build-bundle --target x86_64-unknown-linux-gnu --source-sha \"$RELEASE_SOURCE_SHA\" --output-dir artifacts cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- release build-bundle --target x86_64-pc-windows-msvc --source-sha \"$RELEASE_SOURCE_SHA\" --output-dir artifacts cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- release build-bundle --target aarch64-apple-darwin --source-sha \"$RELEASE_SOURCE_SHA\" --output-dir artifacts cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- release build-bundle --target x86_64-apple-darwin --source-sha \"$RELEASE_SOURCE_SHA\" --output-dir artifacts cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- release verify assemble --asset-dir artifacts --repository \"$RELEASE_REPOSITORY\" --workflow .github/workflows/release.yml --ref \"$RELEASE_REF\" --source-sha \"$RELEASE_SOURCE_SHA\"",
        &[
            "RELEASE_REPOSITORY",
            "${{ github.repository }}",
            "RELEASE_REF",
            "${{ github.ref }}",
            "RELEASE_SOURCE_SHA",
            "${{ github.sha }}",
        ],
    )?;
    require_action_step(
        &steps[6],
        "Attest the verified release archives",
        "actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8",
        &["subject-path", "artifacts/*.tar.gz\nartifacts/*.zip\n"],
    )?;
    require_command_step(
        &steps[7],
        "Stage and freshly verify the private draft with the Rust gate",
        "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- release stage-candidate-draft --repo \"$RELEASE_REPOSITORY\" --candidate-tag \"$RELEASE_CANDIDATE_TAG\" --source-sha \"$RELEASE_SOURCE_SHA\" --asset-dir artifacts --deadline-seconds 600",
        &[
            "GH_TOKEN",
            "${{ github.token }}",
            "RELEASE_REPOSITORY",
            "${{ github.repository }}",
            "RELEASE_CANDIDATE_TAG",
            "${{ github.ref_name }}",
            "RELEASE_SOURCE_SHA",
            "${{ github.sha }}",
        ],
    )?;
    Ok(())
}

fn require_command_step(
    step: &Step,
    name: &str,
    command: &str,
    env: &[&str],
) -> Result<(), GateError> {
    require_command_step_with_timeout(step, name, command, env, None)
}

fn require_command_step_with_timeout(
    step: &Step,
    name: &str,
    command: &str,
    env: &[&str],
    timeout_minutes: Option<u64>,
) -> Result<(), GateError> {
    if step.name.as_deref() != Some(name)
        || step.id.is_some()
        || step.condition.is_some()
        || step.uses.is_some()
        || step.with.is_some()
        || step
            .run
            .as_deref()
            .is_none_or(|run| !exact_script(run, command))
        || step.shell.as_deref() != Some("bash")
        || !exact_value_map(step.env.as_ref(), env)
        || step.continue_on_error.is_some()
        || step.timeout_minutes != timeout_minutes
        || step.working_directory.is_some()
    {
        return Err(policy(format!(
            "release command step {name:?} differs from reviewed policy"
        )));
    }
    Ok(())
}

fn require_action_step(
    step: &Step,
    name: &str,
    action: &str,
    arguments: &[&str],
) -> Result<(), GateError> {
    if step.name.as_deref() != Some(name)
        || step.id.is_some()
        || step.condition.is_some()
        || step.uses.as_deref() != Some(action)
        || !exact_value_map(step.with.as_ref(), arguments)
        || step.run.is_some()
        || step.shell.is_some()
        || step.env.is_some()
        || step.continue_on_error.is_some()
        || step.timeout_minutes.is_some()
        || step.working_directory.is_some()
    {
        return Err(policy(format!(
            "release action step {name:?} differs from reviewed policy"
        )));
    }
    Ok(())
}

fn exact_script(observed: &str, expected: &str) -> bool {
    observed
        .split_whitespace()
        .filter(|token| *token != "\\")
        .eq(expected.split_whitespace())
}

fn exact_value_map(observed: Option<&BTreeMap<String, Value>>, expected: &[&str]) -> bool {
    if !expected.len().is_multiple_of(2) {
        return false;
    }
    let Some(observed) = observed else {
        return expected.is_empty();
    };
    if observed.len() != expected.len() / 2 {
        return false;
    }
    expected.chunks_exact(2).all(|pair| {
        observed
            .get(pair[0])
            .is_some_and(|value| value_matches(value, pair[1]))
    })
}

fn value_matches(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Bool(value) => value.to_string() == expected,
        Value::Number(value) => value.to_string() == expected,
        _ => false,
    }
}

fn validate_publication(workflow: &Workflow) -> Result<(), GateError> {
    let dispatch = workflow
        .triggers
        .get("workflow_dispatch")
        .and_then(Value::as_object)
        .and_then(|dispatch| dispatch.get("inputs"))
        .and_then(Value::as_object)
        .ok_or_else(|| policy("publication workflow_dispatch inputs are absent"))?;
    let required = [
        "candidate_tag",
        "candidate_authorization_blob_sha",
        "candidate_authorization_sha256",
        "candidate_decision_blob_sha",
        "candidate_evidence_blob_sha",
        "source_sha",
        "build_workflow_sha",
        "publish_workflow_ref",
        "publish_workflow_sha",
        "candidate_release_id",
        "approval_payload_blob_sha",
        "approval_record_sha256",
        "approval_mode",
        "dispatch_id",
    ];
    if dispatch.keys().map(String::as_str).collect::<BTreeSet<_>>()
        != required.into_iter().collect()
    {
        return Err(policy(
            "publication dispatch inputs differ from reviewed policy",
        ));
    }
    let job = workflow
        .jobs
        .get("publish-approved-release")
        .ok_or_else(|| policy("protected publication job is absent"))?;
    if job.environment.as_ref().and_then(Value::as_str) != Some("release") {
        return Err(policy(
            "publication job must use the protected release environment",
        ));
    }
    let concurrency = workflow
        .concurrency
        .as_ref()
        .ok_or_else(|| policy("publication concurrency is absent"))?;
    if concurrency.cancel_in_progress.as_bool() != Some(false)
        || !concurrency.group.contains("candidate_tag")
    {
        return Err(policy(
            "publication must serialize without cancelling by candidate tag",
        ));
    }
    Ok(())
}

fn exact_string_array(value: Option<&Value>) -> Vec<&str> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

fn file_name(path: &Path) -> Result<&str, GateError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| policy("workflow file name is not UTF-8"))
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("workflow.policy", detail)
}

fn consume_optional_fields(workflow: &Workflow) {
    let _ = (&workflow.run_name, &workflow.env, &workflow.concurrency);
    for job in workflow.jobs.values() {
        let _ = (
            &job.name,
            &job.needs,
            &job.condition,
            &job.timeout_minutes,
            &job.environment,
            &job.with,
            &job.env,
        );
        if let Some(strategy) = &job.strategy {
            let _ = (&strategy.fail_fast, &strategy.max_parallel);
        }
        for step in job.steps.iter().flatten() {
            let _ = (
                &step.name,
                &step.id,
                &step.shell,
                &step.env,
                &step.continue_on_error,
                &step.timeout_minutes,
                &step.working_directory,
            );
        }
    }
}
