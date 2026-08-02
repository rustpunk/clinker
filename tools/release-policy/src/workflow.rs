//! Typed GitHub Actions workflow trust policy.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

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
            (
                "build",
                BTreeMap::from([
                    ("attestations", Access::Write),
                    ("contents", Access::Read),
                    ("id-token", Access::Write),
                ]),
            ),
            (
                "assemble-draft",
                BTreeMap::from([("contents", Access::Write)]),
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
    let build = workflow
        .jobs
        .get("build")
        .ok_or_else(|| policy("release build job is absent"))?;
    let assemble = workflow
        .jobs
        .get("assemble-draft")
        .ok_or_else(|| policy("release assembly job is absent"))?;
    require_action(build, "actions/attest-build-provenance")?;
    require_action(build, "actions/upload-artifact")?;
    require_action(assemble, "actions/download-artifact")?;
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

fn require_action(job: &Job, action: &str) -> Result<(), GateError> {
    if !job
        .steps
        .iter()
        .flatten()
        .filter_map(|step| step.uses.as_deref())
        .any(|target| target.split_once('@').map(|(name, _)| name) == Some(action))
    {
        return Err(policy(format!(
            "release job is missing required action '{action}'"
        )));
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
