//! Ordered repository ownership, update, and authenticated control verification.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Map, Value, json};
use tempfile::NamedTempFile;

use crate::canonical;
use crate::child::{self, ChildSpec, Termination};
use crate::decision::{self, DecisionRequest};
use crate::error::GateError;
use crate::evidence;
use crate::limits::{MAX_CHILD_OUTPUT_BYTES, MAX_INPUT_BYTES, read_bounded};

const DECISION_ROOT: &str = "release/decisions";
const APPROVED_OWNER: &str = "@rustpunk";

/// Fully preflighted authenticated repository-control application.
#[derive(Debug)]
pub(super) struct ApplyRequest {
    pub(super) repository: String,
    pub(super) release_rules: PathBuf,
    pub(super) environment_policy: PathBuf,
    pub(super) publication_policy: PathBuf,
    pub(super) evidence_manifest: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Dependabot {
    version: u8,
    updates: Vec<Update>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Update {
    #[serde(rename = "package-ecosystem")]
    package_ecosystem: String,
    directory: String,
    schedule: Schedule,
    #[serde(rename = "open-pull-requests-limit")]
    open_pull_requests_limit: u8,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Schedule {
    interval: String,
    day: String,
    time: String,
    timezone: String,
}

#[derive(Debug)]
struct CodeownersRule {
    pattern: String,
    owners: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct Actor {
    actor_type: &'static str,
    id: u64,
}

#[derive(Debug)]
struct Desired {
    rulesets: Value,
    environment: Value,
    branch_policies: Value,
    immutable_releases: Value,
    repository_settings: Value,
}

#[derive(Debug)]
struct Snapshot {
    rulesets: Value,
    environment: Value,
    branch_policies: Value,
    immutable_releases: Value,
    repository_settings: Value,
}

/// Verify only committed repository ownership and updater policy.
pub(super) fn verify_configuration(repo_root: &Path) -> Result<(), GateError> {
    verify_codeowners(&repo_root.join(".github/CODEOWNERS"))?;
    verify_dependabot(&repo_root.join(".github/dependabot.yml"))
}

/// Verify live controls against the committed accepted decision records.
pub(super) fn verify_readback(repo_root: &Path, repository: &str) -> Result<(), GateError> {
    let request = ApplyRequest {
        repository: repository.to_owned(),
        release_rules: repo_root.join(DECISION_ROOT).join("release-rules.json"),
        environment_policy: repo_root
            .join(DECISION_ROOT)
            .join("release-environment.json"),
        publication_policy: repo_root
            .join(DECISION_ROOT)
            .join("publication-policy.json"),
        evidence_manifest: PathBuf::new(),
    };
    verify_configuration(repo_root)?;
    validate_decisions(&request)?;
    let desired = desired_controls(&request)?;
    let observed = read_snapshot(&request.repository)?;
    compare_snapshot(&observed, &desired)
}

/// Apply only approved deltas and require a complete immediate readback.
pub(super) fn apply_and_verify(repo_root: &Path, request: &ApplyRequest) -> Result<(), GateError> {
    verify_configuration(repo_root)?;
    validate_repository_name(&request.repository)?;
    validate_decisions(request)?;
    let desired = desired_controls(request)?;
    let before = read_snapshot(&request.repository)?;
    apply_delta(
        &request.repository,
        &before,
        &desired,
        &request.evidence_manifest,
    )?;
    let after = read_snapshot(&request.repository)?;
    compare_snapshot(&after, &desired)?;

    let evidence_value = json!({
        "schema": "clinker.repository-controls-evidence/v1",
        "repository": request.repository,
        "release_status": "incomplete",
        "completion_eligible": false,
        "readback": {
            "rulesets": normalize_rulesets(&after.rulesets)?,
            "environment": normalize_environment(&after.environment)?,
            "deployment_branch_policies": normalize_branch_policies(&after.branch_policies)?,
            "immutable_releases": normalize_immutable(&after.immutable_releases)?,
            "repository_settings": normalize_repository(&after.repository_settings)?,
        }
    });
    let encoded = serde_json::to_vec(&evidence_value)
        .map_err(|_| internal("serialize repository controls evidence"))?;
    let canonical = canonical::parse_json(&encoded)?;
    evidence::create_only(&request.evidence_manifest, &canonical)?;
    Ok(())
}

fn verify_dependabot(path: &Path) -> Result<(), GateError> {
    let bytes = read_bounded(path, "read Dependabot configuration", MAX_INPUT_BYTES)?;
    let source =
        std::str::from_utf8(&bytes).map_err(|_| policy("Dependabot configuration is not UTF-8"))?;
    let config: Dependabot = serde_saphyr::from_str(source)
        .map_err(|error| policy(format!("Dependabot configuration is invalid: {error}")))?;
    if config.version != 2 || config.updates.len() != 1 {
        return Err(policy("Dependabot must contain one version 2 update"));
    }
    let update = &config.updates[0];
    if update.package_ecosystem != "github-actions"
        || update.directory != "/"
        || update.schedule.interval != "weekly"
        || update.schedule.day != "monday"
        || update.schedule.time != "06:00"
        || update.schedule.timezone != "Etc/UTC"
        || update.open_pull_requests_limit != 5
    {
        return Err(policy(
            "Dependabot update policy differs from the reviewed schedule",
        ));
    }
    Ok(())
}

fn verify_codeowners(path: &Path) -> Result<(), GateError> {
    let bytes = read_bounded(path, "read CODEOWNERS", MAX_INPUT_BYTES)?;
    let source = std::str::from_utf8(&bytes).map_err(|_| policy("CODEOWNERS is not UTF-8"))?;
    let mut rules = Vec::new();
    for (index, line) in source.lines().enumerate() {
        let stripped = line.trim();
        if stripped.is_empty() || stripped.starts_with('#') {
            continue;
        }
        let fields = stripped.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 2 {
            return Err(policy(format!(
                "CODEOWNERS line {} has no owner",
                index + 1
            )));
        }
        let pattern = fields[0];
        if pattern.contains('!') || pattern.contains('[') || pattern.contains(']') {
            return Err(policy(format!(
                "CODEOWNERS line {} uses unsupported pattern syntax",
                index + 1
            )));
        }
        let owners = fields[1..]
            .iter()
            .map(|owner| (*owner).to_owned())
            .collect::<BTreeSet<_>>();
        if owners.iter().any(|owner| !valid_owner(owner)) {
            return Err(policy(format!(
                "CODEOWNERS line {} has an invalid owner",
                index + 1
            )));
        }
        rules.push(CodeownersRule {
            pattern: pattern.to_owned(),
            owners,
        });
    }
    let required = [
        ".github/dependabot.yml",
        ".github/workflows/ci.yml",
        ".github/workflows/release.yml",
        ".github/workflows/publish-release.yml",
        "scripts/release/check-workflow-trust.sh",
        "scripts/release/release-decision.schema.json",
        "release/inventory.toml",
        "rust-toolchain.toml",
        "tools/dependency-policy/Cargo.toml",
        "tools/release-policy/Cargo.toml",
        "Cargo.toml",
        "Cargo.lock",
        "LICENSE",
    ];
    let approved = BTreeSet::from([APPROVED_OWNER.to_owned()]);
    for critical in required {
        let owners = rules
            .iter()
            .rev()
            .find(|rule| codeowners_match(&rule.pattern, critical))
            .map(|rule| &rule.owners)
            .ok_or_else(|| policy(format!("CODEOWNERS does not cover '{critical}'")))?;
        if owners != &approved {
            return Err(policy(format!(
                "CODEOWNERS final rule for '{critical}' widens or changes authority"
            )));
        }
    }
    Ok(())
}

fn valid_owner(owner: &str) -> bool {
    owner.starts_with('@')
        && owner.len() > 1
        && owner[1..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'/'))
}

fn codeowners_match(pattern: &str, path: &str) -> bool {
    let anchored = pattern.starts_with('/');
    let pattern = pattern.strip_prefix('/').unwrap_or(pattern);
    if let Some(directory) = pattern.strip_suffix('/') {
        if anchored || directory.contains('/') {
            return path == directory || path.starts_with(&format!("{directory}/"));
        }
        return path
            .split('/')
            .any(|component| wildcard_match(directory.as_bytes(), component.as_bytes()));
    }
    if !anchored && !pattern.contains('/') {
        return path
            .split('/')
            .any(|component| wildcard_match(pattern.as_bytes(), component.as_bytes()));
    }
    wildcard_match(pattern.as_bytes(), path.as_bytes())
}

fn wildcard_match(pattern: &[u8], value: &[u8]) -> bool {
    let mut current = vec![false; value.len() + 1];
    current[0] = true;
    let mut index = 0;
    while index < pattern.len() {
        let mut next = vec![false; value.len() + 1];
        if pattern[index] == b'*' && pattern.get(index + 1) == Some(&b'*') {
            next[0] = current[0];
            for value_index in 1..=value.len() {
                next[value_index] = current[value_index] || next[value_index - 1];
            }
            index += 2;
        } else if pattern[index] == b'*' {
            next[0] = current[0];
            for value_index in 1..=value.len() {
                next[value_index] = current[value_index]
                    || (value[value_index - 1] != b'/' && next[value_index - 1]);
            }
            index += 1;
        } else {
            for value_index in 1..=value.len() {
                next[value_index] = current[value_index - 1]
                    && (pattern[index] == value[value_index - 1]
                        || (pattern[index] == b'?' && value[value_index - 1] != b'/'));
            }
            index += 1;
        }
        current = next;
    }
    current[value.len()]
}

fn validate_decisions(request: &ApplyRequest) -> Result<(), GateError> {
    decision::validate(&DecisionRequest {
        schema: None,
        records: vec![
            request.release_rules.clone(),
            request.environment_policy.clone(),
            request.publication_policy.clone(),
        ],
        authorization_schema: None,
        authorization_record: None,
        candidate_evidence: None,
        require_ids: vec![
            "release-rules".to_owned(),
            "release-environment".to_owned(),
            "publication-policy".to_owned(),
        ],
        require_authorization_id: None,
        require_authorized: false,
        require_complete: false,
        require_accepted: true,
    })
}

fn desired_controls(request: &ApplyRequest) -> Result<Desired, GateError> {
    let rules_record = read_json(&request.release_rules, "read release rules decision")?;
    let environment_record = read_json(
        &request.environment_policy,
        "read release environment decision",
    )?;
    let publication_record = read_json(
        &request.publication_policy,
        "read publication policy decision",
    )?;
    if publication_record.get("selection").and_then(Value::as_str) != Some("live-gate-required") {
        return Err(policy("publication policy is not live-gate-required"));
    }
    let rules = object_field(&rules_record, "ruleset", "release rules decision")?;
    let main = object_field_value(rules, "main_rule", "ruleset")?;
    let tag = object_field_value(rules, "tag_rule", "ruleset")?;
    let environment = object_field(
        &environment_record,
        "environment_policy",
        "release environment decision",
    )?;
    let owner = request
        .repository
        .split_once('/')
        .map(|(owner, _)| owner)
        .ok_or_else(|| policy("repository must be owner/name"))?;

    let actor_refs = string_array(main.get("bypass_actor_refs"), "main bypass actors")?;
    let tag_actors = string_array(tag.get("creation_actor_refs"), "tag creation actors")?;
    if actor_refs != tag_actors {
        return Err(policy("branch and tag bypass actor sets differ"));
    }
    let reviewer_refs = string_array(
        environment.get("maintainer_actor_refs"),
        "environment maintainers",
    )?;
    let all_refs = actor_refs
        .iter()
        .chain(reviewer_refs.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut actors = BTreeMap::new();
    for actor_ref in all_refs {
        actors.insert(actor_ref.clone(), resolve_actor(owner, &actor_ref)?);
    }
    let app_id = resolve_app_id("github-actions")?;

    let bypass = actor_refs
        .iter()
        .map(|actor_ref| {
            let actor = actors
                .get(actor_ref)
                .ok_or_else(|| internal("resolved bypass actor is absent"))?;
            Ok(json!({
                "actor_id": actor.id,
                "actor_type": actor.actor_type,
                "bypass_mode": "always"
            }))
        })
        .collect::<Result<Vec<_>, GateError>>()?;
    let check_records = main
        .get("required_status_checks")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("required status checks are absent"))?;
    let check_contexts = check_records
        .iter()
        .filter_map(|check| check.get("context").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();
    for required in ["Dependency policy", "Release policy"] {
        if !check_contexts.contains(required) {
            return Err(policy(format!(
                "required status checks omit the {required} job"
            )));
        }
    }
    let checks = check_records
        .iter()
        .map(|check| {
            let context = check
                .get("context")
                .and_then(Value::as_str)
                .ok_or_else(|| policy("required status check context is absent"))?;
            Ok(json!({"context": context, "integration_id": app_id}))
        })
        .collect::<Result<Vec<_>, GateError>>()?;
    let rulesets = json!([
        {
            "name": "Clinker protected main",
            "target": "branch",
            "enforcement": "active",
            "bypass_actors": bypass,
            "conditions": {"ref_name": {"include": ["refs/heads/main"], "exclude": []}},
            "rules": [
                {"type": "required_linear_history"},
                {"type": "non_fast_forward"},
                {"type": "required_status_checks", "parameters": {
                    "strict_required_status_checks_policy": true,
                    "do_not_enforce_on_create": false,
                    "required_status_checks": checks,
                }},
                {"type": "pull_request", "parameters": {
                    "required_approving_review_count": 1,
                    "dismiss_stale_reviews_on_push": false,
                    "require_code_owner_review": true,
                    "require_last_push_approval": true,
                    "required_review_thread_resolution": true,
                    "allowed_merge_methods": ["squash"],
                }},
            ],
        },
        {
            "name": "Clinker protected release tags",
            "target": "tag",
            "enforcement": "active",
            "bypass_actors": bypass,
            "conditions": {"ref_name": {"include": ["refs/tags/v*.*.*"], "exclude": []}},
            "rules": [
                {"type": "creation"},
                {"type": "update"},
                {"type": "deletion"},
                {"type": "non_fast_forward"},
            ],
        }
    ]);
    let reviewers = reviewer_refs
        .iter()
        .map(|actor_ref| {
            let actor = actors
                .get(actor_ref)
                .ok_or_else(|| internal("resolved environment reviewer is absent"))?;
            Ok(json!({
                "type": actor.actor_type,
                "reviewer": {"id": actor.id, "login": actor_login(actor_ref)},
            }))
        })
        .collect::<Result<Vec<_>, GateError>>()?;
    let prevent_self_review = environment
        .get("prevent_self_review")
        .and_then(Value::as_bool)
        .ok_or_else(|| policy("environment prevent_self_review is absent"))?;
    let environment = json!({
        "name": "release",
        "protection_rules": [{
            "type": "required_reviewers",
            "prevent_self_review": prevent_self_review,
            "reviewers": reviewers,
        }],
        "deployment_branch_policy": {
            "protected_branches": false,
            "custom_branch_policies": true,
        }
    });
    Ok(Desired {
        rulesets,
        environment,
        branch_policies: json!({
            "total_count": 1,
            "branch_policies": [{"name": "v*.*.*", "type": "tag"}]
        }),
        immutable_releases: json!({"enabled": true, "enforced_by_owner": false}),
        repository_settings: json!({
            "allow_squash_merge": true,
            "allow_merge_commit": false,
            "allow_rebase_merge": false,
            "allow_update_branch": true,
        }),
    })
}

fn actor_login(actor_ref: &str) -> &str {
    actor_ref
        .split_once(':')
        .map(|(_, name)| name)
        .unwrap_or(actor_ref)
}

fn resolve_actor(owner: &str, actor_ref: &str) -> Result<Actor, GateError> {
    let (kind, name) = actor_ref
        .split_once(':')
        .ok_or_else(|| policy("actor reference must have a typed prefix"))?;
    let (endpoint, actor_type) = match kind {
        "user" => (format!("users/{name}"), "User"),
        "team" => (format!("orgs/{owner}/teams/{name}"), "Team"),
        _ => return Err(policy("live actor references must use user: or team:")),
    };
    let value = gh_json(&["api", "--method", "GET", &endpoint])?;
    let id = value
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| policy("resolved actor has no numeric id"))?;
    Ok(Actor { actor_type, id })
}

fn resolve_app_id(slug: &str) -> Result<u64, GateError> {
    let endpoint = format!("apps/{slug}");
    gh_json(&["api", "--method", "GET", &endpoint])?
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| policy("GitHub Actions app has no numeric id"))
}

fn read_snapshot(repository: &str) -> Result<Snapshot, GateError> {
    validate_repository_name(repository)?;
    let endpoint = |suffix: &str| format!("repos/{repository}{suffix}");
    Ok(Snapshot {
        rulesets: gh_json(&[
            "api",
            "--method",
            "GET",
            &endpoint("/rulesets?includes_parents=false"),
        ])?,
        environment: gh_json(&["api", "--method", "GET", &endpoint("/environments/release")])?,
        branch_policies: gh_json(&[
            "api",
            "--method",
            "GET",
            &endpoint("/environments/release/deployment-branch-policies"),
        ])?,
        immutable_releases: gh_json(&["api", "--method", "GET", &endpoint("/immutable-releases")])?,
        repository_settings: gh_json(&["api", "--method", "GET", &endpoint("")])?,
    })
}

fn compare_snapshot(snapshot: &Snapshot, desired: &Desired) -> Result<(), GateError> {
    if normalize_rulesets(&snapshot.rulesets)? != normalize_rulesets(&desired.rulesets)?
        || normalize_environment(&snapshot.environment)?
            != normalize_environment(&desired.environment)?
        || normalize_branch_policies(&snapshot.branch_policies)?
            != normalize_branch_policies(&desired.branch_policies)?
        || normalize_immutable(&snapshot.immutable_releases)?
            != normalize_immutable(&desired.immutable_releases)?
        || normalize_repository(&snapshot.repository_settings)?
            != normalize_repository(&desired.repository_settings)?
    {
        return Err(policy("repository controls do not match approved policy"));
    }
    Ok(())
}

fn apply_delta(
    repository: &str,
    before: &Snapshot,
    desired: &Desired,
    evidence_path: &Path,
) -> Result<(), GateError> {
    let endpoint = |suffix: &str| format!("repos/{repository}{suffix}");
    if normalize_rulesets(&before.rulesets)? != normalize_rulesets(&desired.rulesets)? {
        let observed = before
            .rulesets
            .as_array()
            .ok_or_else(|| policy("ruleset readback must be an array"))?;
        for wanted in desired
            .rulesets
            .as_array()
            .ok_or_else(|| internal("desired rulesets must be an array"))?
        {
            let target = wanted
                .get("target")
                .and_then(Value::as_str)
                .ok_or_else(|| internal("desired ruleset target is absent"))?;
            let matching = observed
                .iter()
                .filter(|ruleset| {
                    ruleset.get("target").and_then(Value::as_str) == Some(target)
                        && ruleset_relevant(ruleset)
                })
                .collect::<Vec<_>>();
            if matching.len() > 1 {
                return Err(policy("multiple rulesets govern one approved target"));
            }
            let destination = matching
                .first()
                .and_then(|ruleset| ruleset.get("id"))
                .and_then(Value::as_u64)
                .map_or_else(
                    || endpoint("/rulesets"),
                    |id| endpoint(&format!("/rulesets/{id}")),
                );
            let method = if matching.is_empty() { "POST" } else { "PUT" };
            gh_input(method, &destination, wanted, evidence_path)?;
        }
    }
    if normalize_environment(&before.environment)? != normalize_environment(&desired.environment)? {
        let payload = environment_payload(&desired.environment)?;
        gh_input(
            "PUT",
            &endpoint("/environments/release"),
            &payload,
            evidence_path,
        )?;
    }
    if normalize_branch_policies(&before.branch_policies)?
        != normalize_branch_policies(&desired.branch_policies)?
    {
        let observed = before
            .branch_policies
            .get("branch_policies")
            .and_then(Value::as_array)
            .ok_or_else(|| policy("deployment branch policy readback is incomplete"))?;
        for policy_value in observed {
            let id = policy_value
                .get("id")
                .and_then(Value::as_u64)
                .ok_or_else(|| policy("deployment branch policy id is absent"))?;
            gh_empty(
                "DELETE",
                &endpoint(&format!(
                    "/environments/release/deployment-branch-policies/{id}"
                )),
            )?;
        }
        gh_input(
            "POST",
            &endpoint("/environments/release/deployment-branch-policies"),
            &json!({"name": "v*.*.*", "type": "tag"}),
            evidence_path,
        )?;
    }
    if normalize_immutable(&before.immutable_releases)?
        != normalize_immutable(&desired.immutable_releases)?
    {
        gh_empty("PUT", &endpoint("/immutable-releases"))?;
    }
    if normalize_repository(&before.repository_settings)?
        != normalize_repository(&desired.repository_settings)?
    {
        gh_input(
            "PATCH",
            &endpoint(""),
            &desired.repository_settings,
            evidence_path,
        )?;
    }
    Ok(())
}

fn environment_payload(environment: &Value) -> Result<Value, GateError> {
    let rule = environment
        .get("protection_rules")
        .and_then(Value::as_array)
        .and_then(|rules| rules.first())
        .ok_or_else(|| internal("desired environment reviewer rule is absent"))?;
    let reviewers = rule
        .get("reviewers")
        .and_then(Value::as_array)
        .ok_or_else(|| internal("desired environment reviewers are absent"))?
        .iter()
        .map(|reviewer| {
            Ok(json!({
                "type": reviewer.get("type").and_then(Value::as_str)
                    .ok_or_else(|| internal("desired reviewer type is absent"))?,
                "id": reviewer.pointer("/reviewer/id").and_then(Value::as_u64)
                    .ok_or_else(|| internal("desired reviewer id is absent"))?,
            }))
        })
        .collect::<Result<Vec<_>, GateError>>()?;
    Ok(json!({
        "wait_timer": 0,
        "prevent_self_review": rule.get("prevent_self_review"),
        "reviewers": reviewers,
        "deployment_branch_policy": environment.get("deployment_branch_policy"),
    }))
}

fn normalize_rulesets(value: &Value) -> Result<Value, GateError> {
    let rulesets = value
        .as_array()
        .ok_or_else(|| policy("ruleset readback must be an array"))?;
    let mut normalized = Vec::new();
    for ruleset in rulesets {
        let target = ruleset.get("target").and_then(Value::as_str);
        if !matches!(target, Some("branch" | "tag")) || !ruleset_relevant(ruleset) {
            continue;
        }
        let mut bypass = ruleset
            .get("bypass_actors")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| policy("ruleset bypass actors are absent"))?;
        bypass.sort_by_key(|actor| actor.get("actor_id").and_then(Value::as_u64));
        let mut rules = ruleset
            .get("rules")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| policy("ruleset rules are absent"))?;
        for rule in &mut rules {
            if let Some(checks) = rule
                .get_mut("parameters")
                .and_then(Value::as_object_mut)
                .and_then(|parameters| parameters.get_mut("required_status_checks"))
                .and_then(Value::as_array_mut)
            {
                checks.sort_by(|left, right| {
                    left.get("context")
                        .and_then(Value::as_str)
                        .cmp(&right.get("context").and_then(Value::as_str))
                });
            }
        }
        rules.sort_by(|left, right| {
            left.get("type")
                .and_then(Value::as_str)
                .cmp(&right.get("type").and_then(Value::as_str))
        });
        normalized.push(json!({
            "name": ruleset.get("name"),
            "target": target,
            "enforcement": ruleset.get("enforcement"),
            "bypass_actors": bypass,
            "conditions": ruleset.get("conditions"),
            "rules": rules,
        }));
    }
    normalized.sort_by(|left, right| {
        left.get("target")
            .and_then(Value::as_str)
            .cmp(&right.get("target").and_then(Value::as_str))
    });
    Ok(Value::Array(normalized))
}

fn ruleset_relevant(ruleset: &Value) -> bool {
    let target = ruleset.get("target").and_then(Value::as_str);
    let includes = ruleset
        .pointer("/conditions/ref_name/include")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str);
    includes.into_iter().any(|pattern| match target {
        Some("branch") => {
            pattern == "~DEFAULT_BRANCH" || wildcard_match(pattern.as_bytes(), b"refs/heads/main")
        }
        Some("tag") => wildcard_match(pattern.as_bytes(), b"refs/tags/v1.2.3"),
        _ => false,
    })
}

fn normalize_environment(value: &Value) -> Result<Value, GateError> {
    let rules = value
        .get("protection_rules")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("environment protection rules are absent"))?;
    if rules.len() != 1
        || rules[0].get("type").and_then(Value::as_str) != Some("required_reviewers")
    {
        return Err(policy(
            "environment must have exactly one required-reviewers rule",
        ));
    }
    let mut reviewers = rules[0]
        .get("reviewers")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("environment reviewers are absent"))?
        .iter()
        .map(|reviewer| {
            json!({
                "type": reviewer.get("type"),
                "id": reviewer.pointer("/reviewer/id"),
            })
        })
        .collect::<Vec<_>>();
    reviewers.sort_by_key(|reviewer| reviewer.get("id").and_then(Value::as_u64));
    Ok(json!({
        "name": required_value(value, "name", "environment readback")?,
        "protection_rules": [{
            "type": "required_reviewers",
            "prevent_self_review": required_value(&rules[0], "prevent_self_review", "environment readback")?,
            "reviewers": reviewers,
        }],
        "deployment_branch_policy": required_value(
            value,
            "deployment_branch_policy",
            "environment readback",
        )?,
    }))
}

fn normalize_branch_policies(value: &Value) -> Result<Value, GateError> {
    let mut policies = value
        .get("branch_policies")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| policy("deployment branch policies are absent"))?
        .into_iter()
        .map(|policy_value| {
            json!({
                "name": policy_value.get("name"),
                "type": policy_value.get("type"),
            })
        })
        .collect::<Vec<_>>();
    policies.sort_by(|left, right| {
        left.get("name")
            .and_then(Value::as_str)
            .cmp(&right.get("name").and_then(Value::as_str))
    });
    Ok(json!({"total_count": policies.len(), "branch_policies": policies}))
}

fn normalize_immutable(value: &Value) -> Result<Value, GateError> {
    Ok(json!({
        "enabled": required_value(value, "enabled", "immutable releases readback")?,
        "enforced_by_owner": required_value(
            value,
            "enforced_by_owner",
            "immutable releases readback",
        )?,
    }))
}

fn normalize_repository(value: &Value) -> Result<Value, GateError> {
    Ok(json!({
        "allow_squash_merge": required_value(value, "allow_squash_merge", "repository readback")?,
        "allow_merge_commit": required_value(value, "allow_merge_commit", "repository readback")?,
        "allow_rebase_merge": required_value(value, "allow_rebase_merge", "repository readback")?,
        "allow_update_branch": required_value(value, "allow_update_branch", "repository readback")?,
    }))
}

fn required_value<'a>(value: &'a Value, field: &str, scope: &str) -> Result<&'a Value, GateError> {
    value
        .get(field)
        .ok_or_else(|| policy(format!("{scope} is missing '{field}'")))
}

fn gh_input(
    method: &str,
    endpoint: &str,
    value: &Value,
    evidence_path: &Path,
) -> Result<(), GateError> {
    let directory = evidence_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(directory)
        .map_err(|error| GateError::io("create repository control evidence directory", &error))?;
    let mut input = NamedTempFile::new_in(directory)
        .map_err(|error| GateError::io("create repository API payload", &error))?;
    serde_json::to_writer(&mut input, value)
        .map_err(|_| internal("serialize repository API payload"))?;
    input
        .flush()
        .map_err(|error| GateError::io("flush repository API payload", &error))?;
    let path = input
        .path()
        .to_str()
        .ok_or_else(|| policy("repository API payload path is not UTF-8"))?;
    gh(&["api", "--method", method, endpoint, "--input", path])?;
    Ok(())
}

fn gh_empty(method: &str, endpoint: &str) -> Result<(), GateError> {
    gh(&["api", "--method", method, endpoint])?;
    Ok(())
}

fn gh_json(arguments: &[&str]) -> Result<Value, GateError> {
    let output = gh(arguments)?;
    serde_json::from_slice(&output)
        .map_err(|_| policy("GitHub repository response is not valid JSON"))
}

fn gh(arguments: &[&str]) -> Result<Vec<u8>, GateError> {
    let mut environment = BTreeMap::new();
    for name in [
        "PATH",
        "GH_TOKEN",
        "GITHUB_TOKEN",
        "NO_COLOR",
        "LANG",
        "LC_ALL",
    ] {
        if let Some(value) = std::env::var_os(name) {
            environment.insert(OsString::from(name), value);
        }
    }
    let mut command_arguments = Vec::with_capacity(arguments.len() + 2);
    for (index, argument) in arguments.iter().enumerate() {
        command_arguments.push(OsString::from(argument));
        if index == 0 && *argument == "api" {
            command_arguments.push(OsString::from("-H"));
            command_arguments.push(OsString::from("X-GitHub-Api-Version: 2026-03-10"));
        }
    }
    let result = child::run(ChildSpec {
        program: PathBuf::from("gh"),
        arguments: command_arguments,
        environment,
        timeout: Duration::from_secs(300),
        output_limit: MAX_CHILD_OUTPUT_BYTES,
    })?;
    if result.termination != Termination::Exited(Some(0))
        || result.stdout_truncated
        || result.stderr_truncated
    {
        return Err(policy(
            "authenticated GitHub repository operation failed or exceeded its bound",
        ));
    }
    Ok(result.stdout)
}

fn validate_repository_name(repository: &str) -> Result<(), GateError> {
    let Some((owner, name)) = repository.split_once('/') else {
        return Err(policy("repository must be owner/name"));
    };
    if owner.is_empty()
        || name.is_empty()
        || name.contains('/')
        || !repository
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/'))
    {
        return Err(policy("repository contains an invalid owner or name"));
    }
    Ok(())
}

fn read_json(path: &Path, operation: &'static str) -> Result<Value, GateError> {
    serde_json::from_slice(&read_bounded(path, operation, MAX_INPUT_BYTES)?)
        .map_err(|_| policy(format!("{operation} is not valid JSON")))
}

fn object_field<'a>(
    value: &'a Value,
    field: &str,
    scope: &str,
) -> Result<&'a Map<String, Value>, GateError> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| policy(format!("{scope} is missing object '{field}'")))
}

fn object_field_value<'a>(
    value: &'a Map<String, Value>,
    field: &str,
    scope: &str,
) -> Result<&'a Map<String, Value>, GateError> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| policy(format!("{scope} is missing object '{field}'")))
}

fn string_array(value: Option<&Value>, scope: &str) -> Result<Vec<String>, GateError> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| policy(format!("{scope} must be an array")))?
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| policy(format!("{scope} must contain strings")))
        })
        .collect()
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("repository.policy", detail)
}

fn internal(detail: impl Into<String>) -> GateError {
    GateError::internal("repository.internal", detail)
}

#[cfg(test)]
mod codeowners_tests {
    use super::codeowners_match;

    #[test]
    fn root_anchor_and_segment_wildcards_match_github_semantics() {
        assert!(codeowners_match("/Cargo.toml", "Cargo.toml"));
        assert!(!codeowners_match("/Cargo.toml", "nested/Cargo.toml"));
        assert!(codeowners_match("Cargo.toml", "nested/Cargo.toml"));
        assert!(codeowners_match(
            "/.github/workflows/**",
            ".github/workflows/release/publish.yml"
        ));
        assert!(codeowners_match("/docs/*.md", "docs/index.md"));
        assert!(!codeowners_match("/docs/*.md", "docs/guides/index.md"));
        assert!(codeowners_match("/release/", "release/inventory.toml"));
        assert!(!codeowners_match(
            "/release/",
            "nested/release/inventory.toml"
        ));
    }
}
