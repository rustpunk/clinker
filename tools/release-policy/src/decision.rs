//! Strict typed release decision and authorization validation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use chrono::DateTime;

use crate::canonical::{self, CanonicalValue};
use crate::digest;
use crate::error::GateError;
use crate::limits::{MAX_DECISION_RECORDS, MAX_INPUT_BYTES, MAX_SCHEMA_BYTES, read_bounded};

const DECISION_SCHEMA: &str = "clinker.decision/v1";
const AUTHORIZATION_SCHEMA: &str = "clinker.release-candidate-authorization/v1";
const AUTHORIZATION_ID: &str = "release-candidate-authorization";
const AUTHORIZATION_CONTROL: &str = "candidate-authorization";
const TARGETS: [&str; 4] = [
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
];
const IDENTITY_FIELDS: [&str; 14] = [
    "candidate_tag",
    "candidate_version",
    "source_sha",
    "build_workflow_sha",
    "publish_workflow_ref",
    "publish_workflow_ref_resolved_sha",
    "publish_workflow_sha",
    "candidate_release_id",
    "checksum_sha256",
    "archive_digests",
    "ci_run_ref",
    "changelog_ref",
    "inventory_ref",
    "authorized_release_maintainer_ref",
];
const AUTHORIZATION_IDENTITY_FIELDS: [&str; 9] = [
    "candidate_tag",
    "candidate_version",
    "source_sha",
    "publish_workflow_ref",
    "publish_workflow_ref_resolved_sha",
    "publish_workflow_sha",
    "changelog_ref",
    "inventory_ref",
    "authorized_release_maintainer_ref",
];

/// Fully preflighted decision-validation request.
#[derive(Debug, Clone)]
pub struct DecisionRequest {
    /// Decision schema used by every `record`.
    pub schema: Option<PathBuf>,
    /// One or more decision record documents.
    pub records: Vec<PathBuf>,
    /// Candidate authorization schema.
    pub authorization_schema: Option<PathBuf>,
    /// Separate candidate authorization record.
    pub authorization_record: Option<PathBuf>,
    /// Optional candidate evidence cross-check.
    pub candidate_evidence: Option<PathBuf>,
    /// Required decision identifiers.
    pub require_ids: Vec<String>,
    /// Required authorization identifier.
    pub require_authorization_id: Option<String>,
    /// Require a status-authorized candidate authorization.
    pub require_authorized: bool,
    /// Require the complete eight-record set and candidate evidence.
    pub require_complete: bool,
    /// Require every supplied decision record to be accepted.
    pub require_accepted: bool,
}

/// Validate the strict decision, authorization, and cross-artifact contract.
pub fn validate(request: &DecisionRequest) -> Result<(), GateError> {
    if let Some(path) = request.schema.as_deref() {
        validate_schema(path, DECISION_SCHEMA, "decision schema")?;
    }
    if let Some(path) = request.authorization_schema.as_deref() {
        validate_schema(path, AUTHORIZATION_SCHEMA, "authorization schema")?;
    }

    let authorization = request
        .authorization_record
        .as_deref()
        .map(|path| load_json(path, "authorization record"))
        .transpose()?;
    if let Some(value) = authorization.as_ref() {
        validate_authorization(
            value,
            request.require_authorized,
            request.require_authorization_id.as_deref(),
        )?;
    }

    let mut records = Vec::with_capacity(request.records.len());
    for path in &request.records {
        records.push(load_json(path, "decision record")?);
    }
    let candidate_evidence = request
        .candidate_evidence
        .as_deref()
        .map(|path| load_json(path, "candidate evidence"))
        .transpose()?;

    if !records.is_empty() {
        validate_record_set(
            &records,
            authorization.as_ref(),
            candidate_evidence.as_ref(),
            request.require_complete,
            request.require_accepted,
        )?;
        let ids: BTreeSet<&str> = records
            .iter()
            .filter_map(|record| record.as_object())
            .filter_map(|record| record.get("decision_id"))
            .filter_map(CanonicalValue::as_str)
            .collect();
        for required in &request.require_ids {
            if !ids.contains(required.as_str()) {
                return Err(policy(format!(
                    "required decision_id is missing: {required}"
                )));
            }
        }
    }
    Ok(())
}

fn load_json(path: &Path, operation: &'static str) -> Result<CanonicalValue, GateError> {
    canonical::parse_json(&read_bounded(path, operation, MAX_INPUT_BYTES)?)
}

fn validate_schema(path: &Path, expected: &str, operation: &'static str) -> Result<(), GateError> {
    let value = canonical::parse_json_with_limit(
        &read_bounded(path, operation, MAX_SCHEMA_BYTES)?,
        MAX_SCHEMA_BYTES,
    )?;
    let schema = object(&value, operation)?;
    if string(required(schema, "$id", operation)?, "$id")? != expected {
        return Err(policy(format!("{operation} $id must be {expected}")));
    }
    Ok(())
}

struct DecisionSpec {
    control: &'static str,
    selections: &'static [&'static str],
    completing: &'static [&'static str],
    extra: &'static [&'static str],
}

fn decision_spec(id: &str) -> Option<DecisionSpec> {
    match id {
        "semantic-identity" => Some(DecisionSpec {
            control: "identity-binding",
            selections: &["shared-primitive-now", "defer-shared-primitive"],
            completing: &["shared-primitive-now"],
            extra: &["semantic_identity"],
        }),
        "native-filesystem" => Some(DecisionSpec {
            control: "local-filesystem-safety",
            selections: &["existing-crates-features", "direct-platform-api"],
            completing: &["existing-crates-features", "direct-platform-api"],
            extra: &["platform_matrix"],
        }),
        "remote-share" => Some(DecisionSpec {
            control: "remote-filesystem-safety",
            selections: &["evidenced-matrix", "no-positive-remote-support"],
            completing: &["evidenced-matrix", "no-positive-remote-support"],
            extra: &["support_matrix"],
        }),
        "release-rules" => Some(DecisionSpec {
            control: "release-configuration",
            selections: &["strict-ruleset", "stop-incomplete"],
            completing: &["strict-ruleset"],
            extra: &["ruleset"],
        }),
        "release-environment" => Some(DecisionSpec {
            control: "release-environment",
            selections: &[
                "two-person-non-self",
                "single-maintainer-inspect-then-approve",
                "stop-incomplete",
            ],
            completing: &[
                "two-person-non-self",
                "single-maintainer-inspect-then-approve",
            ],
            extra: &["environment_policy"],
        }),
        "publication-policy" => Some(DecisionSpec {
            control: "publication-policy",
            selections: &["live-gate-required", "stop-incomplete"],
            completing: &["live-gate-required"],
            extra: &["publication_policy"],
        }),
        "release-candidate" => Some(DecisionSpec {
            control: "candidate-verification",
            selections: &["approve-exact-candidate", "stop-without-completion"],
            completing: &["approve-exact-candidate"],
            extra: &[
                "candidate_authorization_schema",
                "candidate_authorization_ref",
                "candidate_authorization_sha256",
                "candidate_tag_creation_ref",
                "candidate_tag_readback_ref",
                "approved_at",
                "authorization_recorded_at",
                "candidate_tag_created_at",
                "finalized_at",
            ],
        }),
        "publication-approval" => Some(DecisionSpec {
            control: "publication-approval",
            selections: &["approve-exact-candidate", "stop-without-completion"],
            completing: &["approve-exact-candidate"],
            extra: &["candidate_authorization_sha256", "candidate_evidence_ref"],
        }),
        _ => None,
    }
}

fn validate_record(value: &CanonicalValue) -> Result<(), GateError> {
    let record = object(value, "decision record")?;
    let id = string(
        required(record, "decision_id", "decision record")?,
        "decision_id",
    )?;
    let spec =
        decision_spec(id).ok_or_else(|| policy(format!("decision_id is not recognized: {id}")))?;
    let mut expected = vec![
        "schema",
        "decision_id",
        "control_id",
        "status",
        "selection",
        "approver_role_ref",
        "evidence_refs",
        "recorded_at",
    ];
    expected.extend_from_slice(spec.extra);
    if matches!(id, "release-candidate" | "publication-approval") {
        expected.extend_from_slice(&IDENTITY_FIELDS);
    }
    exact_keys(record, &expected, &format!("record {id}"))?;
    expect_string(record, "schema", DECISION_SCHEMA, id)?;
    expect_string(record, "control_id", spec.control, id)?;

    let status = string(required(record, "status", id)?, "status")?;
    if !["accepted", "rejected", "stopped", "incomplete"].contains(&status) {
        return Err(policy(format!("record {id} has invalid status")));
    }
    let selection = string(required(record, "selection", id)?, "selection")?;
    if !spec.selections.contains(&selection) {
        return Err(policy(format!("record {id} has invalid selection")));
    }
    if status == "accepted" && !spec.completing.contains(&selection) {
        return Err(policy(format!(
            "record {id} accepted selection must be completion-eligible"
        )));
    }
    nonempty_string(record, "approver_role_ref", id)?;
    evidence_refs(required(record, "evidence_refs", id)?, "evidence_refs")?;
    timestamp(required(record, "recorded_at", id)?, "recorded_at")?;

    match id {
        "semantic-identity" => validate_semantic(record),
        "native-filesystem" => validate_native(record),
        "remote-share" => validate_remote(record, selection),
        "release-rules" => validate_rules(record, selection),
        "release-environment" => validate_environment(record, selection),
        "publication-policy" => validate_publication_policy(record, selection),
        "release-candidate" => validate_candidate_record(record),
        "publication-approval" => validate_approval_record(record),
        _ => Err(GateError::internal(
            "decision.dispatch",
            "validated decision was not dispatched",
        )),
    }
}

fn validate_semantic(record: &BTreeMap<String, CanonicalValue>) -> Result<(), GateError> {
    let identity = object(
        required(record, "semantic_identity", "record")?,
        "semantic_identity",
    )?;
    exact_keys(
        identity,
        &["owner_crate", "pipeline_hash_substitution_forbidden"],
        "semantic_identity",
    )?;
    expect_string(identity, "owner_crate", "clinker-plan", "semantic_identity")?;
    expect_bool(
        identity,
        "pipeline_hash_substitution_forbidden",
        true,
        "semantic_identity",
    )
}

fn validate_native(record: &BTreeMap<String, CanonicalValue>) -> Result<(), GateError> {
    let matrix = object(
        required(record, "platform_matrix", "record")?,
        "platform_matrix",
    )?;
    exact_keys(matrix, &["linux", "macos", "windows"], "platform_matrix")?;
    for platform in ["linux", "macos", "windows"] {
        let entry = object(required(matrix, platform, "platform_matrix")?, platform)?;
        exact_keys(entry, &["primitive", "unavailable_behavior"], platform)?;
        nonempty_string(entry, "primitive", platform)?;
        expect_string(entry, "unavailable_behavior", "policy_required", platform)?;
    }
    Ok(())
}

fn validate_remote(
    record: &BTreeMap<String, CanonicalValue>,
    selection: &str,
) -> Result<(), GateError> {
    let matrix = object(
        required(record, "support_matrix", "record")?,
        "support_matrix",
    )?;
    exact_keys(
        matrix,
        &["policy", "combinations", "unlisted_behavior"],
        "support_matrix",
    )?;
    expect_string(matrix, "policy", selection, "support_matrix")?;
    expect_string(
        matrix,
        "unlisted_behavior",
        "policy_required",
        "support_matrix",
    )?;
    let combinations = string_array(
        required(matrix, "combinations", "support_matrix")?,
        "combinations",
    )?;
    unique(&combinations, "support_matrix.combinations")?;
    if (selection == "evidenced-matrix" && combinations.is_empty())
        || (selection == "no-positive-remote-support" && !combinations.is_empty())
    {
        return Err(policy(
            "support_matrix.combinations conflicts with selection",
        ));
    }
    Ok(())
}

fn validate_rules(
    record: &BTreeMap<String, CanonicalValue>,
    selection: &str,
) -> Result<(), GateError> {
    let rules = object(required(record, "ruleset", "record")?, "ruleset")?;
    if selection == "stop-incomplete" {
        exact_keys(rules, &["blocking_reason"], "ruleset")?;
        nonempty_string(rules, "blocking_reason", "ruleset")?;
        return Ok(());
    }
    exact_keys(
        rules,
        &["main_rule", "tag_rule", "actions_policy", "release_policy"],
        "ruleset",
    )?;
    let main = object(
        required(rules, "main_rule", "ruleset")?,
        "ruleset.main_rule",
    )?;
    exact_keys(
        main,
        &[
            "branch",
            "enforcement",
            "required_status_checks",
            "strict_status_checks",
            "required_approving_review_count",
            "codeowner_review_required",
            "latest_push_approval_required",
            "conversation_resolution_required",
            "linear_history_required",
            "allowed_merge_methods",
            "update_branch_enabled",
            "bypass_actor_refs",
            "generic_admin_bypass",
        ],
        "ruleset.main_rule",
    )?;
    expect_string(main, "branch", "main", "ruleset.main_rule")?;
    expect_string(main, "enforcement", "active", "ruleset.main_rule")?;
    for field in [
        "strict_status_checks",
        "codeowner_review_required",
        "latest_push_approval_required",
        "conversation_resolution_required",
        "linear_history_required",
        "update_branch_enabled",
    ] {
        expect_bool(main, field, true, "ruleset.main_rule")?;
    }
    expect_bool(main, "generic_admin_bypass", false, "ruleset.main_rule")?;
    if required(main, "required_approving_review_count", "ruleset.main_rule")?
        .as_u64()
        .filter(|count| *count >= 1)
        .is_none()
    {
        return Err(policy("ruleset.main_rule requires at least one approval"));
    }
    let merge = string_array(
        required(main, "allowed_merge_methods", "ruleset.main_rule")?,
        "allowed_merge_methods",
    )?;
    if merge != ["squash"] {
        return Err(policy("ruleset.main_rule must allow squash merge only"));
    }
    let bypass = string_array(
        required(main, "bypass_actor_refs", "ruleset.main_rule")?,
        "bypass_actor_refs",
    )?;
    unique(&bypass, "bypass_actor_refs")?;
    if bypass.is_empty() {
        return Err(policy("bypass_actor_refs must not be empty"));
    }
    let checks = array(
        required(main, "required_status_checks", "ruleset.main_rule")?,
        "required_status_checks",
    )?;
    if checks.is_empty() {
        return Err(policy("required_status_checks must not be empty"));
    }
    let mut contexts = Vec::with_capacity(checks.len());
    for (index, check) in checks.iter().enumerate() {
        let check = object(check, "required status check")?;
        exact_keys(check, &["context", "app_ref"], "required status check")?;
        contexts.push(nonempty_string(check, "context", "required status check")?);
        nonempty_string(check, "app_ref", &format!("required status check {index}"))?;
    }
    unique(&contexts, "required status check contexts")?;

    let tag = object(required(rules, "tag_rule", "ruleset")?, "ruleset.tag_rule")?;
    exact_keys(
        tag,
        &[
            "tag_pattern",
            "enforcement",
            "creation_actor_refs",
            "update_allowed",
            "deletion_allowed",
            "generic_admin_bypass",
        ],
        "ruleset.tag_rule",
    )?;
    expect_string(tag, "tag_pattern", "v<semver>", "ruleset.tag_rule")?;
    expect_string(tag, "enforcement", "active", "ruleset.tag_rule")?;
    for field in ["update_allowed", "deletion_allowed", "generic_admin_bypass"] {
        expect_bool(tag, field, false, "ruleset.tag_rule")?;
    }
    let actors = string_array(
        required(tag, "creation_actor_refs", "ruleset.tag_rule")?,
        "creation_actor_refs",
    )?;
    if as_set(&actors) != as_set(&bypass) {
        return Err(policy("tag creation actors must match the bypass set"));
    }

    let actions = object(
        required(rules, "actions_policy", "ruleset")?,
        "ruleset.actions_policy",
    )?;
    exact_keys(
        actions,
        &[
            "full_sha_pinning_required",
            "workflow_codeowner_review_required",
        ],
        "ruleset.actions_policy",
    )?;
    expect_bool(actions, "full_sha_pinning_required", true, "actions_policy")?;
    expect_bool(
        actions,
        "workflow_codeowner_review_required",
        true,
        "actions_policy",
    )?;
    let release = object(
        required(rules, "release_policy", "ruleset")?,
        "ruleset.release_policy",
    )?;
    exact_keys(
        release,
        &["immutable_releases_required", "mismatch_recovery"],
        "ruleset.release_policy",
    )?;
    expect_bool(
        release,
        "immutable_releases_required",
        true,
        "release_policy",
    )?;
    expect_string(
        release,
        "mismatch_recovery",
        "new-version-only",
        "release_policy",
    )
}

fn validate_environment(
    record: &BTreeMap<String, CanonicalValue>,
    selection: &str,
) -> Result<(), GateError> {
    let policy_value = required(record, "environment_policy", "record")?;
    let environment = object(policy_value, "environment_policy")?;
    if selection == "stop-incomplete" {
        exact_keys(environment, &["blocking_reason"], "environment_policy")?;
        nonempty_string(environment, "blocking_reason", "environment_policy")?;
        return Ok(());
    }
    let mut expected = vec![
        "approval_mode",
        "environment",
        "workflow",
        "job",
        "approval_kind",
        "automated_approval",
        "generic_admin_bypass",
        "prevent_self_review",
        "maintainer_actor_refs",
        "trigger_actor_ref",
        "deployment_restriction",
        "concurrency",
        "inspection_contract",
        "approval_contract",
        "ordering_contract",
        "draft_policy",
    ];
    if selection == "single-maintainer-inspect-then-approve" {
        expected.push("two_person_unavailable_reason");
    }
    exact_keys(environment, &expected, "environment_policy")?;
    expect_string(
        environment,
        "approval_mode",
        selection,
        "environment_policy",
    )?;
    expect_string(environment, "environment", "release", "environment_policy")?;
    expect_string(
        environment,
        "workflow",
        ".github/workflows/publish-release.yml",
        "environment_policy",
    )?;
    expect_string(
        environment,
        "job",
        "publish-approved-release",
        "environment_policy",
    )?;
    expect_string(environment, "approval_kind", "manual", "environment_policy")?;
    expect_bool(
        environment,
        "automated_approval",
        false,
        "environment_policy",
    )?;
    expect_bool(
        environment,
        "generic_admin_bypass",
        false,
        "environment_policy",
    )?;
    nonempty_string(environment, "trigger_actor_ref", "environment_policy")?;
    let maintainers = string_array(
        required(environment, "maintainer_actor_refs", "environment_policy")?,
        "maintainer_actor_refs",
    )?;
    unique(&maintainers, "maintainer_actor_refs")?;

    let deployment = object(
        required(environment, "deployment_restriction", "environment_policy")?,
        "deployment_restriction",
    )?;
    exact_keys(
        deployment,
        &["ref_type", "tag_pattern", "branches_allowed"],
        "deployment_restriction",
    )?;
    expect_string(deployment, "ref_type", "tag", "deployment_restriction")?;
    expect_string(
        deployment,
        "tag_pattern",
        "v<semver>",
        "deployment_restriction",
    )?;
    expect_bool(
        deployment,
        "branches_allowed",
        false,
        "deployment_restriction",
    )?;

    let concurrency = object(
        required(environment, "concurrency", "environment_policy")?,
        "concurrency",
    )?;
    exact_keys(
        concurrency,
        &["scope", "max_in_progress", "redispatch_allowed"],
        "concurrency",
    )?;
    expect_string(concurrency, "scope", "candidate-tag", "concurrency")?;
    if required(concurrency, "max_in_progress", "concurrency")?.as_u64() != Some(1) {
        return Err(policy("concurrency.max_in_progress must equal one"));
    }
    expect_bool(concurrency, "redispatch_allowed", false, "concurrency")?;

    validate_contract_fields(
        environment,
        "inspection_contract",
        &[
            "run_id",
            "job_id",
            "environment",
            "candidate_tag",
            "inspector_actor_ref",
            "inspection_started_at",
            "inspection_completed_at",
            "inspection_evidence_ref",
        ],
        "eligible-maintainer",
    )?;
    let approval_actor_rule = if selection == "two-person-non-self" {
        "eligible-maintainer-distinct-from-trigger"
    } else {
        "configured-maintainer"
    };
    validate_contract_fields(
        environment,
        "approval_contract",
        &[
            "run_id",
            "job_id",
            "environment",
            "candidate_tag",
            "approver_actor_ref",
            "approved_at",
            "approval_receipt_ref",
            "approval_kind",
            "automated_approval",
        ],
        approval_actor_rule,
    )?;

    let ordering = object(
        required(environment, "ordering_contract", "environment_policy")?,
        "ordering_contract",
    )?;
    exact_keys(
        ordering,
        &[
            "shared_identity_fields",
            "timestamp_order",
            "strictly_increasing",
        ],
        "ordering_contract",
    )?;
    expect_string_array(
        ordering,
        "shared_identity_fields",
        &["run_id", "job_id", "environment", "candidate_tag"],
    )?;
    expect_string_array(
        ordering,
        "timestamp_order",
        &[
            "inspection_started_at",
            "inspection_completed_at",
            "approved_at",
        ],
    )?;
    expect_bool(ordering, "strictly_increasing", true, "ordering_contract")?;

    let draft = object(
        required(environment, "draft_policy", "environment_policy")?,
        "draft_policy",
    )?;
    exact_keys(
        draft,
        &[
            "exact_final_reread_required",
            "byte_identical_partial_reconciliation_only",
            "already_published_exact_match_behavior",
            "mismatch_recovery",
        ],
        "draft_policy",
    )?;
    expect_bool(draft, "exact_final_reread_required", true, "draft_policy")?;
    expect_bool(
        draft,
        "byte_identical_partial_reconciliation_only",
        true,
        "draft_policy",
    )?;
    expect_string(
        draft,
        "already_published_exact_match_behavior",
        "verification-only",
        "draft_policy",
    )?;
    expect_string(
        draft,
        "mismatch_recovery",
        "new-version-only",
        "draft_policy",
    )?;

    if selection == "two-person-non-self" {
        expect_bool(
            environment,
            "prevent_self_review",
            true,
            "environment_policy",
        )?;
        if maintainers.len() < 2 {
            return Err(policy("two-person mode requires at least two maintainers"));
        }
    } else {
        expect_bool(
            environment,
            "prevent_self_review",
            false,
            "environment_policy",
        )?;
        if maintainers.len() != 1 {
            return Err(policy("single-maintainer mode requires one maintainer"));
        }
        nonempty_string(
            environment,
            "two_person_unavailable_reason",
            "environment_policy",
        )?;
    }
    Ok(())
}

fn validate_contract_fields(
    environment: &BTreeMap<String, CanonicalValue>,
    field: &str,
    required_fields: &[&str],
    actor_rule: &str,
) -> Result<(), GateError> {
    let contract = object(required(environment, field, "environment_policy")?, field)?;
    exact_keys(
        contract,
        &[
            "required_fields",
            "exact_run_candidate_required",
            "actor_rule",
        ],
        field,
    )?;
    expect_string_array(contract, "required_fields", required_fields)?;
    expect_bool(contract, "exact_run_candidate_required", true, field)?;
    expect_string(contract, "actor_rule", actor_rule, field)
}

fn validate_publication_policy(
    record: &BTreeMap<String, CanonicalValue>,
    selection: &str,
) -> Result<(), GateError> {
    let policy_value = required(record, "publication_policy", "record")?;
    let publication = object(policy_value, "publication_policy")?;
    if selection == "stop-incomplete" {
        exact_keys(publication, &["blocking_reason"], "publication_policy")?;
        nonempty_string(publication, "blocking_reason", "publication_policy")?;
        return Ok(());
    }
    exact_keys(
        publication,
        &[
            "required_path",
            "immutable_publication",
            "repair_in_place_forbidden",
        ],
        "publication_policy",
    )?;
    expect_string_array(
        publication,
        "required_path",
        &[
            "release-candidate",
            "candidate-readback",
            "publication-approval",
            "public-release-readback",
        ],
    )?;
    expect_bool(
        publication,
        "immutable_publication",
        true,
        "publication_policy",
    )?;
    expect_bool(
        publication,
        "repair_in_place_forbidden",
        true,
        "publication_policy",
    )
}

fn validate_candidate_record(record: &BTreeMap<String, CanonicalValue>) -> Result<(), GateError> {
    validate_candidate_identity(record, "release candidate")?;
    expect_string(
        record,
        "candidate_authorization_schema",
        AUTHORIZATION_SCHEMA,
        "release candidate",
    )?;
    evidence_ref(
        required(record, "candidate_authorization_ref", "record")?,
        "candidate_authorization_ref",
    )?;
    digest64(
        required(record, "candidate_authorization_sha256", "record")?,
        "candidate_authorization_sha256",
    )?;
    let creation = evidence_ref(
        required(record, "candidate_tag_creation_ref", "record")?,
        "candidate_tag_creation_ref",
    )?;
    let readback = evidence_ref(
        required(record, "candidate_tag_readback_ref", "record")?,
        "candidate_tag_readback_ref",
    )?;
    if creation == readback {
        return Err(policy(
            "candidate tag creation and readback refs must differ",
        ));
    }
    let approved = timestamp(required(record, "approved_at", "record")?, "approved_at")?;
    let authorized = timestamp(
        required(record, "authorization_recorded_at", "record")?,
        "authorization_recorded_at",
    )?;
    let created = timestamp(
        required(record, "candidate_tag_created_at", "record")?,
        "candidate_tag_created_at",
    )?;
    let finalized = timestamp(required(record, "finalized_at", "record")?, "finalized_at")?;
    let recorded = timestamp(required(record, "recorded_at", "record")?, "recorded_at")?;
    if !(approved <= authorized && authorized <= created && created <= finalized) {
        return Err(policy(
            "release-candidate authorization and tag timestamps are out of order",
        ));
    }
    if finalized != recorded {
        return Err(policy(
            "release-candidate finalized_at must equal recorded_at",
        ));
    }
    Ok(())
}

fn validate_approval_record(record: &BTreeMap<String, CanonicalValue>) -> Result<(), GateError> {
    validate_candidate_identity(record, "publication approval")?;
    digest64(
        required(record, "candidate_authorization_sha256", "record")?,
        "candidate_authorization_sha256",
    )?;
    evidence_ref(
        required(record, "candidate_evidence_ref", "record")?,
        "candidate_evidence_ref",
    )?;
    let canonical = CanonicalValue::Object(record.clone());
    canonical::to_bytes(&canonical)?;
    Ok(())
}

fn validate_authorization(
    value: &CanonicalValue,
    require_authorized: bool,
    required_id: Option<&str>,
) -> Result<(), GateError> {
    let artifact = object(value, "authorization artifact")?;
    exact_keys(
        artifact,
        &[
            "schema",
            "decision_id",
            "control_id",
            "status",
            "selection",
            "approver_role_ref",
            "evidence_refs",
            "recorded_at",
            "authorization",
            "candidate_authorization_sha256",
        ],
        "authorization artifact",
    )?;
    expect_string(
        artifact,
        "schema",
        AUTHORIZATION_SCHEMA,
        "authorization artifact",
    )?;
    expect_string(
        artifact,
        "decision_id",
        required_id.unwrap_or(AUTHORIZATION_ID),
        "authorization artifact",
    )?;
    expect_string(
        artifact,
        "control_id",
        AUTHORIZATION_CONTROL,
        "authorization artifact",
    )?;
    let status = nonempty_string(artifact, "status", "authorization artifact")?;
    if !["authorized", "rejected", "stopped", "incomplete"].contains(&status.as_str()) {
        return Err(policy("authorization status is invalid"));
    }
    nonempty_string(artifact, "selection", "authorization artifact")?;
    nonempty_string(artifact, "approver_role_ref", "authorization artifact")?;
    evidence_refs(
        required(artifact, "evidence_refs", "authorization artifact")?,
        "evidence_refs",
    )?;
    timestamp(
        required(artifact, "recorded_at", "authorization artifact")?,
        "recorded_at",
    )?;
    let nested_value = required(artifact, "authorization", "authorization artifact")?;
    let nested = object(nested_value, "authorization")?;
    let mut expected = vec![
        "status",
        "selection",
        "approver_role_ref",
        "evidence_refs",
        "recorded_at",
    ];
    if status == "authorized" {
        expected.extend_from_slice(&AUTHORIZATION_IDENTITY_FIELDS);
        expected.push("approved_at");
    }
    exact_keys(nested, &expected, "authorization")?;
    for field in [
        "status",
        "selection",
        "approver_role_ref",
        "evidence_refs",
        "recorded_at",
    ] {
        if artifact.get(field) != nested.get(field) {
            return Err(policy(format!(
                "authorization top-level {field} must equal nested {field}"
            )));
        }
    }
    if status == "authorized" {
        expect_string(
            artifact,
            "selection",
            "approve-exact-candidate",
            "authorization",
        )?;
        let approved = timestamp(
            required(nested, "approved_at", "authorization")?,
            "approved_at",
        )?;
        let recorded = timestamp(
            required(nested, "recorded_at", "authorization")?,
            "recorded_at",
        )?;
        if approved > recorded {
            return Err(policy(
                "authorization approved_at must not follow recorded_at",
            ));
        }
        validate_authorization_identity(nested)?;
    } else {
        expect_string(
            artifact,
            "selection",
            "stop-without-completion",
            "authorization",
        )?;
    }
    let expected_digest = digest::sha256_hex(&canonical::to_bytes(nested_value)?);
    let actual_digest = digest64(
        required(
            artifact,
            "candidate_authorization_sha256",
            "authorization artifact",
        )?,
        "candidate_authorization_sha256",
    )?;
    if !actual_digest.eq_ignore_ascii_case(&expected_digest) {
        return Err(policy("candidate authorization digest mismatch"));
    }
    if require_authorized && status != "authorized" {
        return Err(policy("authorization must have status authorized"));
    }
    Ok(())
}

fn validate_record_set(
    records: &[CanonicalValue],
    authorization: Option<&CanonicalValue>,
    candidate_evidence: Option<&CanonicalValue>,
    require_complete: bool,
    require_accepted: bool,
) -> Result<(), GateError> {
    if records.is_empty() || records.len() > MAX_DECISION_RECORDS {
        return Err(policy(format!(
            "decision record count must be between 1 and {MAX_DECISION_RECORDS}"
        )));
    }
    let mut by_id = BTreeMap::new();
    for value in records {
        validate_record(value)?;
        let record = object(value, "decision record")?;
        let id = string(required(record, "decision_id", "record")?, "decision_id")?;
        if by_id.insert(id, record).is_some() {
            return Err(policy(format!("duplicate decision record: {id}")));
        }
    }
    if require_complete {
        let expected: BTreeSet<&str> = [
            "semantic-identity",
            "native-filesystem",
            "remote-share",
            "release-rules",
            "release-environment",
            "publication-policy",
            "release-candidate",
            "publication-approval",
        ]
        .into_iter()
        .collect();
        if by_id.keys().copied().collect::<BTreeSet<_>>() != expected {
            return Err(policy(
                "complete decision record set does not contain exactly eight decisions",
            ));
        }
    }
    if require_accepted {
        for (id, record) in &by_id {
            if string(required(record, "status", id)?, "status")? != "accepted" {
                return Err(policy(format!("record {id} must have status accepted")));
            }
        }
    }
    if require_complete {
        for (id, record) in &by_id {
            let selection = string(required(record, "selection", id)?, "selection")?;
            let spec = decision_spec(id).ok_or_else(|| {
                GateError::internal("decision.spec", "validated decision spec disappeared")
            })?;
            if !spec.completing.contains(&selection) {
                return Err(policy(format!("record {id} is not completion-eligible")));
            }
        }
    }

    if let Some(candidate) = by_id.get("release-candidate")
        && string(required(candidate, "status", "candidate")?, "status")? == "accepted"
    {
        let authority = authorization.ok_or_else(|| {
            policy("accepted release candidate requires independent authorization")
        })?;
        validate_authorization(authority, true, None)?;
        let artifact = object(authority, "authorization artifact")?;
        let nested = object(
            required(artifact, "authorization", "authorization artifact")?,
            "authorization",
        )?;
        compare_authorization_identity(nested, candidate, "release candidate")?;
        compare_field(
            artifact,
            candidate,
            "candidate_authorization_sha256",
            "release candidate",
        )?;
        compare_field(nested, candidate, "approved_at", "release candidate")?;
        if artifact.get("recorded_at") != candidate.get("authorization_recorded_at") {
            return Err(policy(
                "release-candidate authorization_recorded_at must match authorization",
            ));
        }

        if let Some(evidence) = candidate_evidence {
            validate_candidate_evidence(evidence)?;
            let evidence = object(evidence, "candidate evidence")?;
            compare_identity(candidate, evidence, "candidate evidence")?;
            compare_field(
                artifact,
                evidence,
                "candidate_authorization_sha256",
                "candidate evidence",
            )?;
        } else if require_complete {
            return Err(policy(
                "complete decision validation requires candidate evidence",
            ));
        }
        if let Some(approval) = by_id.get("publication-approval") {
            compare_identity(candidate, approval, "publication approval")?;
            compare_field(
                artifact,
                approval,
                "candidate_authorization_sha256",
                "publication approval",
            )?;
        } else if require_complete {
            return Err(policy(
                "complete decision validation requires publication approval",
            ));
        }
    }

    if let (Some(environment), Some(approval)) = (
        by_id.get("release-environment"),
        by_id.get("publication-approval"),
    ) {
        let environment_policy = object(
            required(environment, "environment_policy", "environment")?,
            "environment_policy",
        )?;
        let maintainers = string_array(
            required(
                environment_policy,
                "maintainer_actor_refs",
                "environment_policy",
            )?,
            "maintainer_actor_refs",
        )?;
        let approver = string(
            required(approval, "approver_role_ref", "approval")?,
            "approver_role_ref",
        )?;
        if !maintainers.iter().any(|candidate| candidate == approver) {
            return Err(policy(
                "publication-approval approver is not an eligible environment maintainer",
            ));
        }
        let mode = string(
            required(environment_policy, "approval_mode", "environment_policy")?,
            "approval_mode",
        )?;
        if mode == "two-person-non-self" {
            let trigger = string(
                required(
                    environment_policy,
                    "trigger_actor_ref",
                    "environment_policy",
                )?,
                "trigger_actor_ref",
            )?;
            if approver == trigger {
                return Err(policy(
                    "publication-approval approver must differ from trigger actor",
                ));
            }
        } else if approver != maintainers[0] {
            return Err(policy(
                "publication-approval approver must be the configured single maintainer",
            ));
        }
    }
    Ok(())
}

fn validate_candidate_evidence(value: &CanonicalValue) -> Result<(), GateError> {
    let evidence = object(value, "candidate evidence")?;
    let mut expected = vec![
        "schema",
        "kind",
        "state",
        "revision",
        "release_status",
        "completion_eligible",
        "immutable_authority_sha256",
        "build_workflow_path",
        "build_run_id",
        "build_head_sha",
        "publish_workflow_path",
        "archives",
        "attestations",
        "assets",
        "tag_mutation_performed",
        "tag_readback_ref",
        "release_trigger_event_ref",
        "candidate_authorization_sha256",
    ];
    expected.extend_from_slice(&IDENTITY_FIELDS);
    exact_keys(evidence, &expected, "candidate evidence")?;
    expect_string(
        evidence,
        "schema",
        "clinker.candidate-evidence/v1",
        "candidate evidence",
    )?;
    expect_string(evidence, "kind", "candidate", "candidate evidence")?;
    expect_string(
        evidence,
        "state",
        "candidate-verified",
        "candidate evidence",
    )?;
    if required(evidence, "revision", "candidate evidence")?.as_u64() != Some(0) {
        return Err(policy("candidate evidence.revision must equal 0"));
    }
    expect_string(
        evidence,
        "release_status",
        "incomplete",
        "candidate evidence",
    )?;
    expect_bool(evidence, "completion_eligible", false, "candidate evidence")?;
    expect_bool(
        evidence,
        "tag_mutation_performed",
        false,
        "candidate evidence",
    )?;
    evidence_ref(
        required(evidence, "tag_readback_ref", "candidate evidence")?,
        "tag_readback_ref",
    )?;
    evidence_ref(
        required(evidence, "release_trigger_event_ref", "candidate evidence")?,
        "release_trigger_event_ref",
    )?;
    digest64(
        required(
            evidence,
            "candidate_authorization_sha256",
            "candidate evidence",
        )?,
        "candidate_authorization_sha256",
    )?;
    digest64(
        required(evidence, "immutable_authority_sha256", "candidate evidence")?,
        "immutable_authority_sha256",
    )?;
    if evidence.get("immutable_authority_sha256") != evidence.get("candidate_authorization_sha256")
    {
        return Err(policy(
            "candidate immutable authority must equal candidate authorization digest",
        ));
    }
    expect_string(
        evidence,
        "build_workflow_path",
        ".github/workflows/release.yml",
        "candidate evidence",
    )?;
    expect_string(
        evidence,
        "publish_workflow_path",
        ".github/workflows/publish-release.yml",
        "candidate evidence",
    )?;
    nonempty_string(evidence, "build_run_id", "candidate evidence")?;
    if evidence.get("build_head_sha") != evidence.get("source_sha") {
        return Err(policy(
            "candidate evidence.build_head_sha must equal source_sha",
        ));
    }
    validate_candidate_release_entries(evidence)?;
    validate_candidate_identity(evidence, "candidate evidence")
}

fn validate_candidate_release_entries(
    evidence: &BTreeMap<String, CanonicalValue>,
) -> Result<(), GateError> {
    let archives = required(evidence, "archives", "candidate evidence")?
        .as_array()
        .ok_or_else(|| policy("candidate evidence.archives must be an array"))?;
    let attestations = required(evidence, "attestations", "candidate evidence")?
        .as_array()
        .ok_or_else(|| policy("candidate evidence.attestations must be an array"))?;
    if archives.len() != TARGETS.len() || attestations.len() != TARGETS.len() {
        return Err(policy(
            "candidate archives and attestations must each contain exactly four entries",
        ));
    }
    let assets = required(evidence, "assets", "candidate evidence")?
        .as_array()
        .ok_or_else(|| policy("candidate evidence.assets must be an array"))?;
    if assets.len() != 13 {
        return Err(policy(
            "candidate assets must contain exactly 13 governed entries",
        ));
    }
    let mut asset_names = BTreeSet::new();
    let mut aggregate = 0_u64;
    for (index, asset) in assets.iter().enumerate() {
        let label = format!("candidate evidence.assets[{index}]");
        let asset = object(asset, &label)?;
        exact_keys(asset, &["name", "length", "sha256"], &label)?;
        let name = nonempty_string(asset, "name", &label)?;
        if !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            || !asset_names.insert(name)
        {
            return Err(policy(
                "candidate asset names must be unique safe identifiers",
            ));
        }
        let length = required(asset, "length", &label)?
            .as_u64()
            .ok_or_else(|| policy("candidate asset length must be an integer"))?;
        if length > 512 * 1024 * 1024 {
            return Err(policy("candidate asset exceeds its byte limit"));
        }
        aggregate = aggregate
            .checked_add(length)
            .ok_or_else(|| policy("candidate asset sizes overflowed"))?;
        if aggregate > 1024 * 1024 * 1024 {
            return Err(policy("candidate asset set exceeds its byte limit"));
        }
        digest64(required(asset, "sha256", &label)?, "candidate asset sha256")?;
    }
    let digests = object(
        required(evidence, "archive_digests", "candidate evidence")?,
        "candidate evidence.archive_digests",
    )?;
    let version = nonempty_string(evidence, "candidate_version", "candidate evidence")?;
    let source = required(evidence, "source_sha", "candidate evidence")?;
    let tag = nonempty_string(evidence, "candidate_tag", "candidate evidence")?;
    let mut archive_names = BTreeSet::new();
    let mut targets = BTreeSet::new();
    for (index, archive) in archives.iter().enumerate() {
        let label = format!("candidate evidence.archives[{index}]");
        let archive = object(archive, &label)?;
        exact_keys(archive, &["target", "archive_name", "sha256"], &label)?;
        let target = nonempty_string(archive, "target", &label)?;
        if !TARGETS.contains(&target.as_str()) || !targets.insert(target.clone()) {
            return Err(policy(
                "candidate archives must cover each governed target exactly once",
            ));
        }
        let extension = if target == "x86_64-pc-windows-msvc" {
            "zip"
        } else {
            "tar.gz"
        };
        let expected_name = format!("clinker-v{version}-{target}.{extension}");
        let name = nonempty_string(archive, "archive_name", &label)?;
        if name != expected_name || !archive_names.insert(name) {
            return Err(policy(
                "candidate archive names must be unique inventory-derived identities",
            ));
        }
        if archive.get("sha256") != digests.get(&target) {
            return Err(policy(
                "candidate archive digest must match archive_digests authority",
            ));
        }
        digest64(required(archive, "sha256", &label)?, "archive sha256")?;
    }
    let mut governed_asset_names = BTreeSet::from(["SHA256SUMS".to_owned()]);
    for archive_name in &archive_names {
        governed_asset_names.insert(archive_name.clone());
        governed_asset_names.insert(format!("{archive_name}.sha256"));
        governed_asset_names.insert(format!("{archive_name}.intoto.jsonl"));
    }
    if asset_names != governed_asset_names {
        return Err(policy(
            "candidate assets must cover the governed checksum, archives, checksum sidecars, and provenance statements exactly",
        ));
    }
    let mut attested_names = BTreeSet::new();
    for (index, attestation) in attestations.iter().enumerate() {
        let label = format!("candidate evidence.attestations[{index}]");
        let attestation = object(attestation, &label)?;
        exact_keys(
            attestation,
            &[
                "archive_name",
                "subject_sha256",
                "repository",
                "workflow",
                "ref",
                "source_sha",
                "runner_environment",
            ],
            &label,
        )?;
        let name = nonempty_string(attestation, "archive_name", &label)?;
        if !archive_names.contains(&name) || !attested_names.insert(name) {
            return Err(policy(
                "candidate attestations must cover each archive name exactly once",
            ));
        }
        expect_string(attestation, "repository", "rustpunk/clinker", &label)?;
        expect_string(
            attestation,
            "workflow",
            ".github/workflows/release.yml",
            &label,
        )?;
        expect_string(attestation, "runner_environment", "github-hosted", &label)?;
        expect_string(attestation, "ref", &format!("refs/tags/{tag}"), &label)?;
        if attestation.get("source_sha") != Some(source) {
            return Err(policy(
                "candidate attestation source_sha must equal candidate source",
            ));
        }
        let archive = archives
            .iter()
            .filter_map(CanonicalValue::as_object)
            .find(|archive| archive.get("archive_name") == attestation.get("archive_name"))
            .ok_or_else(|| policy("candidate attestation archive is unknown"))?;
        if attestation.get("subject_sha256") != archive.get("sha256") {
            return Err(policy(
                "candidate attestation digest must equal archive digest",
            ));
        }
        digest64(
            required(attestation, "subject_sha256", &label)?,
            "attestation subject_sha256",
        )?;
    }
    Ok(())
}

fn validate_candidate_identity(
    value: &BTreeMap<String, CanonicalValue>,
    field: &str,
) -> Result<(), GateError> {
    let tag = nonempty_string(value, "candidate_tag", field)?;
    if !valid_semver_tag(&tag) {
        return Err(policy(format!("{field}.candidate_tag must be v<semver>")));
    }
    expect_string(value, "candidate_version", &tag[1..], field)?;
    let source = sha40(required(value, "source_sha", field)?, "source_sha")?;
    let build = sha40(
        required(value, "build_workflow_sha", field)?,
        "build_workflow_sha",
    )?;
    let resolved = sha40(
        required(value, "publish_workflow_ref_resolved_sha", field)?,
        "publish_workflow_ref_resolved_sha",
    )?;
    let publish = sha40(
        required(value, "publish_workflow_sha", field)?,
        "publish_workflow_sha",
    )?;
    expect_string(value, "publish_workflow_ref", &tag, field)?;
    if build != source || resolved != publish || publish != source {
        return Err(policy(format!(
            "{field} build and publish authority must equal source_sha"
        )));
    }
    nonempty_string(value, "candidate_release_id", field)?;
    digest64(
        required(value, "checksum_sha256", field)?,
        "checksum_sha256",
    )?;
    let archives = object(
        required(value, "archive_digests", field)?,
        "archive_digests",
    )?;
    exact_keys(archives, &TARGETS, "archive_digests")?;
    for target in TARGETS {
        digest64(required(archives, target, "archive_digests")?, target)?;
    }
    for name in ["ci_run_ref", "changelog_ref", "inventory_ref"] {
        evidence_ref(required(value, name, field)?, name)?;
    }
    nonempty_string(value, "authorized_release_maintainer_ref", field)?;
    Ok(())
}

fn validate_authorization_identity(
    value: &BTreeMap<String, CanonicalValue>,
) -> Result<(), GateError> {
    let tag = nonempty_string(value, "candidate_tag", "authorization")?;
    if !valid_semver_tag(&tag) {
        return Err(policy("authorization.candidate_tag must be v<semver>"));
    }
    expect_string(value, "candidate_version", &tag[1..], "authorization")?;
    let source = sha40(
        required(value, "source_sha", "authorization")?,
        "source_sha",
    )?;
    let resolved = sha40(
        required(value, "publish_workflow_ref_resolved_sha", "authorization")?,
        "publish_workflow_ref_resolved_sha",
    )?;
    let publish = sha40(
        required(value, "publish_workflow_sha", "authorization")?,
        "publish_workflow_sha",
    )?;
    expect_string(value, "publish_workflow_ref", &tag, "authorization")?;
    if resolved != publish || publish != source {
        return Err(policy(
            "authorization publish authority must equal source_sha",
        ));
    }
    for name in ["changelog_ref", "inventory_ref"] {
        evidence_ref(required(value, name, "authorization")?, name)?;
    }
    nonempty_string(value, "authorized_release_maintainer_ref", "authorization")?;
    Ok(())
}

fn valid_semver_tag(value: &str) -> bool {
    let Some(version) = value.strip_prefix('v') else {
        return false;
    };
    let core = version.split_once('-').map_or(version, |(core, _)| core);
    let mut parts = core.split('.');
    let valid_component = |part: &str| {
        !part.is_empty()
            && part.bytes().all(|byte| byte.is_ascii_digit())
            && (part == "0" || !part.starts_with('0'))
    };
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(major), Some(minor), Some(patch), None)
            if valid_component(major) && valid_component(minor) && valid_component(patch)
    )
}

fn compare_identity(
    authority: &BTreeMap<String, CanonicalValue>,
    consumer: &BTreeMap<String, CanonicalValue>,
    name: &str,
) -> Result<(), GateError> {
    for field in IDENTITY_FIELDS {
        if authority.get(field) != consumer.get(field) {
            return Err(policy(format!(
                "{name}.{field} must match candidate authorization"
            )));
        }
    }
    Ok(())
}

fn compare_authorization_identity(
    authority: &BTreeMap<String, CanonicalValue>,
    consumer: &BTreeMap<String, CanonicalValue>,
    name: &str,
) -> Result<(), GateError> {
    for field in AUTHORIZATION_IDENTITY_FIELDS {
        if authority.get(field) != consumer.get(field) {
            return Err(policy(format!(
                "{name}.{field} must match candidate authorization"
            )));
        }
    }
    Ok(())
}

fn compare_field(
    authority: &BTreeMap<String, CanonicalValue>,
    consumer: &BTreeMap<String, CanonicalValue>,
    field: &str,
    name: &str,
) -> Result<(), GateError> {
    if authority.get(field) != consumer.get(field) {
        return Err(policy(format!("{name}.{field} must match authority")));
    }
    Ok(())
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("decision.invalid", detail)
}

fn object<'a>(
    value: &'a CanonicalValue,
    field: &str,
) -> Result<&'a BTreeMap<String, CanonicalValue>, GateError> {
    value
        .as_object()
        .ok_or_else(|| policy(format!("{field} must be an object")))
}

fn array<'a>(value: &'a CanonicalValue, field: &str) -> Result<&'a [CanonicalValue], GateError> {
    value
        .as_array()
        .ok_or_else(|| policy(format!("{field} must be an array")))
}

fn required<'a>(
    value: &'a BTreeMap<String, CanonicalValue>,
    field: &str,
    parent: &str,
) -> Result<&'a CanonicalValue, GateError> {
    value
        .get(field)
        .ok_or_else(|| policy(format!("{parent} is missing required field {field}")))
}

fn string<'a>(value: &'a CanonicalValue, field: &str) -> Result<&'a str, GateError> {
    value
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| policy(format!("{field} must be a non-empty string")))
}

fn nonempty_string(
    value: &BTreeMap<String, CanonicalValue>,
    field: &str,
    parent: &str,
) -> Result<String, GateError> {
    Ok(string(required(value, field, parent)?, field)?.to_owned())
}

fn expect_string(
    value: &BTreeMap<String, CanonicalValue>,
    field: &str,
    expected: &str,
    parent: &str,
) -> Result<(), GateError> {
    if string(required(value, field, parent)?, field)? != expected {
        return Err(policy(format!("{parent}.{field} must be {expected}")));
    }
    Ok(())
}

fn expect_bool(
    value: &BTreeMap<String, CanonicalValue>,
    field: &str,
    expected: bool,
    parent: &str,
) -> Result<(), GateError> {
    if required(value, field, parent)?.as_bool() != Some(expected) {
        return Err(policy(format!("{parent}.{field} must be {expected}")));
    }
    Ok(())
}

fn exact_keys(
    value: &BTreeMap<String, CanonicalValue>,
    expected: &[&str],
    field: &str,
) -> Result<(), GateError> {
    let expected: BTreeSet<&str> = expected.iter().copied().collect();
    let actual: BTreeSet<&str> = value.keys().map(String::as_str).collect();
    if let Some(missing) = expected.difference(&actual).next() {
        return Err(policy(format!(
            "{field} is missing required field {missing}"
        )));
    }
    if let Some(unknown) = actual.difference(&expected).next() {
        return Err(policy(format!("{field} contains unknown field {unknown}")));
    }
    Ok(())
}

fn string_array(value: &CanonicalValue, field: &str) -> Result<Vec<String>, GateError> {
    let values = array(value, field)?;
    let mut result = Vec::with_capacity(values.len());
    for (index, item) in values.iter().enumerate() {
        result.push(string(item, &format!("{field}[{index}]"))?.to_owned());
    }
    Ok(result)
}

fn expect_string_array(
    value: &BTreeMap<String, CanonicalValue>,
    field: &str,
    expected: &[&str],
) -> Result<(), GateError> {
    let actual = string_array(required(value, field, field)?, field)?;
    if actual.iter().map(String::as_str).collect::<Vec<_>>() != expected {
        return Err(policy(format!(
            "{field} does not match the locked contract"
        )));
    }
    Ok(())
}

fn unique(values: &[String], field: &str) -> Result<(), GateError> {
    if as_set(values).len() != values.len() {
        return Err(policy(format!("{field} must contain unique values")));
    }
    Ok(())
}

fn as_set(values: &[String]) -> BTreeSet<&str> {
    values.iter().map(String::as_str).collect()
}

fn timestamp(
    value: &CanonicalValue,
    field: &str,
) -> Result<DateTime<chrono::FixedOffset>, GateError> {
    DateTime::parse_from_rfc3339(string(value, field)?).map_err(|_| {
        policy(format!(
            "{field} must be a timezone-qualified RFC3339 timestamp"
        ))
    })
}

fn sha40(value: &CanonicalValue, field: &str) -> Result<String, GateError> {
    hexadecimal(value, field, 40)
}

fn digest64(value: &CanonicalValue, field: &str) -> Result<String, GateError> {
    hexadecimal(value, field, 64)
}

fn hexadecimal(value: &CanonicalValue, field: &str, length: usize) -> Result<String, GateError> {
    let text = string(value, field)?;
    if text.len() != length || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(policy(format!(
            "{field} must be exactly {length} hexadecimal characters"
        )));
    }
    Ok(text.to_owned())
}

fn evidence_refs(value: &CanonicalValue, field: &str) -> Result<(), GateError> {
    let refs = array(value, field)?;
    if refs.is_empty() {
        return Err(policy(format!("{field} must not be empty")));
    }
    let mut seen = BTreeSet::new();
    for (index, item) in refs.iter().enumerate() {
        let reference = evidence_ref(item, &format!("{field}[{index}]"))?;
        if !seen.insert(reference) {
            return Err(policy(format!("{field} contains a duplicate reference")));
        }
    }
    Ok(())
}

fn evidence_ref(value: &CanonicalValue, field: &str) -> Result<String, GateError> {
    let text = string(value, field)?;
    if text.chars().any(char::is_whitespace) {
        return Err(policy(format!("{field} contains whitespace")));
    }
    if let Some(authority_and_path) = text.strip_prefix("https://") {
        let authority = authority_and_path.split('/').next().unwrap_or_default();
        if authority.is_empty() || authority.contains('@') {
            return Err(policy(format!(
                "{field} contains an invalid HTTPS authority"
            )));
        }
        return Ok(text.to_owned());
    }
    if text.contains("://")
        || text.starts_with(['/', '\\', '~'])
        || text.contains('\\')
        || text
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
        || text
            .split('/')
            .next()
            .is_some_and(|component| component.contains(':'))
    {
        return Err(policy(format!(
            "{field} must be repository-relative or HTTPS"
        )));
    }
    Ok(text.to_owned())
}
