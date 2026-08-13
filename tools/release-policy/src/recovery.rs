//! Strict validation for the Phase 3 recovery sign-off receipt.

use std::path::{Component, Path};

use serde::Deserialize;

use crate::canonical;
use crate::error::GateError;
use crate::limits::{MAX_INPUT_BYTES, read_bounded};

const RECEIPT_MARKER: &str = "<!-- clinker-phase3-recovery-receipt:v1 -->";
const RECEIPT_OPEN: &str = "\n```json\n";
const RECEIPT_CLOSE: &str = "\n```";
const MAX_RECEIPT_BYTES: usize = 256 * 1024;
const MAX_VALUE_BYTES: usize = 512;

const COMMANDS: [(&str, &[&str]); 21] = [
    (
        "CMD-01",
        &[
            "cargo",
            "test",
            "--locked",
            "--offline",
            "-p",
            "clinker-plan",
            "semantic_fingerprint",
        ],
    ),
    (
        "CMD-02",
        &[
            "cargo",
            "test",
            "--locked",
            "--offline",
            "-p",
            "clinker-core-types",
            "--test",
            "failure_classification",
        ],
    ),
    (
        "CMD-03",
        &[
            "cargo",
            "test",
            "--locked",
            "--offline",
            "-p",
            "clinker",
            "--test",
            "machine_protocol_cli",
        ],
    ),
    (
        "CMD-04",
        &[
            "cargo",
            "test",
            "--locked",
            "--offline",
            "-p",
            "clinker",
            "--test",
            "machine_supervision",
        ],
    ),
    (
        "CMD-05",
        &[
            "cargo",
            "test",
            "--locked",
            "--offline",
            "-p",
            "clinker",
            "--test",
            "attempt_publication",
        ],
    ),
    (
        "CMD-06",
        &[
            "cargo",
            "run",
            "--locked",
            "--offline",
            "-p",
            "clinker",
            "--",
            "run",
            "--help",
        ],
    ),
    (
        "CMD-07",
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "clinker-exec",
            "--features",
            "test-utils",
            "--test",
            "invariant_errors",
            "--",
            "--nocapture",
        ],
    ),
    (
        "CMD-08",
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "clinker-plan",
            "--test",
            "observability_config",
        ],
    ),
    (
        "CMD-09",
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "clinker-plan",
            "--test",
            "transform_observability",
        ],
    ),
    (
        "CMD-10",
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "clinker-net",
            "--test",
            "otlp_http",
        ],
    ),
    (
        "CMD-11",
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "clinker-lineage",
            "--test",
            "logical_identity",
        ],
    ),
    (
        "CMD-12",
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "clinker-lineage",
            "--test",
            "lifecycle_delivery",
        ],
    ),
    (
        "CMD-13",
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "clinker-exec",
            "--test",
            "observability_isolation",
        ],
    ),
    (
        "CMD-14",
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "clinker",
            "--test",
            "lineage_cli",
        ],
    ),
    (
        "CMD-15",
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "clinker",
            "--test",
            "observability_isolation",
        ],
    ),
    (
        "CMD-16",
        &[
            "cargo",
            "run",
            "--manifest-path",
            "tools/dependency-policy/Cargo.toml",
            "--locked",
            "--offline",
            "--",
            "--scope",
            "final",
            "--root",
            ".",
        ],
    ),
    ("CMD-17", &["bash", "scripts/check-ai-docs.sh"]),
    ("CMD-18", &["mdbook", "build", "docs/user"]),
    ("CMD-19", &["mdbook", "build", "docs/engine"]),
    ("CMD-20", &["cargo", "fmt", "--all", "--check"]),
    ("CMD-21", &["git", "diff", "--check"]),
];

const CHECKS: [(&str, &[&str]); 10] = [
    ("CHECK-01", &["CMD-07"]),
    ("CHECK-02", &["VALIDATION-D22"]),
    ("CHECK-03", &["CMD-04", "VALIDATION-D26"]),
    ("CHECK-04", &["CMD-05", "VALIDATION-D33"]),
    (
        "CHECK-05",
        &["CMD-08", "CMD-09", "CMD-10", "CMD-13", "CMD-15"],
    ),
    ("CHECK-06", &["CMD-12", "CMD-13", "CMD-15"]),
    ("CHECK-07", &["CMD-11", "CMD-14"]),
    ("CHECK-08", &["CMD-05", "CMD-07"]),
    ("CHECK-09", &["CMD-02", "CMD-07", "CMD-16"]),
    ("CHECK-10", &["VALIDATION-OWNERSHIP", "VALIDATION-WAVES"]),
];

#[derive(Clone, Copy)]
struct ExpectedProhibition {
    id: &'static str,
    source_ids: &'static [&'static str],
    tier: Tier,
    command_ids: &'static [&'static str],
    reviewed_paths: &'static [&'static str],
    disposition: &'static str,
}

const PROHIBITIONS: [ExpectedProhibition; 8] = [
    ExpectedProhibition {
        id: "PROHIB-01",
        source_ids: &["MACHINE-PROHIB-01"],
        tier: Tier::Judgment,
        command_ids: &["CMD-16"],
        reviewed_paths: &[
            "Cargo.toml",
            "crates/clinker/Cargo.toml",
            "crates/clinker/src/main.rs",
            "docs/ai/10_ARCHITECTURE.md",
        ],
        disposition: "no-product-orchestrator-runtime",
    },
    ExpectedProhibition {
        id: "PROHIB-02",
        source_ids: &["MACHINE-PROHIB-02"],
        tier: Tier::Test,
        command_ids: &["CMD-03"],
        reviewed_paths: &[],
        disposition: "",
    },
    ExpectedProhibition {
        id: "PROHIB-03",
        source_ids: &["MACHINE-PROHIB-03", "SUPERVISION-PROHIB-02"],
        tier: Tier::Test,
        command_ids: &["CMD-02", "CMD-03", "CMD-04"],
        reviewed_paths: &[],
        disposition: "",
    },
    ExpectedProhibition {
        id: "PROHIB-04",
        source_ids: &["MACHINE-PROHIB-04"],
        tier: Tier::Test,
        command_ids: &["CMD-03"],
        reviewed_paths: &[],
        disposition: "",
    },
    ExpectedProhibition {
        id: "PROHIB-05",
        source_ids: &["MACHINE-PROHIB-05"],
        tier: Tier::Test,
        command_ids: &["CMD-03", "CMD-05"],
        reviewed_paths: &[],
        disposition: "",
    },
    ExpectedProhibition {
        id: "PROHIB-06",
        source_ids: &["SUPERVISION-PROHIB-01"],
        tier: Tier::Judgment,
        command_ids: &[],
        reviewed_paths: &[
            "crates/clinker/src/main.rs",
            "crates/clinker/tests/support/process.rs",
            "crates/clinker/tests/machine_supervision.rs",
        ],
        disposition: "process-launch-test-only",
    },
    ExpectedProhibition {
        id: "PROHIB-07",
        source_ids: &["SUPERVISION-PROHIB-03"],
        tier: Tier::Test,
        command_ids: &["CMD-03", "CMD-06"],
        reviewed_paths: &[],
        disposition: "",
    },
    ExpectedProhibition {
        id: "PROHIB-08",
        source_ids: &["SUPERVISION-PROHIB-04"],
        tier: Tier::Test,
        command_ids: &["CMD-04", "CMD-05"],
        reviewed_paths: &[],
        disposition: "",
    },
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema: String,
    plan: String,
    status: String,
    commands: Vec<CommandRow>,
    checks: Vec<CheckRow>,
    prohibitions: Vec<ProhibitionRow>,
    dependency_closure: DependencyClosure,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandRow {
    id: String,
    argv: Vec<String>,
    status: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckRow {
    id: String,
    evidence_ids: Vec<String>,
    status: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProhibitionRow {
    id: String,
    source_ids: Vec<String>,
    tier: Tier,
    command_ids: Vec<String>,
    reviewed_paths: Vec<String>,
    disposition: String,
    status: String,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Tier {
    Test,
    Judgment,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyClosure {
    plans: Vec<String>,
    status: String,
}

/// Validate the unique Phase 3 recovery receipt embedded in a summary.
pub fn validate_receipt(summary: &Path, repository_root: &Path) -> Result<(), GateError> {
    let summary = read_bounded(summary, "read Phase 3 recovery summary", MAX_INPUT_BYTES)?;
    let receipt_bytes = extract_receipt(&summary)?;
    canonical::parse_json_with_limit(receipt_bytes, MAX_RECEIPT_BYTES)?;
    let receipt: Receipt = serde_json::from_slice(receipt_bytes)
        .map_err(|_| policy("receipt JSON does not match the v1 schema"))?;
    validate(&receipt, repository_root)
}

fn extract_receipt(summary: &[u8]) -> Result<&[u8], GateError> {
    let summary = std::str::from_utf8(summary)
        .map_err(|_| policy("summary must be UTF-8 with one exact receipt block"))?;
    let markers = summary.match_indices(RECEIPT_MARKER).collect::<Vec<_>>();
    if markers.len() != 1 {
        return Err(policy("summary must contain exactly one receipt marker"));
    }
    let marker = markers[0].0;
    if marker != 0 && summary.as_bytes().get(marker.wrapping_sub(1)) != Some(&b'\n') {
        return Err(policy("receipt marker must occupy one exact line"));
    }
    let after_marker = marker + RECEIPT_MARKER.len();
    let tail = summary
        .get(after_marker..)
        .ok_or_else(|| policy("receipt marker is truncated"))?;
    if !tail.starts_with(RECEIPT_OPEN) {
        return Err(policy(
            "receipt marker must be followed immediately by a json fence",
        ));
    }
    let receipt_start = after_marker + RECEIPT_OPEN.len();
    let receipt_tail = summary
        .get(receipt_start..)
        .ok_or_else(|| policy("receipt block is truncated"))?;
    let close = receipt_tail
        .find(RECEIPT_CLOSE)
        .ok_or_else(|| policy("receipt block has no exact closing fence"))?;
    let receipt = receipt_tail
        .get(..close)
        .ok_or_else(|| policy("receipt block is malformed"))?;
    if receipt.len() > MAX_RECEIPT_BYTES {
        return Err(policy("receipt exceeds the 262144-byte limit"));
    }
    if !receipt.starts_with('{') || !receipt.ends_with('}') {
        return Err(policy("receipt fence must contain one JSON object"));
    }
    let after_close = receipt_tail
        .get(close + RECEIPT_CLOSE.len()..)
        .ok_or_else(|| policy("receipt closing fence is malformed"))?;
    if !matches!(after_close, "" | "\n") {
        return Err(policy("receipt block must be the final summary block"));
    }
    Ok(receipt.as_bytes())
}

fn validate(receipt: &Receipt, repository_root: &Path) -> Result<(), GateError> {
    exact(
        &receipt.schema,
        "clinker.phase3-recovery-receipt/v1",
        "schema",
    )?;
    exact(&receipt.plan, "03-51", "plan")?;
    pass(&receipt.status, "receipt")?;
    validate_commands(&receipt.commands)?;
    validate_checks(&receipt.checks)?;
    validate_prohibitions(&receipt.prohibitions, repository_root)?;
    validate_closure(&receipt.dependency_closure)
}

fn validate_commands(rows: &[CommandRow]) -> Result<(), GateError> {
    // `exact_row_ids` has already compared the whole id sequence element by
    // element, so row `n` is `COMMANDS[n]` and there is nothing left for a
    // registry lookup to find. Keeping one meant two unreachable "is not
    // registered" arms a reader had to reason about.
    exact_row_ids(
        rows.iter().map(|row| row.id.as_str()),
        COMMANDS.iter().map(|(id, _)| *id),
        "command",
    )?;
    for (index, row) in rows.iter().enumerate() {
        bounded_nonempty(&row.id, "command id")?;
        pass(&row.status, "command")?;
        if row.argv.is_empty() {
            return Err(policy("command argv must not be empty"));
        }
        for token in &row.argv {
            bounded_nonempty(token, "command argv token")?;
            if token.chars().any(char::is_whitespace) || token.chars().any(char::is_control) {
                return Err(policy("command argv tokens must be shell-free"));
            }
        }
        let (expected_id, expected_argv) = COMMANDS[index];
        if !same_strings(&row.argv, expected_argv) {
            return Err(policy(format!("{expected_id} argv or order changed")));
        }
    }
    Ok(())
}

fn validate_checks(rows: &[CheckRow]) -> Result<(), GateError> {
    exact_row_ids(
        rows.iter().map(|row| row.id.as_str()),
        CHECKS.iter().map(|(id, _)| *id),
        "check",
    )?;
    for (index, row) in rows.iter().enumerate() {
        bounded_nonempty(&row.id, "check id")?;
        pass(&row.status, "check")?;
        if row.evidence_ids.is_empty() {
            return Err(policy("check evidence_ids must not be empty"));
        }
        validate_string_list(&row.evidence_ids, "check evidence id")?;
        let (expected_id, expected) = CHECKS[index];
        if !same_strings(&row.evidence_ids, expected) {
            return Err(policy(format!("{expected_id} evidence or order changed")));
        }
    }
    Ok(())
}

fn validate_prohibitions(rows: &[ProhibitionRow], repository_root: &Path) -> Result<(), GateError> {
    exact_row_ids(
        rows.iter().map(|row| row.id.as_str()),
        PROHIBITIONS.iter().map(|row| row.id),
        "prohibition",
    )?;
    for (row, expected) in rows.iter().zip(PROHIBITIONS.iter()) {
        bounded_nonempty(&row.id, "prohibition id")?;
        pass(&row.status, "prohibition")?;
        validate_string_list(&row.source_ids, "prohibition source id")?;
        validate_string_list(&row.command_ids, "prohibition command id")?;
        validate_string_list(&row.reviewed_paths, "prohibition reviewed path")?;
        if row.id != expected.id
            || !same_strings(&row.source_ids, expected.source_ids)
            || row.tier != expected.tier
            || !same_strings(&row.command_ids, expected.command_ids)
            || !same_strings(&row.reviewed_paths, expected.reviewed_paths)
            || row.disposition != expected.disposition
        {
            return Err(policy(format!("{} evidence tuple changed", expected.id)));
        }
        match row.tier {
            Tier::Test => {
                if row.command_ids.is_empty()
                    || !row.reviewed_paths.is_empty()
                    || !row.disposition.is_empty()
                {
                    return Err(policy("test prohibition evidence is malformed"));
                }
            }
            Tier::Judgment => {
                bounded_nonempty(&row.disposition, "judgment disposition")?;
                if row.reviewed_paths.is_empty() {
                    return Err(policy("judgment reviewed_paths must not be empty"));
                }
                for path in &row.reviewed_paths {
                    validate_reviewed_path(path, repository_root)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_reviewed_path(path: &str, repository_root: &Path) -> Result<(), GateError> {
    let path = Path::new(path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        || !repository_root.join(path).is_file()
    {
        return Err(policy(
            "judgment reviewed path must be an existing repository-relative file",
        ));
    }
    Ok(())
}

fn validate_closure(closure: &DependencyClosure) -> Result<(), GateError> {
    pass(&closure.status, "dependency closure")?;
    let expected = (36..=50)
        .map(|plan| format!("03-{plan}"))
        .collect::<Vec<_>>();
    validate_string_list(&closure.plans, "dependency plan")?;
    if closure.plans != expected {
        return Err(policy(
            "dependency closure must list 03-36 through 03-50 in order",
        ));
    }
    Ok(())
}

fn exact_row_ids<'a>(
    observed: impl Iterator<Item = &'a str>,
    expected: impl Iterator<Item = &'a str>,
    label: &str,
) -> Result<(), GateError> {
    // Sequence equality, which subsumes the set comparison this also used to
    // make: equal sequences have equal sets, and unequal ones already fail
    // here, so the set clause never decided anything a reader had to weigh.
    let observed = observed.collect::<Vec<_>>();
    let expected = expected.collect::<Vec<_>>();
    if observed != expected {
        return Err(policy(format!(
            "{label} rows must match the exact registry"
        )));
    }
    Ok(())
}

fn validate_string_list(values: &[String], label: &str) -> Result<(), GateError> {
    for value in values {
        bounded_nonempty(value, label)?;
    }
    Ok(())
}

fn bounded_nonempty(value: &str, label: &str) -> Result<(), GateError> {
    if value.is_empty() || value.len() > MAX_VALUE_BYTES {
        return Err(policy(format!(
            "{label} must contain 1 through {MAX_VALUE_BYTES} bytes"
        )));
    }
    Ok(())
}

fn exact(observed: &str, expected: &str, label: &str) -> Result<(), GateError> {
    bounded_nonempty(observed, label)?;
    if observed != expected {
        return Err(policy(format!("receipt {label} is not the required value")));
    }
    Ok(())
}

fn pass(status: &str, label: &str) -> Result<(), GateError> {
    if status != "PASS" {
        return Err(policy(format!("{label} status must be PASS")));
    }
    Ok(())
}

fn same_strings(observed: &[String], expected: &[&str]) -> bool {
    observed.len() == expected.len()
        && observed
            .iter()
            .zip(expected.iter())
            .all(|(observed, expected)| observed == expected)
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("recovery.receipt.invalid", detail)
}
