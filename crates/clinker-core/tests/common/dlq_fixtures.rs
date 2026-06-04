//! Fixture builder shared by the correlation-key DLQ integration tests
//! (`correlated_dlq.rs` and its retract-path sibling
//! `correlated_dlq_retract.rs`).
//!
//! Both suites lean on one recurring shape: a CSV `src` carrying a
//! `correlation_key`, a `validate` transform that echoes the employee id
//! and coerces `value` to an int (so a non-numeric cell triggers a
//! per-group DLQ), and a single `out` sink — all under the `continue`
//! error strategy. Only the pipeline name, the correlation key, and the
//! source schema vary between callers.
//!
//! [`dlq_validate_pipeline`] emits that shape. The schema rows are passed
//! verbatim (including their trailing newline) so a caller can append the
//! blank line some fixtures place before the first node; the assembled
//! string is byte-for-byte identical to the hand-written YAML it replaces.

/// Build a correlation-key DLQ pipeline named `name`, keyed on
/// `correlation_key`, whose source schema rows are `schema`.
///
/// `schema` is the indented YAML for the source `schema:` list — each row
/// like `"      - { name: employee_id, type: string }\n"` — and carries
/// its own trailing whitespace so callers reproduce the exact spacing
/// before the `validate` transform.
pub fn dlq_validate_pipeline(name: &str, correlation_key: &str, schema: &str) -> String {
    format!(
        "\npipeline:\n  name: {name}\nerror_handling:\n  strategy: continue\nnodes:\n\
- type: source\n  name: src\n  config:\n    name: src\n    path: input.csv\n    correlation_key: {correlation_key}\n    type: csv\n    schema:\n\
{schema}\
- type: transform\n  name: validate\n  input: src\n  config:\n    cxl: 'emit emp_id = employee_id\n\n      emit val = value.to_int()\n\n      '\n\
- type: output\n  name: out\n  input: validate\n  config:\n    name: out\n    path: output.csv\n    type: csv\n    include_unmapped: true\n"
    )
}
