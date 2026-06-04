//! Fixture builder for the branch-dispatch integration tests.
//!
//! Every route/merge pipeline in `branching.rs` shares one byte-identical
//! scaffold: a single CSV `src` over `{ id, amount }`, a `classify_emit`
//! transform that lifts `amount` to an int, and a terminal `combine`
//! transform feeding the `dest` output. Only the middle — the `classify`
//! route, the per-branch transforms, and the `combine__merge` inputs —
//! distinguishes one test from another.
//!
//! [`branch_pipeline`] emits that fixed head and `dest` tail and splices
//! the caller's `body` (the route through the final `combine` transform)
//! between them, so each test keeps only its distinctive routing logic
//! inline. The assembled string is byte-for-byte identical to the
//! hand-written YAML it replaces.

/// Build a branch-dispatch pipeline named `name` whose routing middle is
/// `body`.
///
/// `body` must be the YAML from the `classify` route node through the
/// terminal `combine` transform — exactly the span that varies between
/// branch tests. The shared `src`/`classify_emit` head and the `dest`
/// output tail are supplied here.
pub fn branch_pipeline(name: &str, body: &str) -> String {
    format!(
        "\npipeline:\n  name: {name}\nnodes:\n\
- type: source\n  name: src\n  config:\n    name: src\n    type: csv\n    path: input.csv\n    schema:\n      - {{ name: id, type: string }}\n      - {{ name: amount, type: string }}\n\n\
- type: transform\n  name: classify_emit\n  input: src\n  config:\n    cxl: 'emit amount_val = amount.to_int()\n\n      '\n\
{body}\
- type: output\n  name: dest\n  input: combine\n  config:\n    name: dest\n    type: csv\n    path: output.csv\n    include_unmapped: true\n"
    )
}
