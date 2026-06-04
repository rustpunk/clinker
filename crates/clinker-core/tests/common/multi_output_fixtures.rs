//! Fixture builder for the multi-output integration tests.
//!
//! Most pipelines in `multi_output.rs` open with one byte-identical
//! preamble: a single CSV `src` over `{ id, amount }` feeding a
//! `classify_emit` transform that lifts `amount` to an int. What differs
//! per test is everything downstream — the `classify` route and the fan
//! of `output` sinks (two-way, three-way, inclusive, error-injecting,
//! and so on).
//!
//! [`multi_output_pipeline`] emits that fixed head and splices the
//! caller's `body` (the route through the output sinks) after it. The
//! assembled string is byte-for-byte identical to the hand-written YAML
//! it replaces. Fixtures that diverge in the head itself (a custom
//! schema, an `error_handling` block, a non-`classify_emit` transform)
//! stay written out in full.

/// Build a multi-output pipeline named `name` whose route-and-sink tail
/// is `body`.
///
/// `body` must be the YAML from the first node after `classify_emit`
/// (typically the `classify` route) through the final `output` node. The
/// shared `src`/`classify_emit` head is supplied here.
pub fn multi_output_pipeline(name: &str, body: &str) -> String {
    format!(
        "\npipeline:\n  name: {name}\nnodes:\n\
- type: source\n  name: src\n  config:\n    name: src\n    path: input.csv\n    type: csv\n    schema:\n      - {{ name: id, type: string }}\n      - {{ name: amount, type: string }}\n\n\
- type: transform\n  name: classify_emit\n  input: src\n  config:\n    cxl: 'emit amount_val = amount.to_int()\n\n      '\n\
{body}"
    )
}
