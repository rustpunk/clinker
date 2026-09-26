//! The two-direction memory-pressure check every retained-state consumer's
//! tests share.
//!
//! Item 6 of the Memory budget checklist in `docs/ai/32_NODE_OBLIGATIONS.md`
//! asks for two runs of the same input: one under a limit smaller than the
//! state, which must spill that state and still produce the same output, and
//! one with ample memory, which must write nothing to disk while the state's
//! bytes are charged. [`assert_arbitrated`] checks both runs at once.
//!
//! Its first check is the reason it exists. A pressure test whose state fits
//! under its own low limit proves nothing about spilling, and the cheapest
//! way to make a failing pressure test pass is to shrink its input until the
//! state fits. The helper refuses that pair outright: the ample run's charged
//! peak must exceed the low run's limit. Shrinking a test's input, raising its
//! limit or relaxing an assertion to get past this check is a stop, not a
//! deviation.
//!
//! Include it with `#[path = "common/memory_pressure.rs"] mod memory_pressure;`.

use clinker_exec::executor::ExecutionReport;

/// The figures [`assert_arbitrated`] compares from one run of a pressure
/// test.
#[derive(Debug, Clone)]
pub struct PressureRun {
    /// The memory limit the run was given, in bytes.
    pub limit_bytes: u64,
    /// The highest number of bytes the run's consumers held charged at once.
    pub peak_charged_bytes: u64,
    /// Bytes the node under test spilled to disk.
    pub node_spill_bytes: u64,
    /// Bytes the whole run spilled to disk.
    pub total_spill_bytes: u64,
    /// The run's output, with any run-variant columns (timestamps, ids)
    /// already masked by the caller so equal data compares equal.
    pub output: Vec<u8>,
}

impl PressureRun {
    /// Read the figures of a finished run from its report.
    ///
    /// `node` is the name of the node whose state is under test; its spill
    /// bytes are `0` when the report attributes no spill to it. `limit_bytes`
    /// is the limit the run was given and `output` its masked output.
    #[allow(dead_code)] // A target that builds its runs by hand does not read a report.
    pub fn from_report(
        report: &ExecutionReport,
        node: &str,
        limit_bytes: u64,
        output: Vec<u8>,
    ) -> Self {
        Self {
            limit_bytes,
            peak_charged_bytes: report.peak_consumer_usage_bytes,
            node_spill_bytes: report.per_stage_spill_bytes.get(node).copied().unwrap_or(0),
            total_spill_bytes: report.cumulative_spill_bytes,
            output,
        }
    }
}

/// Assert that `node`'s state was arbitrated in both directions: under the
/// `low` limit it spilled and still produced the ample run's output, and under
/// `ample` memory nothing reached disk.
///
/// Panics, naming `node` and the figures, when:
///
/// 1. the ample run's charged peak does not exceed the low run's limit (the
///    state fits under the low limit, so the pair proves nothing);
/// 2. the ample run spilled anything;
/// 3. the low run did not spill `node`;
/// 4. the low run's charged peak is not below the ample run's (it held the
///    whole state rather than spilling part of it);
/// 5. the two outputs differ.
#[allow(dead_code)] // A target that only reads reports may not assert a pair.
pub fn assert_arbitrated(node: &str, low: &PressureRun, ample: &PressureRun) {
    assert!(
        ample.peak_charged_bytes > low.limit_bytes,
        "`{node}`: state fits under the low limit: a pressure test must hold more than its \
         low limit (ample peak charged {} bytes, low limit {} bytes); do not shrink the input \
         or raise the limit to pass",
        ample.peak_charged_bytes,
        low.limit_bytes
    );
    assert!(
        ample.total_spill_bytes == 0,
        "`{node}`: ample memory spilled {} bytes; the ample run must write nothing to disk",
        ample.total_spill_bytes
    );
    assert!(
        low.node_spill_bytes > 0,
        "`{node}`: the low run did not spill `{node}` (limit {} bytes, run spilled {} bytes \
         in total)",
        low.limit_bytes,
        low.total_spill_bytes
    );
    assert!(
        low.peak_charged_bytes < ample.peak_charged_bytes,
        "`{node}`: the low run held the whole state (low peak charged {} bytes, ample peak \
         charged {} bytes)",
        low.peak_charged_bytes,
        ample.peak_charged_bytes
    );
    assert!(
        low.output == ample.output,
        "`{node}`: output differs across limits ({} bytes under the low limit, {} bytes with \
         ample memory)",
        low.output.len(),
        ample.output.len()
    );
}
