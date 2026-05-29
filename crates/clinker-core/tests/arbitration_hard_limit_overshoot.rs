//! Hard-limit overshoot scenario:
//!
//! When peak RSS exceeds the arbitrator's hard limit, `should_abort()`
//! must return true regardless of how many consumers are registered
//! or what policy is installed. Operator hot loops poll this in their
//! 10K-record cadence and surface `PipelineError::MemoryBudgetExceeded`
//! when the abort gate trips.
//!
//! Pairs with the existing per-category abort tests in
//! `pipeline/memory.rs` — this one focuses on the multi-consumer
//! registry case where the soft-limit spill round elected a victim
//! that could not free enough memory.

use clinker_core::pipeline::memory::{
    ConsumerHandle, ConsumerSpillError, MemoryArbitrator, MemoryConsumer, Priority,
};
use std::sync::Arc;

struct StuckAggregate {
    handle: Arc<ConsumerHandle>,
}

impl MemoryConsumer for StuckAggregate {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }
    fn spill_priority(&self) -> i32 {
        30
    }
    fn try_spill(&mut self, target: u64) -> Result<u64, ConsumerSpillError> {
        // Simulates a consumer that cannot free what the arbitrator
        // asked for — e.g. a per-group accumulator whose entire
        // working set fits in a single record's heap. The arbitrator
        // walks to the next victim, but if all consumers are stuck,
        // RSS keeps climbing until the hard limit trips.
        self.handle.request_spill();
        Err(ConsumerSpillError::BelowTarget { target, freed: 0 })
    }
    fn can_back_pressure(&self) -> bool {
        false
    }
}

// Tests use limits in the hundreds of GiB so the real process RSS
// (which `observe()` reads via `rss_bytes()` and folds into
// `peak_rss` via `fetch_max`) cannot push the counter above our
// test-set value. `fetch_max` keeps whichever is larger, so a
// 100 GiB seed dominates any realistic test-process RSS.
const HARD_LIMIT: u64 = 100 * 1024 * 1024 * 1024; // 100 GiB
const SOFT_FRAC: f64 = 0.50; // soft = 50 GiB

#[test]
fn should_abort_trips_when_rss_exceeds_hard_limit() {
    let arbitrator = MemoryArbitrator::with_policy(HARD_LIMIT, SOFT_FRAC, Box::new(Priority));

    let handle = ConsumerHandle::new();
    handle.set_bytes(64 * 1024);
    arbitrator.register_consumer(Box::new(StuckAggregate {
        handle: handle.clone(),
    }));

    // Push peak RSS just over the hard limit.
    arbitrator.set_peak_rss_for_test(HARD_LIMIT + 1024);

    assert!(
        arbitrator.should_abort(),
        "should_abort must trip when peak RSS exceeds hard limit"
    );
}

#[test]
fn soft_limit_arbitration_runs_before_hard_limit_aborts() {
    // The arbitrator's two-tier model: soft-limit spill attempts run
    // first; only an honest overshoot of the hard limit aborts.
    let arbitrator = MemoryArbitrator::with_policy(HARD_LIMIT, SOFT_FRAC, Box::new(Priority));

    let handle = ConsumerHandle::new();
    handle.set_bytes(64 * 1024);
    arbitrator.register_consumer(Box::new(StuckAggregate {
        handle: handle.clone(),
    }));

    // Peak RSS above soft limit (50 GiB) but below hard limit (100 GiB).
    arbitrator.set_peak_rss_for_test(75 * 1024 * 1024 * 1024);

    assert!(arbitrator.should_spill(), "soft-limit trip must fire");
    assert!(
        !arbitrator.should_abort(),
        "should_abort must NOT fire while RSS stays below the hard limit"
    );
    assert!(
        handle.take_spill_request(),
        "the policy must have driven the stuck aggregate's try_spill at the soft-limit trip"
    );

    // Now the operator can't free anything (StuckAggregate returns
    // BelowTarget). Bump RSS past the hard limit and confirm the
    // abort gate fires.
    arbitrator.set_peak_rss_for_test(HARD_LIMIT + 1024);
    assert!(
        arbitrator.should_abort(),
        "after the soft-limit round failed to free memory, an honest hard-limit overshoot \
         must surface as should_abort = true"
    );
}
