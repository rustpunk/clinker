//! Fast-Source / slow-Combine arbitration scenario:
//!
//! Configure a `BackPressurePreferred(Priority)` policy and register
//! one back-pressureable consumer (Source-shaped) plus one
//! non-back-pressureable consumer (Aggregate-shaped). When the
//! arbitrator's peak RSS crosses the soft limit, the policy must
//! prefer pausing the Source over forcing a spill on the Aggregate —
//! pauses are reversible and free; spills cost disk I/O.
//!
//! Asserts the Source's `PauseSignal` flips after `should_spill()`
//! returns true, demonstrating the full arbitration round-trip:
//! policy elects victim → arbitrator invokes pause → handle's
//! `is_paused` reads true → a producer calling `wait_while_paused`
//! would now block until resume.

use clinker_exec::executor::source_stream::SourceConsumer;
use clinker_exec::pipeline::memory::{
    BackPressurePreferred, ConsumerHandle, ConsumerSpillError, MemoryArbitrator, MemoryConsumer,
    Priority,
};
use std::sync::Arc;

/// Aggregate-shaped consumer for the test — non-back-pressureable,
/// reports a fixed in-memory footprint via its handle.
struct AggregateLike {
    handle: Arc<ConsumerHandle>,
}

impl MemoryConsumer for AggregateLike {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }
    fn spill_priority(&self) -> i32 {
        30
    }
    fn try_spill(&self, _target: u64) -> Result<u64, ConsumerSpillError> {
        self.handle.request_spill();
        Ok(self.handle.bytes())
    }
    fn can_back_pressure(&self) -> bool {
        false
    }
}

#[test]
fn back_pressure_preferred_pauses_source_under_pressure() {
    // 100 GiB hard limit so `observe()`'s `fetch_max(real_rss, ...)`
    // call inside `should_spill` cannot push peak_rss above what the
    // test seeds; real process RSS during cargo test is < 1 GiB.
    let arbitrator = MemoryArbitrator::with_policy(
        100 * 1024 * 1024 * 1024,
        0.50,
        0.40,
        Box::new(BackPressurePreferred::wrapping(Priority)),
    );

    let source_handle = ConsumerHandle::new();
    source_handle.set_bytes(64 * 1024);
    arbitrator.register_consumer(Arc::new(SourceConsumer::new(source_handle.clone())));

    let aggregate_handle = ConsumerHandle::new();
    aggregate_handle.set_bytes(256 * 1024);
    arbitrator.register_consumer(Arc::new(AggregateLike {
        handle: aggregate_handle.clone(),
    }));

    // Drive peak RSS above the soft limit via the test hook so the
    // arbitration round fires deterministically without depending on
    // actual process RSS.
    arbitrator.set_peak_rss_for_test(75 * 1024 * 1024 * 1024);

    assert!(
        !source_handle.is_paused(),
        "source should not be paused before arbitration runs"
    );
    assert!(
        !aggregate_handle.take_spill_request(),
        "aggregate should not have a pending spill request before arbitration runs"
    );

    let tripped = arbitrator.should_spill();
    assert!(tripped, "RSS above soft limit should trip should_spill");
    assert!(
        source_handle.is_paused(),
        "BackPressurePreferred policy must pause the back-pressureable Source ahead of \
         spilling the Aggregate"
    );
    assert!(
        !aggregate_handle.take_spill_request(),
        "Aggregate should not be the spill victim while a back-pressureable consumer exists"
    );
}
