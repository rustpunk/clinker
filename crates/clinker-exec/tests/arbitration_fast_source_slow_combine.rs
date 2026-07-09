//! Fast-Source / slow-Combine arbitration scenario, on current-pressure
//! semantics:
//!
//! Under `BackPressurePreferred(Priority)`, the resume controller pauses a
//! back-pressureable Source when CURRENT pressure exceeds the soft limit and
//! resumes it once current pressure recedes below the (lower) resume
//! watermark — a hysteresis band. Pause election reads current pressure, not
//! the monotonic peak RSS: a peak-keyed resume could never fire because
//! `peak_rss` only rises. Current pressure is driven here through the
//! registered consumers' charged bytes (`sum_consumer_usage`), so the band is
//! exercised deterministically without faking OS RSS; a seeded peak keeps
//! `should_spill` tripped so its arbitration round runs while current
//! pressure moves across the band.

use clinker_exec::executor::source_stream::SourceConsumer;
use clinker_exec::pipeline::memory::{
    BackPressurePreferred, ConsumerHandle, ConsumerSpillError, MemoryArbitrator, MemoryConsumer,
    Priority,
};
use std::sync::Arc;

/// Aggregate-shaped consumer for the test — non-back-pressureable, reports a
/// fixed in-memory footprint via its handle. Records whether the arbitrator
/// asked it to spill so the "prefer pause over spill" posture is observable.
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

const HARD: u64 = 100 * 1024 * 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;

#[test]
fn source_pauses_on_current_pressure_and_resumes_below_the_resume_watermark() {
    // spill 0.50 -> soft 50 GiB; resume 0.40 -> resume_limit 40 GiB.
    let arbitrator = MemoryArbitrator::with_policy(
        HARD,
        0.50,
        0.40,
        Box::new(BackPressurePreferred::wrapping(Priority)),
    );

    let source_handle = ConsumerHandle::new();
    source_handle.set_bytes(64 * 1024);
    arbitrator.register_consumer(Arc::new(SourceConsumer::new(source_handle.clone())));

    // Aggregate charged above the soft limit so `sum_consumer_usage` — and
    // therefore `current_pressure()` — sits over the 50 GiB soft threshold.
    let aggregate_handle = ConsumerHandle::new();
    aggregate_handle.set_bytes(60 * GIB);
    let aggregate = Arc::new(AggregateLike {
        handle: aggregate_handle.clone(),
    });
    arbitrator.register_consumer(aggregate.clone());

    // A seeded peak keeps `should_spill` tripped for the whole test (peak is
    // monotonic), so its arbitration round always runs; the pause/resume
    // decision itself keys on CURRENT pressure, driven by the aggregate's
    // charged bytes above.
    arbitrator.set_peak_rss_for_test(75 * GIB);

    assert!(!source_handle.is_paused(), "source must start unpaused");

    // current pressure (~60 GiB) > soft (50 GiB): pause the back-pressureable
    // Source, and do NOT spill the aggregate (prefer pause over spill).
    assert!(arbitrator.should_spill());
    assert!(
        source_handle.is_paused(),
        "current pressure over the soft limit must pause the back-pressureable Source"
    );
    assert!(
        !aggregate_handle.take_spill_request(),
        "the aggregate must not be spilled while a back-pressureable Source can be paused"
    );

    // Drop current pressure below the resume watermark (~30 GiB < 40 GiB).
    // `should_spill` still trips (peak is monotonic), so its round runs and
    // the resume controller unparks the Source.
    aggregate_handle.set_bytes(30 * GIB);
    assert!(arbitrator.should_spill());
    assert!(
        !source_handle.is_paused(),
        "current pressure below the resume watermark must resume the paused Source"
    );
}

#[test]
fn an_actively_drained_source_is_exempt_from_pause() {
    // A Source the walk is currently draining (active) is never paused, even
    // above the soft limit — pausing the producer feeding the recv() the walk
    // is blocked on would deadlock the single-threaded walk.
    let arbitrator = MemoryArbitrator::with_policy(
        HARD,
        0.50,
        0.40,
        Box::new(BackPressurePreferred::wrapping(Priority)),
    );

    let source_handle = ConsumerHandle::new();
    arbitrator.register_consumer(Arc::new(SourceConsumer::new(source_handle.clone())));

    let aggregate_handle = ConsumerHandle::new();
    aggregate_handle.set_bytes(60 * GIB); // current pressure over soft
    arbitrator.register_consumer(Arc::new(AggregateLike {
        handle: aggregate_handle.clone(),
    }));

    arbitrator.set_peak_rss_for_test(75 * GIB);

    source_handle.set_active();
    assert!(arbitrator.should_spill());
    assert!(
        !source_handle.is_paused(),
        "an active (currently-drained) Source is exempt from the pause step"
    );

    // Once the drain arm clears the exemption, the same pressure pauses it.
    source_handle.clear_active();
    assert!(arbitrator.should_spill());
    assert!(
        source_handle.is_paused(),
        "a non-active Source above the soft limit is paused"
    );
}
