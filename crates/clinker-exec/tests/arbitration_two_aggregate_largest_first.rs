//! Two-Aggregate `LargestFirst` arbitration scenario:
//!
//! Configure the bare `LargestFirst` policy (matching `memory.backpressure: both`
//! when no back-pressureable consumer is available) and register two
//! Aggregate-shaped consumers — both priority 30, distinct current_usage.
//! `LargestFirst` must elect the heavier one as the spill victim on
//! every arbitration round.

use clinker_exec::pipeline::memory::{
    ConsumerHandle, ConsumerSpillError, LargestFirst, MemoryArbitrator, MemoryConsumer,
};
use std::sync::Arc;

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
        // The wrapper's `request_spill` flips the handle's flag — the
        // operator hot loop consumes it via `take_spill_request`.
        // Returning the handle's current bytes simulates an operator
        // that drains its entire in-memory state on demand.
        self.handle.request_spill();
        Ok(self.handle.bytes())
    }
    fn can_back_pressure(&self) -> bool {
        false
    }
}

#[test]
fn largest_first_picks_the_larger_aggregate() {
    // 100 GiB hard limit so real-process RSS cannot push peak_rss
    // above the test-seeded value via `observe()`'s `fetch_max`.
    let arbitrator =
        MemoryArbitrator::with_policy(100 * 1024 * 1024 * 1024, 0.50, 0.40, Box::new(LargestFirst));

    // Smaller aggregate: 64 KiB
    let small_handle = ConsumerHandle::new();
    small_handle.set_bytes(64 * 1024);
    arbitrator.register_consumer(Arc::new(AggregateLike {
        handle: small_handle.clone(),
    }));

    // Larger aggregate: 256 KiB
    let large_handle = ConsumerHandle::new();
    large_handle.set_bytes(256 * 1024);
    arbitrator.register_consumer(Arc::new(AggregateLike {
        handle: large_handle.clone(),
    }));

    arbitrator.set_peak_rss_for_test(75 * 1024 * 1024 * 1024);

    assert!(!small_handle.take_spill_request());
    assert!(!large_handle.take_spill_request());

    assert!(arbitrator.should_spill());

    assert!(
        large_handle.take_spill_request(),
        "LargestFirst must request spill on the heavier aggregate"
    );
    assert!(
        !small_handle.take_spill_request(),
        "smaller aggregate must not be the victim while a larger one exists"
    );
}

#[test]
fn largest_first_picks_a_new_victim_each_round() {
    // After the larger aggregate spills (its bytes drop), the next
    // arbitration round elects the remaining heaviest. Demonstrates
    // that `LargestFirst` reads `current_usage` at every poll.
    // 100 GiB hard limit so real-process RSS cannot push peak_rss
    // above the test-seeded value via `observe()`'s `fetch_max`.
    let arbitrator =
        MemoryArbitrator::with_policy(100 * 1024 * 1024 * 1024, 0.50, 0.40, Box::new(LargestFirst));

    let small_handle = ConsumerHandle::new();
    small_handle.set_bytes(64 * 1024);
    arbitrator.register_consumer(Arc::new(AggregateLike {
        handle: small_handle.clone(),
    }));

    let large_handle = ConsumerHandle::new();
    large_handle.set_bytes(256 * 1024);
    arbitrator.register_consumer(Arc::new(AggregateLike {
        handle: large_handle.clone(),
    }));

    arbitrator.set_peak_rss_for_test(75 * 1024 * 1024 * 1024);
    assert!(arbitrator.should_spill());
    assert!(large_handle.take_spill_request());

    // Operator "drains" — handle reports 0 bytes now (post-spill).
    large_handle.set_bytes(0);

    // Next round: smaller aggregate is now the heaviest registered
    // in-memory consumer.
    assert!(arbitrator.should_spill());
    assert!(
        small_handle.take_spill_request(),
        "after the larger aggregate drained, LargestFirst must elect the next heaviest"
    );
}
