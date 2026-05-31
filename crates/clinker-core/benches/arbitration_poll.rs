//! Microbenchmarks for `MemoryArbitrator::should_spill` and
//! `MemoryArbitrator::poll_arbitration` at varying consumer-registry
//! sizes (0, 1, 6, 24).
//!
//! 0 = empty registry (skeleton baseline before any operator
//! registers). 1 = single Source or single Aggregate. 6 = the named
//! operator categories in the 117c plan (Source, Aggregate, Sort,
//! GraceHash, SortMerge, node_buffer slot). 24 = a deep pipeline
//! with multiple parallel arms.
//!
//! Watching for: a Mutex-locked linear scan over registered consumers
//! that materially regresses `should_spill()` cost beyond the
//! 5µs/call target. If this bench shows a sub-5µs result at all four
//! registry sizes, the current `Mutex<HashMap<ConsumerId, ...>>`
//! shape is good; if 24-consumer cost exceeds the target, swap the
//! registry for an `ArcSwap<Vec<...>>` snapshot read by hot paths.

use clinker_core::pipeline::memory::{
    ConsumerHandle, ConsumerSpillError, MemoryArbitrator, MemoryConsumer, NoOpPolicy,
};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;

struct FakeConsumer {
    handle: Arc<ConsumerHandle>,
    priority: i32,
}

impl FakeConsumer {
    fn new(priority: i32) -> Self {
        let handle = ConsumerHandle::new();
        handle.set_bytes(1024 * 1024);
        Self { handle, priority }
    }
}

impl MemoryConsumer for FakeConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        self.priority
    }

    fn try_spill(&mut self, _target_bytes: u64) -> Result<u64, ConsumerSpillError> {
        Ok(self.handle.bytes())
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

fn build_arbitrator(n: usize) -> MemoryArbitrator {
    let arbitrator = MemoryArbitrator::with_policy(u64::MAX, 0.80, Box::new(NoOpPolicy));
    for i in 0..n {
        let priority = (i % 4) as i32 * 10;
        arbitrator.register_consumer(Box::new(FakeConsumer::new(priority)));
    }
    arbitrator
}

fn bench_should_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("arbitration_should_spill");
    for n in [0usize, 1, 6, 24] {
        let arbitrator = build_arbitrator(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                black_box(arbitrator.should_spill());
            });
        });
    }
    group.finish();
}

fn bench_sum_consumer_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("arbitration_sum_consumer_usage");
    for n in [0usize, 1, 6, 24] {
        let arbitrator = build_arbitrator(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                black_box(arbitrator.sum_consumer_usage());
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_should_spill, bench_sum_consumer_usage);
criterion_main!(benches);
