//! Microbenchmarks for `MemoryArbitrator::should_spill` and
//! `MemoryArbitrator::sum_consumer_usage` at varying consumer-registry
//! sizes (0, 1, 6, 24, 48, 96).
//!
//! 0 = empty registry (skeleton baseline before any operator
//! registers). 1 = single Source or single Aggregate. 6 = one of each
//! spill-capable operator category (Source, Aggregate, Sort,
//! GraceHash, SortMerge, node_buffer slot). 24 = a deep pipeline with
//! multiple parallel arms. 48 / 96 = extreme fan-out, used to
//! characterize the linear-scan ceiling of the registry.
//!
//! The registry is a lock-free copy-on-write `ArcSwap<Vec<..>>`
//! snapshot: the hot read paths load the current immutable Vec without
//! a lock and walk it `O(N)`. These benches measure that per-call scan
//! cost so a regression past the ~5µs/call budget for a deep pipeline
//! is visible.

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

    fn try_spill(&self, _target_bytes: u64) -> Result<u64, ConsumerSpillError> {
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
        arbitrator.register_consumer(Arc::new(FakeConsumer::new(priority)));
    }
    arbitrator
}

fn bench_should_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("arbitration_should_spill");
    for n in [0usize, 1, 6, 24, 48, 96] {
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
    for n in [0usize, 1, 6, 24, 48, 96] {
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
