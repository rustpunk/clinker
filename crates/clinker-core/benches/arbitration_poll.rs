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

/// Integer EWMA (alpha = 1/8) of per-record byte sizes, mirroring the
/// private `ewma_step` the Source ingest channel folds samples through.
///
/// Inlined here rather than imported: benches link against the crate's
/// public API only, and the production step is a private free function
/// with no need to widen its visibility for a microbenchmark. The body
/// must track that function — `prev == 0` seeds, the `/ 8` decay is a
/// bit shift, and the subtraction is ordered to stay underflow-safe.
fn ewma_step(prev: u64, sample: u64) -> u64 {
    if prev == 0 {
        sample
    } else if sample >= prev {
        prev + (sample - prev) / 8
    } else {
        prev - (prev - sample) / 8
    }
}

/// Characterizes the per-sample EWMA update cost under both a uniform
/// record-size stream and a 10x mixed-size stream.
///
/// The mixed-size case is the drift scenario the smoothing targets: 99
/// small records punctuated by one kilobyte record, repeated. Folding
/// the whole sequence through the step exercises both the climbing and
/// decaying branches and confirms the update stays sub-microsecond on
/// the hot send path regardless of size variance.
fn bench_record_bytes_ewma(c: &mut Criterion) {
    let mut group = c.benchmark_group("arbitration_record_bytes_ewma");

    // Uniform: every record reports the same heap size, so the average
    // seeds once and then holds steady.
    let uniform: Vec<u64> = vec![256; 1000];
    group.bench_function("uniform", |b| {
        b.iter(|| {
            let mut ewma = 0u64;
            for &sample in &uniform {
                ewma = ewma_step(ewma, black_box(sample));
            }
            black_box(ewma)
        });
    });

    // Mixed: 99 small records then one ~1 KiB record, repeated — a 10x
    // intermittent drift that swings the raw last-sample proxy but that
    // the EWMA absorbs into a stable estimate.
    let mixed: Vec<u64> = (0..1000)
        .map(|i| if i % 100 == 99 { 1024 } else { 96 })
        .collect();
    group.bench_function("mixed_10x_drift", |b| {
        b.iter(|| {
            let mut ewma = 0u64;
            for &sample in &mixed {
                ewma = ewma_step(ewma, black_box(sample));
            }
            black_box(ewma)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_should_spill,
    bench_sum_consumer_usage,
    bench_record_bytes_ewma
);
criterion_main!(benches);
