use std::{alloc::Layout, io::Write, num::NonZeroUsize, sync::Arc};
use clinker_exec::{executor::preparation::ExecutorResources, pipeline::{memory::{MemoryArbitrator, NoOpPolicy}, shutdown::ShutdownToken}};

#[test]
fn stage_competing_grants_never_oversubscribe() {
    let arb = Arc::new(MemoryArbitrator::with_policy(1024, 0.8, 0.7, Box::new(NoOpPolicy)));
    let provider = ExecutorResources::new(arb.clone(), ShutdownToken::detached(), None, NonZeroUsize::new(4).unwrap()).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    std::thread::scope(|threads| {
        for _ in 0..8 {
            let resources = provider.resources();
            let barrier = barrier.clone();
            threads.spawn(move || {
                let scope = resources.scope().unwrap();
                barrier.wait();
                let grant = scope.reserve(Layout::from_size_align(600, 1).unwrap());
                barrier.wait();
                drop(grant);
            });
        }
    });
    assert!(arb.writer_resource_usage().peak_memory <= 1024);
    assert_eq!(arb.writer_resource_usage().memory, 0);
    assert_eq!(arb.consumer_count(), 1);
    drop(provider);
    assert_eq!(arb.consumer_count(), 0);
}

#[test]
fn stage_memory_sealed_bytes_match_standalone() {
    let arb = Arc::new(MemoryArbitrator::with_policy(128 * 1024, 0.8, 0.7, Box::new(NoOpPolicy)));
    let provider = ExecutorResources::new(arb.clone(), ShutdownToken::detached(), None, NonZeroUsize::new(4).unwrap()).unwrap();
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(b"exact bytes\n").unwrap();
    let mut destination = Vec::new();
    stage.finish().unwrap().deliver(&mut destination).unwrap();
    assert_eq!(destination, b"exact bytes\n");
    assert_eq!(arb.writer_resource_usage().memory, 0);
}
