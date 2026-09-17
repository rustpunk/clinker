use clinker_exec::{
    executor::preparation::ExecutorResources,
    pipeline::{
        memory::{MemoryArbitrator, NoOpPolicy},
        shutdown::ShutdownToken,
    },
};
use clinker_record::owned_storage::ResourceErrorKind;

#[test]
fn allocation_capability_clone_and_query_do_not_allocate() {
    let arb = Arc::new(MemoryArbitrator::with_policy(
        4096,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let weak = Arc::downgrade(&arb);
    let observer = arb.writer_resource_observer();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::MIN,
        None,
    )
    .unwrap();
    let allocation = provider.allocation();
    let writers = provider.resources();
    let scope = allocation.scope().unwrap();
    let value = clinker_record::FieldStr::try_new(
        "a governed string long enough to require shared heap backing",
        &scope,
    )
    .unwrap();
    let lease = scope.reserve(Layout::new::<[u8; 64]>()).unwrap();
    let foreign =
        clinker_format::preparation::MemoryOnlyResources::new(NonZeroUsize::new(4096).unwrap());
    let foreign_resources = foreign.resources();
    let foreign_lease = foreign_resources
        .allocation()
        .reserve(scope.owner(), Layout::new::<[u8; 64]>())
        .unwrap();
    assert_eq!(lease.owner(), foreign_lease.owner());
    let charged = observer.usage().memory;
    assert!(charged > 64);

    ALLOCATIONS.with(|count| count.set(Some(0)));
    let cloned = allocation.clone();
    let same_adapter = cloned.identity() == writers.allocation().identity();
    let local_bytes = value.unaccounted_heap_size(&cloned);
    let foreign_bytes = value.unaccounted_heap_size(foreign_resources.allocation());
    let local_lease = lease.is_accounted_by(writers.allocation());
    let unrelated_lease = foreign_lease.is_accounted_by(&cloned);
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(
        allocations, 0,
        "cloning capabilities and borrowed queries allocate nothing"
    );
    assert!(same_adapter);
    assert_eq!(local_bytes, 0);
    assert!(foreign_bytes > 0);
    assert!(local_lease);
    assert!(!unrelated_lease);
    assert_eq!(observer.usage().memory, charged);

    drop(writers);
    drop(provider);
    drop(arb);
    assert!(
        weak.upgrade().is_none(),
        "allocation capabilities must not retain the run"
    );
    assert!(observer.is_closed());
    assert!(!observer.has_managed_handle());
    assert_eq!(observer.usage().memory, charged);
    assert_eq!(
        cloned.scope().err().unwrap().kind,
        ResourceErrorKind::Finalized
    );
    drop(value);
    assert_eq!(observer.usage().memory, 64);
    drop(lease);
    assert_eq!(observer.usage().memory, 0);
}

#[test]
fn allocation_release_after_run_retains_only_live_charge() {
    let arb = Arc::new(MemoryArbitrator::with_policy(
        1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let weak = Arc::downgrade(&arb);
    let observer = arb.writer_resource_observer();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let allocation = provider.allocation();
    let scope = allocation.scope().unwrap();
    let lease = scope.reserve(Layout::new::<[u8; 64]>()).unwrap();
    assert_eq!(arb.consumer_count(), 1);
    assert!(observer.has_managed_handle());
    drop(provider);
    arb.close_writer_resources();
    assert_eq!(
        arb.consumer_count(),
        0,
        "escaped authority is not a registered run consumer"
    );
    assert!(!observer.has_managed_handle());
    assert_eq!(observer.usage().memory, 64);
    assert_eq!(
        scope.reserve(Layout::new::<u8>()).err().unwrap().kind,
        ResourceErrorKind::Finalized
    );
    drop(arb);
    assert!(
        weak.upgrade().is_none(),
        "the release observer and scope must not retain the run"
    );
    assert!(observer.is_closed());
    assert_eq!(observer.usage().memory, 64);
    assert_eq!(
        allocation.scope().err().unwrap().kind,
        ResourceErrorKind::Finalized
    );
    ALLOCATIONS.with(|count| count.set(Some(0)));
    drop(lease);
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(allocations, 0, "release after teardown needs no allocation");
    assert_eq!(observer.usage().memory, 0);
    assert_eq!(observer.usage().peak_memory, 64);
}

#[test]
fn allocation_release_after_run_drop_closes_without_explicit_shutdown() {
    let arb = Arc::new(MemoryArbitrator::with_policy(
        1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let observer = arb.writer_resource_observer();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let scope = provider.allocation().scope().unwrap();
    let lease = scope.reserve(Layout::new::<u64>()).unwrap();
    drop(provider);
    drop(arb);
    assert!(observer.is_closed());
    assert!(!observer.has_managed_handle());
    assert_eq!(observer.usage().memory, 8);
    drop(lease);
    assert_eq!(observer.usage().memory, 0);
}

#[test]
fn allocation_release_after_run_preserves_actual_disk_cleanup_result() {
    for restore_before_drop in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let weak = Arc::downgrade(&arb);
        let observer = arb.writer_resource_observer();
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            Some(&configured(root.path())),
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let lease = provider
            .allocation()
            .scope()
            .unwrap()
            .reserve(Layout::new::<[u8; 64]>())
            .unwrap();
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        stage.write_all(&vec![1; 100 * 1024]).unwrap();
        let prepared = stage.finish().unwrap();
        assert_eq!(observer.usage().disk, 100 * 1024);
        assert_eq!(observer.usage().descriptors, 1);
        let path = std::fs::read_dir(root.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let saved = root.path().join("retained");
        std::fs::rename(&path, &saved).unwrap();
        std::fs::create_dir(&path).unwrap();
        drop(prepared);
        assert_eq!(provider.cleanup_debt_count(), 1);
        drop(provider);
        assert_eq!(arb.retry_writer_cleanup(), 1);
        assert_eq!(arb.consumer_count(), 1);
        if restore_before_drop {
            std::fs::remove_dir(&path).unwrap();
            std::fs::rename(&saved, &path).unwrap();
        }
        drop(arb);
        assert!(weak.upgrade().is_none());
        assert!(observer.is_closed());
        assert!(!observer.has_managed_handle());
        assert_eq!(
            observer.usage().memory,
            64,
            "cleanup metadata has actually been dropped"
        );
        assert_eq!(
            observer.usage().descriptors,
            0,
            "the owned file was closed before debt retention"
        );
        assert_eq!(
            observer.usage().disk,
            if restore_before_drop { 0 } else { 100 * 1024 }
        );
        if restore_before_drop {
            assert!(!path.exists());
        } else {
            assert_eq!(std::fs::metadata(&saved).unwrap().len(), 100 * 1024);
        }
        drop(lease);
        assert_eq!(observer.usage().memory, 0);
    }
}

#[test]
fn allocation_close_release_and_admission_are_serialized() {
    for _ in 0..16 {
        let arb = Arc::new(MemoryArbitrator::with_policy(
            64,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let scope = provider.allocation().scope().unwrap();
        let lease = scope.reserve(Layout::new::<[u8; 64]>()).unwrap();
        let barrier = std::sync::Barrier::new(3);
        std::thread::scope(|threads| {
            threads.spawn(|| {
                barrier.wait();
                drop(lease);
            });
            threads.spawn(|| {
                barrier.wait();
                match scope.reserve(Layout::new::<[u8; 64]>()) {
                    Ok(lease) => drop(lease),
                    Err(error) => assert!(matches!(
                        error.kind,
                        ResourceErrorKind::Budget | ResourceErrorKind::Finalized
                    )),
                }
            });
            barrier.wait();
            arb.close_writer_resources();
        });
        assert_eq!(arb.writer_resource_usage().memory, 0);
        assert!(arb.writer_resource_usage().peak_memory <= 64);
        assert_eq!(arb.consumer_count(), 0);
        assert_eq!(
            scope.reserve(Layout::new::<u8>()).err().unwrap().kind,
            ResourceErrorKind::Finalized
        );
    }
}

#[test]
fn allocation_shutdown_before_or_after_reservation_preserves_release() {
    for cancel_first in [true, false] {
        let arb = Arc::new(MemoryArbitrator::with_policy(
            64,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let token = ShutdownToken::detached();
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            None,
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let scope = provider.allocation().scope().unwrap();
        let observer = arb.writer_resource_observer();
        if cancel_first {
            token.request();
        }
        let lease = scope.reserve(Layout::new::<[u8; 64]>());
        if cancel_first {
            assert_eq!(lease.err().unwrap().kind, ResourceErrorKind::Cancelled);
            assert_eq!(observer.usage().memory, 0);
        } else {
            let lease = lease.unwrap();
            token.request();
            assert_eq!(
                scope.reserve(Layout::new::<u8>()).err().unwrap().kind,
                ResourceErrorKind::Cancelled
            );
            assert_eq!(observer.usage().memory, 64);
            drop(provider);
            drop(arb);
            assert!(observer.is_closed());
            drop(lease);
            assert_eq!(observer.usage().memory, 0);
        }
    }
}

struct CountingAllocator;
thread_local! {
    static ALLOCATIONS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static FAIL_ALLOCATION: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}
// SAFETY: allocation and deallocation delegate to System with unchanged
// layouts. Thread-local scalar counting neither allocates nor crosses threads.
unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let attempt = ALLOCATIONS
            .try_with(|count| {
                if let Some(n) = count.get() {
                    count.set(Some(n + 1));
                    Some(n + 1)
                } else {
                    None
                }
            })
            .ok()
            .flatten();
        if attempt.is_some()
            && FAIL_ALLOCATION
                .try_with(|fail| fail.get() == attempt)
                .unwrap_or(false)
        {
            return std::ptr::null_mut();
        }
        // SAFETY: the caller supplies a valid allocation layout.
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: every allocation above came from System.
        unsafe { std::alloc::System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn telemetry() -> (
    clinker_exec::telemetry::TelemetryProducer,
    clinker_exec::telemetry::TelemetryReceiver,
) {
    let config = clinker_plan::config::ClinkerToml::parse(
        r#"
[observability]
arena_bytes = "768KB"
ordinary_lane_bytes = "512KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
rate_limit_per_second = 100000
rate_limit_burst = 100000
[observability.otlp]
endpoint = "https://collector.invalid"
[observability.otlp.auth]
mode = "none"
"#,
    )
    .unwrap();
    clinker_exec::telemetry::TelemetryArena::reserve(&config.resolve_observability(None).unwrap())
        .unwrap()
}

#[test]
fn first_telemetry_emission_uses_only_startup_allocations() {
    use clinker_exec::telemetry::{AdmissionOutcome, SpanFact, SpanName, SpanStatus};
    let (producer, receiver) = telemetry();
    assert_eq!(producer.snapshot().accepted, 0);
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = producer.emit_span(SpanFact {
        name: SpanName::WriterStage,
        status: SpanStatus::Ok,
        logical_node: "writer.stage",
        started_at_unix_nanos: 1,
        ended_at_unix_nanos: 2,
    });
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(allocations, 0);
    assert!(matches!(result, AdmissionOutcome::Accepted { .. }));
    assert_eq!(receiver.try_recv_batch().unwrap().traces().len(), 1);
}

#[test]
fn first_spill_stage_allocates_only_admitted_progress_and_metadata() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let baseline = arb.writer_resource_usage().memory;
    let scope = provider.resources().scope().unwrap();
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = scope.stage();
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(
        allocations, 2,
        "only the admitted progress buffer and stage box"
    );
    drop(result.unwrap());
    assert_eq!(arb.writer_resource_usage().memory, baseline);
    assert_eq!(arb.writer_resource_usage().descriptors, 0);
}

#[test]
fn stage_telemetry_observes_construction_denial_and_allocator_failure() {
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    use clinker_format::preparation::ResourceErrorKind;
    // Deny metadata, deny progress, fail progress allocation, fail stage box.
    for (limit, fail_at) in [
        (1, None),
        (4096, None),
        (128 * 1024, Some(1)),
        (128 * 1024, Some(2)),
    ] {
        let (producer, receiver) = telemetry();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            limit,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            None,
            NonZeroUsize::new(1).unwrap(),
            Some(producer),
        )
        .unwrap();
        let scope = provider.resources().scope().unwrap();
        ALLOCATIONS.with(|count| count.set(Some(0)));
        FAIL_ALLOCATION.with(|fail| fail.set(fail_at));
        let result = scope.stage();
        FAIL_ALLOCATION.with(|fail| fail.set(None));
        let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
        if let Some(fail_at) = fail_at {
            assert_eq!(allocations, fail_at);
        }
        assert!(
            matches!(result, Err(clinker_format::FormatError::Resource(error)) if error.kind == if fail_at.is_some() { ResourceErrorKind::Allocation } else { ResourceErrorKind::Budget })
        );
        assert_eq!(arb.writer_resource_usage().memory, 0);
        let batch = receiver.try_recv_batch().unwrap();
        assert_eq!(batch.metric(MetricKey::WriterStageStarted), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageFailed), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageDropped), 0);
        assert_eq!(batch.metric(MetricKey::WriterStageCompleted), 0);
        assert_eq!(
            batch.metric(MetricKey::WriterAdmissionFailed),
            u64::from(fail_at.is_none())
        );
        let spans: Vec<_> = batch
            .traces()
            .iter()
            .filter(|span| span.name == SpanName::WriterStage)
            .collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].status, SpanStatus::Error);
    }
}

#[test]
fn stage_telemetry_distinguishes_completion_cancellation_and_abandonment() {
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    for outcome in [
        MetricKey::WriterStageCompleted,
        MetricKey::WriterStageInterrupted,
        MetricKey::WriterStageDropped,
        MetricKey::WriterStageFailed,
    ] {
        let (producer, receiver) = telemetry();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let token = ShutdownToken::detached();
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            None,
            NonZeroUsize::new(1).unwrap(),
            Some(producer),
        )
        .unwrap();
        let scope = provider.resources().scope().unwrap();
        let mut stage = scope.stage().unwrap();
        let mut output = Vec::new();
        match outcome {
            MetricKey::WriterStageCompleted => {
                stage.write_all(b"complete").unwrap();
                stage.finish().unwrap().deliver(&mut output).unwrap();
                assert_eq!(output, b"complete");
            }
            MetricKey::WriterStageInterrupted => {
                token.request();
                assert!(stage.write_all(b"cancelled before storage").is_err());
                assert!(stage.finish().is_err());
            }
            MetricKey::WriterStageFailed => {
                assert!(stage.write_all(&[1; 256 * 1024]).is_err());
                assert!(stage.finish().is_err());
            }
            _ => {
                // A shutdown request alone is not an observed cancellation
                // result; abandoning without another operation remains dropped.
                token.request();
                drop(stage);
            }
        }
        assert_eq!(arb.writer_resource_usage().memory, 0);
        let batch = receiver.try_recv_batch().unwrap();
        assert_eq!(batch.metric(MetricKey::WriterStageStarted), 1);
        assert_eq!(batch.metric(outcome), 1);
        assert_eq!(
            [
                MetricKey::WriterStageCompleted,
                MetricKey::WriterStageFailed,
                MetricKey::WriterStageInterrupted,
                MetricKey::WriterStageDropped
            ]
            .into_iter()
            .map(|key| batch.metric(key))
            .sum::<u64>(),
            1
        );
        let spans: Vec<_> = batch
            .traces()
            .iter()
            .filter(|span| span.name == SpanName::WriterStage)
            .collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].status,
            match outcome {
                MetricKey::WriterStageCompleted => SpanStatus::Ok,
                MetricKey::WriterStageFailed => SpanStatus::Error,
                _ => SpanStatus::Unset,
            }
        );
        assert!(spans[0].started_at_unix_nanos <= spans[0].ended_at_unix_nanos);
        assert_eq!(spans[0].logical_node, "writer.stage");
    }
}

#[test]
fn stage_spill_and_cleanup_telemetry_cannot_block_a_full_arena() {
    use clinker_exec::telemetry::{
        AdmissionOutcome, DropReason, MetricKey, SpanFact, SpanName, SpanStatus,
    };
    for full in [false, true] {
        let (producer, receiver) = telemetry();
        if full {
            for status in [SpanStatus::Ok, SpanStatus::Error] {
                loop {
                    let result = producer.emit_span(SpanFact {
                        name: SpanName::Transform,
                        status,
                        logical_node: "fill",
                        started_at_unix_nanos: 1,
                        ended_at_unix_nanos: 2,
                    });
                    if result == AdmissionOutcome::Dropped(DropReason::Full) {
                        break;
                    }
                    assert!(matches!(result, AdmissionOutcome::Accepted { .. }));
                }
            }
        }
        let baseline = producer.snapshot();
        let token = ShutdownToken::detached();
        let root = tempfile::tempdir().unwrap();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            Some(&configured(root.path())),
            NonZeroUsize::new(1).unwrap(),
            Some(producer.clone()),
        )
        .unwrap();
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        stage.write_all(&[7; 100 * 1024]).unwrap();
        let mut output = Vec::new();
        stage.finish().unwrap().deliver(&mut output).unwrap();
        assert_eq!(output, [7; 100 * 1024]);
        arb.set_max_spill_bytes(0).unwrap();
        let scope = provider.resources().scope().unwrap();
        let mut refused = scope.stage().unwrap();
        assert!(refused.write_all(&[9; 100 * 1024]).is_err());
        assert!(refused.finish().is_err());
        let mut cancelled = scope.stage().unwrap();
        token.request();
        assert!(cancelled.write_all(b"cancelled").is_err());
        assert!(cancelled.finish().is_err());
        assert_eq!(arb.writer_resource_usage().disk, 0);
        assert_eq!(arb.writer_resource_usage().descriptors, 0);
        assert_eq!(producer.snapshot().owned_bytes, baseline.owned_bytes);
        if full {
            assert_eq!(producer.snapshot().accepted, baseline.accepted);
        }
        let batch = receiver.try_recv_batch().unwrap();
        assert_eq!(batch.metric(MetricKey::WriterStageCompleted), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageFailed), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageInterrupted), 1);
        assert_eq!(batch.metric(MetricKey::WriterSpillStarted), 2);
        assert_eq!(batch.metric(MetricKey::WriterSpillCompleted), 1);
        assert_eq!(batch.metric(MetricKey::WriterSpillFailed), 1);
        assert_eq!(batch.metric(MetricKey::WriterSpillBytes), 100 * 1024);
        assert_eq!(batch.metric(MetricKey::WriterCleanupCompleted), 3);
    }
}

#[test]
fn writer_primitive_telemetry_vocabulary_is_closed_and_serializable() {
    use clinker_exec::telemetry::{MetricKey, SpanName};
    for name in [
        "writer_admission",
        "writer_stage",
        "writer_spill",
        "writer_cleanup",
    ] {
        let span: SpanName = serde_json::from_str(&format!("\"{name}\"")).unwrap();
        assert_eq!(serde_json::to_string(&span).unwrap(), format!("\"{name}\""));
        for outcome in ["started", "completed", "failed", "interrupted"] {
            let key = format!("\"{name}_{outcome}\"");
            let metric: MetricKey = serde_json::from_str(&key).unwrap();
            assert_eq!(serde_json::to_string(&metric).unwrap(), key);
        }
    }
    for name in ["writer_stage_dropped", "writer_spill_bytes"] {
        let metric: MetricKey = serde_json::from_str(&format!("\"{name}\"")).unwrap();
        assert!(MetricKey::ALL.contains(&metric));
    }
}

#[test]
fn stage_disk_refusal_preserves_quota_evidence_without_error_allocation() {
    use clinker_format::preparation::{ResourceError, ResourceErrorKind};
    let (producer, receiver) = telemetry();
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    arb.set_max_spill_bytes(72 * 1024).unwrap();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        Some(producer),
    )
    .unwrap();
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(&[7; 72 * 1024]).unwrap();
    assert_eq!(arb.writer_resource_usage().disk, 72 * 1024);
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = stage.write(&[9; 1024]);
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert!(result.is_err());
    assert_eq!(allocations, 0);
    let expected = ResourceError::new(ResourceErrorKind::DiskQuota, 1024, 0);
    assert_eq!(stage.failure(), Some(expected));
    assert!(
        matches!(stage.finish(), Err(clinker_format::FormatError::Resource(error)) if error == expected)
    );
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert_eq!(arb.writer_resource_usage().descriptors, 0);
    assert_eq!(
        receiver
            .try_recv_batch()
            .unwrap()
            .metric(clinker_exec::telemetry::MetricKey::WriterStageFailed),
        1
    );
}

fn configured(root: &std::path::Path) -> clinker_exec::executor::ResolvedStorage {
    clinker_exec::executor::ResolvedStorage {
        spill_root_dir: Some(root.to_owned()),
        free_space_warning: None,
        cap_headroom_warning: None,
    }
}

fn cancelled_delivery_releases_resources(empty: bool) {
    use clinker_exec::telemetry::{MetricKey, SpanName, SpanStatus};
    use clinker_format::{
        FormatError,
        preparation::{ResourceError, ResourceErrorKind},
    };

    struct CancelOnLastWrite {
        token: ShutdownToken,
        remaining: usize,
        output: Vec<u8>,
    }
    impl Write for CancelOnLastWrite {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.output.extend_from_slice(bytes);
            self.remaining -= bytes.len();
            if self.remaining == 0 {
                self.token.request();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("delivery does not flush")
        }
    }
    for spill in [false, true] {
        let (producer, receiver) = telemetry();
        let root = tempfile::tempdir().unwrap();
        let arb = Arc::new(MemoryArbitrator::with_policy(
            128 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let token = ShutdownToken::detached();
        let storage = configured(root.path());
        let provider = ExecutorResources::new(
            arb.clone(),
            token.clone(),
            spill.then_some(&storage),
            NonZeroUsize::new(1).unwrap(),
            Some(producer),
        )
        .unwrap();
        let baseline = arb.writer_resource_usage().memory;
        let payload = vec![
            7;
            if empty {
                0
            } else if spill {
                100 * 1024
            } else {
                1024
            }
        ];
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        stage.write_all(&payload).unwrap();
        let prepared = stage.finish().unwrap();
        assert_eq!(prepared.len(), payload.len() as u64);
        if spill && !empty {
            assert_eq!(arb.writer_resource_usage().disk, payload.len() as u64);
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        }
        if empty {
            token.request();
        }
        let mut destination = CancelOnLastWrite {
            token,
            remaining: payload.len(),
            output: Vec::new(),
        };
        assert!(
            matches!(prepared.deliver(&mut destination), Err(FormatError::Resource(error)) if error == ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
        );
        assert_eq!(destination.output, payload);
        assert_eq!(arb.writer_resource_usage().memory, baseline);
        assert_eq!(arb.writer_resource_usage().disk, 0);
        assert_eq!(arb.writer_resource_usage().descriptors, 0);
        assert_eq!(provider.cleanup_debt_count(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        let batch = receiver.try_recv_batch().unwrap();
        assert_eq!(batch.metric(MetricKey::WriterStageStarted), 1);
        assert_eq!(batch.metric(MetricKey::WriterStageInterrupted), 1);
        for key in [
            MetricKey::WriterStageCompleted,
            MetricKey::WriterStageFailed,
            MetricKey::WriterStageDropped,
        ] {
            assert_eq!(batch.metric(key), 0);
        }
        let spans: Vec<_> = batch
            .traces()
            .iter()
            .filter(|span| span.name == SpanName::WriterStage)
            .collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].status, SpanStatus::Unset);
        assert!(spans[0].started_at_unix_nanos <= spans[0].ended_at_unix_nanos);
        drop(provider);
        assert_eq!(arb.writer_resource_usage().memory, 0);
        assert_eq!(arb.consumer_count(), 0);
    }
}

#[test]
fn empty_delivery_cancellation_is_interrupted_and_releases_resources() {
    cancelled_delivery_releases_resources(true);
}

#[test]
fn final_write_cancellation_is_interrupted_and_releases_resources() {
    cancelled_delivery_releases_resources(false);
}

#[test]
fn stage_spills_with_all_other_memory_reserved_and_releases_file() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        96 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    arb.set_max_spill_bytes(1024 * 1024).unwrap();
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(2).unwrap(),
        None,
    )
    .unwrap();
    let baseline = arb.writer_resource_usage().memory;
    let scope = provider.resources().scope().unwrap();
    let mut stage = scope.stage().unwrap();
    let remaining = arb.limit() - arb.writer_resource_usage().memory;
    let pressure = scope
        .reserve(Layout::array::<u8>(remaining as usize).unwrap())
        .unwrap();
    let bytes = vec![42; 150 * 1024];
    stage.write_all(&bytes).unwrap();
    assert_eq!(arb.writer_resource_usage().memory, arb.limit());
    assert_eq!(arb.writer_resource_usage().disk, bytes.len() as u64);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    let mut output = Vec::new();
    stage.finish().unwrap().deliver(&mut output).unwrap();
    assert_eq!(output, bytes);
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert_eq!(arb.writer_resource_usage().descriptors, 0);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    drop(pressure);
    assert_eq!(arb.writer_resource_usage().memory, baseline);
    drop(scope);
    drop(provider);
    assert_eq!(arb.consumer_count(), 0);
    assert_eq!(arb.writer_resource_usage().memory, 0);
}

#[test]
fn stage_quota_and_cancellation_never_seal_or_touch_destination() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    arb.set_max_spill_bytes(20 * 1024).unwrap();
    let token = ShutdownToken::detached();
    let provider = ExecutorResources::new(
        arb.clone(),
        token.clone(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let baseline = arb.writer_resource_usage().memory;
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    assert!(stage.write_all(&vec![7; 100 * 1024]).is_err());
    assert!(stage.finish().is_err());
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert_eq!(arb.writer_resource_usage().memory, baseline);
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(b"prefix").unwrap();
    token.request();
    assert!(stage.write_all(b"suffix").is_err());
    assert!(stage.finish().is_err());
    assert_eq!(arb.writer_resource_usage().descriptors, 0);
    assert_eq!(arb.writer_resource_usage().memory, baseline);
}

#[test]
fn stage_descriptor_denial_and_limit_changes_are_atomic() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    let scope = provider.resources().scope().unwrap();
    let mut stage = scope.stage().unwrap();
    assert!(scope.stage().is_err());
    assert!(arb.set_limit(1).is_err());
    assert_eq!(arb.limit(), 128 * 1024);
    stage.write_all(&vec![1; 100 * 1024]).unwrap();
    assert!(arb.set_max_spill_bytes(1).is_err());
    assert_eq!(arb.max_spill_bytes(), u64::MAX);
    drop(stage);
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert!(scope.stage().is_ok());
}

#[test]
fn stage_failed_unlink_retains_bounded_debt_until_successful_cleanup() {
    let (producer, receiver) = telemetry();
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        Some(producer),
    )
    .unwrap();
    let scope = provider.resources().scope().unwrap();
    let mut stage = scope.stage().unwrap();
    stage.write_all(&vec![1; 100 * 1024]).unwrap();
    let prepared = stage.finish().unwrap();
    let path = std::fs::read_dir(root.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    // Closing the handle precedes unlink; inject the unlink fault via a directory
    // at the owned name while retaining the actual byte file for restoration.
    let saved = root.path().join("retained");
    std::fs::rename(&path, &saved).unwrap();
    std::fs::create_dir(&path).unwrap();
    drop(prepared);
    assert_eq!(provider.cleanup_debt_count(), 1);
    assert_eq!(arb.writer_resource_usage().disk, 100 * 1024);
    assert!(
        scope.stage().is_err(),
        "debt occupies the only cleanup slot"
    );
    provider.cleanup();
    assert_eq!(provider.cleanup_debt_count(), 1);
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(saved, path).unwrap();
    provider.cleanup();
    assert_eq!(provider.cleanup_debt_count(), 0);
    assert_eq!(arb.writer_resource_usage().disk, 0);
    let batch = receiver.try_recv_batch().unwrap();
    assert_eq!(
        batch.metric(clinker_exec::telemetry::MetricKey::WriterCleanupFailed),
        2
    );
    assert_eq!(
        batch.metric(clinker_exec::telemetry::MetricKey::WriterCleanupCompleted),
        1
    );
    assert!(scope.stage().is_ok());
}

#[test]
fn stage_short_readback_refuses_incomplete_prepared_bytes() {
    let (producer, receiver) = telemetry();
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        Some(producer),
    )
    .unwrap();
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(&vec![1; 100 * 1024]).unwrap();
    let prepared = stage.finish().unwrap();
    let path = std::fs::read_dir(root.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(0)
        .unwrap();
    let mut output = Vec::new();
    assert!(prepared.deliver(&mut output).is_err());
    assert!(output.is_empty());
    assert_eq!(arb.writer_resource_usage().disk, 0);
    let batch = receiver.try_recv_batch().unwrap();
    assert_eq!(
        batch.metric(clinker_exec::telemetry::MetricKey::WriterStageFailed),
        1
    );
    assert_eq!(
        batch.metric(clinker_exec::telemetry::MetricKey::WriterStageDropped),
        0
    );
}

#[test]
fn stage_relative_and_long_configured_roots_preserve_exact_storage_directory() {
    let cwd = std::env::current_dir().unwrap();
    let relative = tempfile::Builder::new()
        .prefix("writer-relative-")
        .tempdir_in(&cwd)
        .unwrap();
    let relative_path = std::path::Path::new(relative.path().file_name().unwrap());
    let long = tempfile::tempdir().unwrap();
    let mut long_path = long.path().to_owned();
    for _ in 0..20 {
        long_path.push("several-components-for-path-budget");
    }
    std::fs::create_dir_all(&long_path).unwrap();
    for (configured_path, actual_path) in [
        (relative_path, relative.path()),
        (long_path.as_path(), long_path.as_path()),
    ] {
        let arb = Arc::new(MemoryArbitrator::with_policy(
            1024 * 1024,
            0.8,
            0.7,
            Box::new(NoOpPolicy),
        ));
        let provider = ExecutorResources::new(
            arb.clone(),
            ShutdownToken::detached(),
            Some(&configured(configured_path)),
            NonZeroUsize::new(1).unwrap(),
            None,
        )
        .unwrap();
        let mut stage = provider.resources().scope().unwrap().stage().unwrap();
        stage.write_all(&vec![9; 100 * 1024]).unwrap();
        assert_eq!(std::fs::read_dir(actual_path).unwrap().count(), 1);
        let mut output = Vec::new();
        stage.finish().unwrap().deliver(&mut output).unwrap();
        assert_eq!(output, vec![9; 100 * 1024]);
        assert_eq!(std::fs::read_dir(actual_path).unwrap().count(), 0);
        drop(provider);
        assert_eq!(arb.writer_resource_usage().memory, 0);
        assert_eq!(arb.consumer_count(), 0);
    }
}

#[test]
fn stage_cleanup_debt_outlives_provider_and_remains_retryable() {
    let root = tempfile::tempdir().unwrap();
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        Some(&configured(root.path())),
        NonZeroUsize::new(1).unwrap(),
        None,
    )
    .unwrap();
    assert_eq!(
        arb.retry_writer_cleanup(),
        0,
        "an early cleanup must not detach a live run's owner"
    );
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(&vec![1; 100 * 1024]).unwrap();
    let prepared = stage.finish().unwrap();
    let path = std::fs::read_dir(root.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let saved = root.path().join("retained");
    std::fs::rename(&path, &saved).unwrap();
    std::fs::create_dir(&path).unwrap();
    drop(prepared);
    drop(provider);
    assert_eq!(arb.retry_writer_cleanup(), 1);
    assert_eq!(arb.writer_resource_usage().disk, 100 * 1024);
    assert_eq!(arb.consumer_count(), 1, "debt still owns admitted metadata");
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(saved, path).unwrap();
    assert_eq!(arb.retry_writer_cleanup(), 0);
    assert_eq!(arb.writer_resource_usage().disk, 0);
    assert_eq!(arb.writer_resource_usage().memory, 0);
    assert_eq!(arb.consumer_count(), 0);
}
use std::{alloc::Layout, io::Write, num::NonZeroUsize, sync::Arc};

#[test]
fn stage_competing_grants_never_oversubscribe() {
    let arb = Arc::new(MemoryArbitrator::with_policy(
        1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(4).unwrap(),
        None,
    )
    .unwrap();
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
    let arb = Arc::new(MemoryArbitrator::with_policy(
        128 * 1024,
        0.8,
        0.7,
        Box::new(NoOpPolicy),
    ));
    let provider = ExecutorResources::new(
        arb.clone(),
        ShutdownToken::detached(),
        None,
        NonZeroUsize::new(4).unwrap(),
        None,
    )
    .unwrap();
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(b"exact bytes\n").unwrap();
    let mut destination = Vec::new();
    stage.finish().unwrap().deliver(&mut destination).unwrap();
    assert_eq!(destination, b"exact bytes\n");
    assert_eq!(arb.writer_resource_usage().memory, 0);
}
