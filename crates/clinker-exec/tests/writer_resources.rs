use clinker_exec::{
    executor::preparation::ExecutorResources,
    pipeline::{
        memory::{MemoryArbitrator, NoOpPolicy},
        shutdown::ShutdownToken,
    },
};

fn configured(root: &std::path::Path) -> clinker_exec::executor::ResolvedStorage {
    clinker_exec::executor::ResolvedStorage {
        spill_root_dir: Some(root.to_owned()),
        free_space_warning: None,
        cap_headroom_warning: None,
    }
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
    assert!(scope.stage().is_ok());
}

#[test]
fn stage_short_readback_refuses_incomplete_prepared_bytes() {
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
    )
    .unwrap();
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    stage.write_all(b"exact bytes\n").unwrap();
    let mut destination = Vec::new();
    stage.finish().unwrap().deliver(&mut destination).unwrap();
    assert_eq!(destination, b"exact bytes\n");
    assert_eq!(arb.writer_resource_usage().memory, 0);
}
