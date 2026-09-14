use std::io::Write;
use std::num::NonZeroUsize;

use clinker_format::FormatError;
use clinker_format::preparation::MemoryOnlyResources;
use clinker_format::preparation::{FormatEncoder, OutputOperation, PreparedWriter, WriterScope};
use clinker_format::preparation::{ResourceError, ResourceErrorKind, StageStorage, StorageStage};
use clinker_format::reserved::ReservedBuffer;
use clinker_format::reserved::ReservedVec;

struct FaultAllocator;
thread_local! {
    static ALLOCATIONS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static FAIL_NEXT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
// SAFETY: successful allocations and all deallocations use System with the
// original layouts. Failure returns null, as GlobalAlloc permits. Thread-local
// scalar tracking neither allocates nor affects other test threads.
unsafe impl std::alloc::GlobalAlloc for FaultAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| {
            if let Some(n) = count.get() {
                count.set(Some(n + 1));
            }
        });
        if FAIL_NEXT
            .try_with(|fail| fail.replace(false))
            .unwrap_or(false)
        {
            return std::ptr::null_mut();
        }
        // SAFETY: the caller supplies a valid allocation layout.
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: every non-null allocation above came from System.
        unsafe { std::alloc::System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: FaultAllocator = FaultAllocator;

fn allocation_probe<T>(fail_next: bool, operation: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FAIL_NEXT.with(|fail| fail.set(false));
            ALLOCATIONS.with(|count| count.set(None));
        }
    }
    let reset = Reset;
    ALLOCATIONS.with(|count| count.set(Some(0)));
    FAIL_NEXT.with(|fail| fail.set(fail_next));
    let result = operation();
    let allocations = ALLOCATIONS.with(|count| count.get().unwrap());
    drop(reset);
    (result, allocations)
}

#[test]
fn memory_resource_refusal_does_not_allocate_an_error() {
    for allocation_failure in [false, true] {
        let limit = if allocation_failure { 128 * 1024 } else { 1 };
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(limit).unwrap());
        let mut storage =
            clinker_format::preparation::MemoryStorage::new(provider.resources().scope().unwrap());
        let (result, allocations) = allocation_probe(allocation_failure, || storage.write(b"x"));
        assert!(result.is_err());
        assert_eq!(allocations, usize::from(allocation_failure));
        assert!(result.unwrap_err().get_ref().is_none());
        assert_eq!(storage.len(), 0);
        assert_eq!(provider.used(), 0);
        let expected = ResourceError::new(
            if allocation_failure {
                ResourceErrorKind::Allocation
            } else {
                ResourceErrorKind::Budget
            },
            clinker_format::preparation::STAGE_CHUNK_BYTES,
            usize::from(!allocation_failure),
        );
        assert_eq!(storage.failure(), Some(expected));
        let (retry, allocations) = allocation_probe(false, || storage.write(b"retry"));
        assert!(retry.is_err());
        assert_eq!(allocations, 0);
        assert_eq!(storage.seal(), Err(expected));
    }
}

#[test]
fn storage_recovers_exact_inline_evidence_on_write_flush_and_readback() {
    #[derive(Clone, Copy)]
    enum Boundary {
        Write,
        Flush,
        Read,
    }
    struct FailingStorage {
        boundary: Boundary,
        failed: bool,
        evidence: ResourceError,
    }
    impl Write for FailingStorage {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if matches!(self.boundary, Boundary::Write) {
                self.failed = true;
                Err(std::io::ErrorKind::Other.into())
            } else {
                Ok(bytes.len())
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.failed = true;
            Err(std::io::ErrorKind::Other.into())
        }
    }
    impl std::io::Read for FailingStorage {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            self.failed = true;
            Err(std::io::ErrorKind::Other.into())
        }
    }
    impl StageStorage for FailingStorage {
        fn failure(&self) -> Option<ResourceError> {
            self.failed.then_some(self.evidence)
        }
        fn seal(&mut self) -> Result<u64, ResourceError> {
            Ok(1)
        }
        fn complete(&mut self) -> Result<(), ResourceError> {
            Ok(())
        }
    }
    let evidence = ResourceError {
        kind: ResourceErrorKind::Cancelled,
        requested: 91,
        available: 17,
        field: Some(3),
        offset: Some(41),
    };
    for boundary in [Boundary::Write, Boundary::Flush, Boundary::Read] {
        let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
        let mut stage = StorageStage::create(
            provider.resources().scope().unwrap(),
            FailingStorage {
                boundary,
                failed: false,
                evidence,
            },
        )
        .unwrap();
        match boundary {
            Boundary::Write => {
                assert!(stage.write(b"x").is_err());
                assert_eq!(stage.failure(), Some(evidence));
                assert!(
                    matches!(stage.finish(), Err(FormatError::Resource(error)) if error == evidence)
                );
            }
            Boundary::Flush => {
                assert!(stage.flush().is_err());
                assert_eq!(stage.failure(), Some(evidence));
                assert!(
                    matches!(stage.finish(), Err(FormatError::Resource(error)) if error == evidence)
                );
            }
            Boundary::Read => {
                let mut output = Vec::new();
                let (result, allocations) =
                    allocation_probe(false, || stage.finish().unwrap().deliver(&mut output));
                assert_eq!(allocations, 0);
                assert!(matches!(result, Err(FormatError::Resource(error)) if error == evidence));
                assert!(output.is_empty());
            }
        }
        assert_eq!(provider.used(), 0);
    }
}

#[test]
fn memory_allocator_failure_reaches_prepared_writer_without_error_allocation() {
    struct FailDuringEncode;
    impl FormatEncoder for FailDuringEncode {
        type Pending = ();
        fn prepare(
            &self,
            _: OutputOperation<'_>,
            stage: &mut dyn Write,
            _: &WriterScope,
        ) -> Result<(), FormatError> {
            let (result, allocations) = allocation_probe(true, || stage.write_all(b"x"));
            assert_eq!(
                allocations, 1,
                "only the refused chunk allocation is attempted"
            );
            result?;
            Ok(())
        }
        fn commit(&mut self, _: ()) {
            panic!("failed preparation cannot commit");
        }
    }
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let mut writer =
        PreparedWriter::new(Vec::new(), FailDuringEncode, provider.resources()).unwrap();
    let error = writer
        .write_operation(OutputOperation::Finalize)
        .unwrap_err();
    assert!(
        matches!(error, FormatError::Resource(error) if error.kind == clinker_format::preparation::ResourceErrorKind::Allocation)
    );
    assert!(writer.destination().is_empty());
    assert_eq!(provider.used(), 0);
}

struct Encoder {
    committed: usize,
    reject: bool,
}
impl FormatEncoder for Encoder {
    type Pending = usize;
    fn prepare(
        &self,
        _: OutputOperation<'_>,
        stage: &mut dyn Write,
        _: &WriterScope,
    ) -> Result<usize, FormatError> {
        stage.write_all(b"sealed")?;
        if self.reject {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData).into());
        }
        Ok(self.committed + 1)
    }
    fn commit(&mut self, pending: usize) {
        self.committed = pending;
    }
}

#[test]
fn memory_stage_seals_and_delivers_exact_bytes() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let resources = provider.resources();
    let scope = resources.scope().unwrap();
    let mut stage = scope.stage().unwrap();
    stage.write_all(b"one operation\n").unwrap();
    let prepared = stage.finish().unwrap();
    assert_eq!(prepared.len(), 14);
    let mut destination = Vec::new();
    prepared.deliver(&mut destination).unwrap();
    assert_eq!(destination, b"one operation\n");
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_growth_reserves_old_and_new_blocks_together() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(24).unwrap());
    let scope = provider.resources().scope().unwrap();
    let mut bytes = ReservedBuffer::new(scope);
    bytes.extend_from_slice(&[1; 16]).unwrap();
    assert_eq!(provider.used(), 16);
    assert!(bytes.extend_from_slice(&[2; 16]).is_err());
    assert_eq!(bytes.as_slice(), &[1; 16]);
    assert_eq!(provider.used(), 16);
    drop(bytes);
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_preparation_error_preserves_destination_and_committed_state() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let mut writer = PreparedWriter::new(
        Vec::new(),
        Encoder {
            committed: 0,
            reject: true,
        },
        provider.resources(),
    )
    .unwrap();
    assert!(writer.write_operation(OutputOperation::Finalize).is_err());
    assert!(writer.destination().is_empty());
    assert_eq!(writer.encoder().committed, 0);
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_delivery_failure_poisons_and_never_commits_or_retries() {
    struct Fail {
        attempts: usize,
        bytes: Vec<u8>,
    }
    impl Write for Fail {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.attempts += 1;
            if self.attempts > 1 {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            self.bytes.push(bytes[0]);
            Ok(1)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            panic!("poisoned writer must not flush")
        }
    }
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let mut writer = PreparedWriter::new(
        Fail {
            attempts: 0,
            bytes: Vec::new(),
        },
        Encoder {
            committed: 0,
            reject: false,
        },
        provider.resources(),
    )
    .unwrap();
    assert!(writer.write_operation(OutputOperation::Finalize).is_err());
    assert!(writer.flush().is_err());
    assert!(writer.write_operation(OutputOperation::Finalize).is_err());
    assert_eq!(writer.encoder().committed, 0);
    assert_eq!(writer.destination().attempts, 2);
    assert_eq!(writer.destination().bytes, b"s");
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_alignment_partial_initialization_and_destructor_panic_release() {
    #[repr(align(256))]
    struct Aligned;
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(4096).unwrap());
    let scope = provider.resources().scope().unwrap();
    let mut aligned = ReservedVec::new(scope.clone());
    aligned.push(Aligned).unwrap();
    assert_eq!(aligned.as_slice().as_ptr() as usize % 256, 0);
    assert_eq!(provider.used(), 0, "aligned ZST needs no allocation");
    struct Drops(std::sync::Arc<std::sync::atomic::AtomicUsize>, bool);
    impl Drop for Drops {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(!self.1, "injected destructor panic");
        }
    }
    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut values = ReservedVec::new(scope);
    values.reserve_exact(8).unwrap();
    values.push(Drops(count.clone(), true)).unwrap();
    values.push(Drops(count.clone(), false)).unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(values))).is_err());
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_failed_stage_cannot_seal_and_grants_move_split_merge() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let scope = provider.resources().scope().unwrap();
    let mut grant = scope
        .reserve(std::alloc::Layout::from_size_align(100, 1).unwrap())
        .unwrap();
    let part = grant.split(40).unwrap();
    assert_eq!(provider.used(), 100);
    grant.merge(part).unwrap();
    drop(grant);
    assert_eq!(provider.used(), 0);
    let mut stage = scope.stage().unwrap();
    assert!(stage.write_all(&[1; 256 * 1024]).is_err());
    assert_eq!(
        stage.failure().unwrap().kind,
        clinker_format::preparation::ResourceErrorKind::Budget
    );
    assert!(stage.finish().is_err());
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_zero_capacity_alignment_and_finite_startup_refusal() {
    #[repr(align(256))]
    struct Aligned(u8);
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(4096).unwrap());
    let mut values = ReservedVec::new(provider.resources().scope().unwrap());
    assert!(values.as_slice().is_empty());
    values.push(Aligned(7)).unwrap();
    assert_eq!(values.as_slice().as_ptr() as usize % 256, 0);
    assert_eq!(values.as_slice()[0].0, 7);
    assert_eq!(provider.used(), 256);
    drop(values);
    assert!(provider.resources().scope().unwrap().stage().is_err());
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_finalize_commits_once_and_zero_acceptance_also_poisons() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap());
    let mut writer = PreparedWriter::new(
        Vec::new(),
        Encoder {
            committed: 0,
            reject: false,
        },
        provider.resources(),
    )
    .unwrap();
    writer.flush().unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.encoder().committed, 1);
    assert_eq!(writer.destination(), b"sealed");
    let mut writer = PreparedWriter::new(
        std::io::Cursor::new([0u8; 0]),
        Encoder {
            committed: 0,
            reject: false,
        },
        provider.resources(),
    )
    .unwrap();
    assert!(writer.flush().is_err());
    assert!(writer.flush().is_err());
    assert_eq!(writer.encoder().committed, 0);
}

#[test]
fn memory_standalone_stage_has_no_hidden_operation_byte_cap() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let mut stage = provider.resources().scope().unwrap().stage().unwrap();
    let bytes = vec![42; 192 * 1024];
    stage.write_all(&bytes).unwrap();
    let mut output = Vec::new();
    stage.finish().unwrap().deliver(&mut output).unwrap();
    assert_eq!(output, bytes);
    assert_eq!(provider.used(), 0);
}

#[test]
fn memory_many_appends_have_geometric_growth_and_exact_fallback() {
    let provider = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let mut bytes = ReservedBuffer::new(provider.resources().scope().unwrap());
    let mut replacements = 0;
    let mut capacity = 0;
    for _ in 0..10000 {
        bytes.extend_from_slice(b"x").unwrap();
        if bytes.capacity() != capacity {
            replacements += 1;
            capacity = bytes.capacity();
        }
    }
    assert!(
        replacements <= 15,
        "linear append workload must not cause linear reallocations"
    );
    let small = MemoryOnlyResources::new(NonZeroUsize::new(9).unwrap());
    let mut bytes = ReservedBuffer::new(small.resources().scope().unwrap());
    bytes.extend_from_slice(b"1234").unwrap();
    bytes.extend_from_slice(b"5").unwrap();
    assert_eq!(bytes.capacity(), 5);
    assert_eq!(bytes.as_slice(), b"12345");
}
