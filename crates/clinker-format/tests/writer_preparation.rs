use std::io::Write;
use std::num::NonZeroUsize;

use clinker_format::FormatError;
use clinker_format::preparation::MemoryOnlyResources;
use clinker_format::preparation::{FormatEncoder, OutputOperation, PreparedWriter, WriterScope};
use clinker_format::reserved::ReservedBuffer;
use clinker_format::reserved::ReservedVec;

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
