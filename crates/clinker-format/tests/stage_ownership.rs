use clinker_format::FormatError;
use clinker_format::preparation::{
    MemoryOnlyResources, ResourceError, ResourceErrorKind, StageStorage, StorageStage, WriterScope,
};
use std::io::Write;
use std::num::NonZeroUsize;

struct FaultAllocator;
thread_local! {
    static ALLOCATIONS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static FAIL_NEXT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static ALLOCATIONS_LEFT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static BACKING_WATCH: std::cell::Cell<Option<BackingWatch>> = const { std::cell::Cell::new(None) };
}

#[derive(Clone, Copy)]
struct BackingWatch {
    provider: *const MemoryOnlyResources,
    pointer: *mut u8,
    bytes: usize,
    live_at_deallocation: Option<usize>,
    layout: Option<std::alloc::Layout>,
    deallocations: usize,
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
        if ALLOCATIONS_LEFT
            .try_with(|remaining| match remaining.get() {
                Some(0) => true,
                Some(count) => {
                    remaining.set(Some(count - 1));
                    false
                }
                None => false,
            })
            .unwrap_or(false)
        {
            return std::ptr::null_mut();
        }
        // SAFETY: the caller supplies a valid allocation layout.
        let pointer = unsafe { std::alloc::System.alloc(layout) };
        let _ = BACKING_WATCH.try_with(|watch| {
            if let Some(mut state) = watch.get()
                && state.pointer.is_null()
                && state.layout.is_none_or(|expected| expected == layout)
            {
                state.pointer = pointer;
                state.bytes = layout.size();
                watch.set(Some(state));
            }
        });
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        let _ = BACKING_WATCH.try_with(|watch| {
            if let Some(mut state) = watch.get()
                && state.pointer == pointer
                && state.live_at_deallocation.is_none()
            {
                // SAFETY: the test keeps the provider alive until the watch is
                // removed. The provider's fixed mutex was initialized before
                // observation; used() allocates nothing. Stop after the first
                // deallocation so later pointer reuse cannot replace evidence.
                state.live_at_deallocation = Some(unsafe { &*state.provider }.used());
                state.deallocations += 1;
                watch.set(Some(state));
            }
        });
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
            ALLOCATIONS_LEFT.with(|remaining| remaining.set(None));
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

mod cancellation_harness {
    use super::*;
    use clinker_format::preparation::{
        AllocationAuthority, AllocationLease, MemoryStorage, OperationStage, OwnerId,
        ResourceAuthority,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    pub(super) struct Authority {
        pub(super) memory: MemoryOnlyResources,
        pub(super) cancelled: Arc<AtomicBool>,
        pub(super) cancel_after_seal: bool,
    }
    struct CancelAfterSeal {
        storage: MemoryStorage,
        pub(super) cancelled: Arc<AtomicBool>,
    }
    impl Write for CancelAfterSeal {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.storage.write(bytes)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.storage.flush()
        }
    }
    impl std::io::Read for CancelAfterSeal {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.storage.read(bytes)
        }
    }
    impl StageStorage for CancelAfterSeal {
        fn resource_failed(&mut self, error: ResourceError) {
            self.storage.resource_failed(error);
        }
        fn failure(&self) -> Option<ResourceError> {
            self.storage.failure()
        }
        fn seal(&mut self) -> Result<u64, ResourceError> {
            let len = self.storage.seal()?;
            self.cancelled.store(true, Ordering::SeqCst);
            Ok(len)
        }
        fn complete(&mut self) -> Result<(), ResourceError> {
            self.storage.complete()
        }
    }
    impl AllocationAuthority for Authority {
        fn identity(&self) -> usize {
            self.memory.resources().allocation().identity()
        }
        fn try_reserve(
            self: Arc<Self>,
            owner: OwnerId,
            layout: std::alloc::Layout,
        ) -> Result<AllocationLease, ResourceError> {
            self.check_cancelled()?;
            self.memory.resources().allocation().reserve(owner, layout)
        }
        fn release(&self, _: OwnerId, _: usize) {
            unreachable!("grants belong to the delegated memory authority")
        }
        fn check_cancelled(&self) -> Result<(), ResourceError> {
            if self.cancelled.load(Ordering::SeqCst) {
                Err(ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
            } else {
                Ok(())
            }
        }
    }
    impl ResourceAuthority for Authority {
        fn create_stage(
            self: Arc<Self>,
            scope: WriterScope,
        ) -> Result<OperationStage, FormatError> {
            let storage = MemoryStorage::new(scope.clone());
            if self.cancel_after_seal {
                StorageStage::create(
                    scope,
                    CancelAfterSeal {
                        storage,
                        cancelled: self.cancelled.clone(),
                    },
                )
            } else {
                StorageStage::create(scope, storage)
            }
        }
    }
}

mod stage_lifetimes {
    use super::*;
    use clinker_format::preparation::{PROGRESS_BYTES, WriterResources};
    use std::alloc::Layout;
    use std::io::{Cursor, Read};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Watch<'a>(&'a MemoryOnlyResources);
    impl<'a> Watch<'a> {
        fn new(provider: &'a MemoryOnlyResources, layout: Layout) -> Self {
            assert_eq!(
                provider.used(),
                0,
                "no unrelated grants may mask early release"
            );
            BACKING_WATCH.with(|watch| {
                assert!(watch.get().is_none());
                watch.set(Some(BackingWatch {
                    provider,
                    pointer: std::ptr::null_mut(),
                    bytes: 0,
                    live_at_deallocation: None,
                    layout: Some(layout),
                    deallocations: 0,
                }));
            });
            Self(provider)
        }
        fn live(&self, layout: Layout) {
            let state = BACKING_WATCH.with(|watch| watch.get().unwrap());
            assert!(!state.pointer.is_null());
            assert_eq!(state.bytes, layout.size());
            assert_eq!(state.deallocations, 0);
            assert_eq!(state.live_at_deallocation, None);
        }
        fn released(&self, layout: Layout) {
            let state = BACKING_WATCH.with(|watch| watch.get().unwrap());
            assert_eq!(state.bytes, layout.size());
            assert_eq!(state.deallocations, 1);
            assert_eq!(
                state.live_at_deallocation,
                Some(layout.size()),
                "only the watched backing grant must remain when deallocation runs"
            );
            assert_eq!(self.0.used(), 0);
        }
    }
    impl Drop for Watch<'_> {
        fn drop(&mut self) {
            BACKING_WATCH.with(|watch| watch.set(None));
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Exit {
        EarlyDrop,
        SealedDrop,
        Delivered,
        WriteFailure,
        SealFailure,
        ReadbackFailure,
        CompletionFailure,
        DestinationFailure,
        CancelAfterSeal,
        CancelAfterWrite,
        Unwind,
    }
    struct Storage {
        exit: Exit,
        data: [u8; 4],
        position: usize,
        cancelled: Arc<AtomicBool>,
        failure: Option<ResourceError>,
    }
    impl Storage {
        fn fail(&mut self, kind: ResourceErrorKind) -> ResourceError {
            let error = ResourceError::new(kind, 37, 11);
            self.failure.get_or_insert(error);
            error
        }
    }
    impl Drop for Storage {
        fn drop(&mut self) {
            assert!(self.exit != Exit::Unwind, "intentional stage-drop unwind");
        }
    }
    impl Write for Storage {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.exit == Exit::WriteFailure {
                self.fail(ResourceErrorKind::Storage);
                return Err(std::io::ErrorKind::Other.into());
            }
            assert_eq!(bytes, b"data");
            self.data.copy_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl Read for Storage {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            if self.exit == Exit::ReadbackFailure {
                self.fail(ResourceErrorKind::Readback);
                return Err(std::io::ErrorKind::Other.into());
            }
            let len = bytes.len().min(self.data.len() - self.position);
            bytes[..len].copy_from_slice(&self.data[self.position..self.position + len]);
            self.position += len;
            Ok(len)
        }
    }
    impl StageStorage for Storage {
        fn failure(&self) -> Option<ResourceError> {
            self.failure
        }
        fn seal(&mut self) -> Result<u64, ResourceError> {
            if self.exit == Exit::SealFailure {
                return Err(self.fail(ResourceErrorKind::Storage));
            }
            if self.exit == Exit::CancelAfterSeal {
                self.cancelled.store(true, Ordering::SeqCst);
            }
            Ok(4)
        }
        fn complete(&mut self) -> Result<(), ResourceError> {
            if self.exit == Exit::CompletionFailure {
                return Err(self.fail(ResourceErrorKind::Storage));
            }
            Ok(())
        }
    }
    struct Destination {
        bytes: Cursor<[u8; 4]>,
        exit: Exit,
        cancelled: Arc<AtomicBool>,
    }
    impl Write for Destination {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.exit == Exit::DestinationFailure {
                return Err(std::io::ErrorKind::PermissionDenied.into());
            }
            let len = self.bytes.write(bytes)?;
            if self.exit == Exit::CancelAfterWrite {
                self.cancelled.store(true, Ordering::SeqCst);
            }
            Ok(len)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn stage_construction_refusal_releases_metadata_progress_and_intact_storage() {
        // Independent refusal points: metadata budget, progress budget,
        // progress allocation, then the concrete backend allocation.
        let layout = Layout::new::<StorageStage<Storage>>();
        for (limit, permitted_allocations, expected_kind, expected_allocations) in [
            (1, None, ResourceErrorKind::Budget, 0),
            (layout.size(), None, ResourceErrorKind::Budget, 0),
            (128 * 1024, Some(0), ResourceErrorKind::Allocation, 1),
            (128 * 1024, Some(1), ResourceErrorKind::Allocation, 2),
        ] {
            let provider = MemoryOnlyResources::new(NonZeroUsize::new(limit).unwrap());
            let scope = provider.resources().scope().unwrap();
            let storage = Storage {
                exit: Exit::EarlyDrop,
                data: [0; 4],
                position: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
                failure: None,
            };
            struct Reset;
            impl Drop for Reset {
                fn drop(&mut self) {
                    ALLOCATIONS_LEFT.with(|left| left.set(None));
                }
            }
            let reset = Reset;
            ALLOCATIONS_LEFT.with(|left| left.set(permitted_allocations));
            let (result, allocations) =
                allocation_probe(false, || StorageStage::create(scope, storage));
            drop(reset);
            assert!(
                matches!(result, Err(FormatError::Resource(error)) if error.kind == expected_kind)
            );
            assert_eq!(allocations, expected_allocations);
            assert_eq!(provider.used(), 0);
        }
    }

    #[test]
    fn stage_backing_stays_charged_through_every_actual_deallocation_boundary() {
        for exit in [
            Exit::EarlyDrop,
            Exit::SealedDrop,
            Exit::Delivered,
            Exit::WriteFailure,
            Exit::SealFailure,
            Exit::ReadbackFailure,
            Exit::CompletionFailure,
            Exit::DestinationFailure,
            Exit::CancelAfterSeal,
            Exit::CancelAfterWrite,
            Exit::Unwind,
        ] {
            let cancelled = Arc::new(AtomicBool::new(false));
            let authority = Arc::new(cancellation_harness::Authority {
                memory: MemoryOnlyResources::new(NonZeroUsize::new(128 * 1024).unwrap()),
                cancelled: cancelled.clone(),
                cancel_after_seal: false,
            });
            let scope = WriterResources::new(authority.clone()).scope().unwrap();
            let layout = Layout::new::<StorageStage<Storage>>();
            assert_ne!(layout.size(), PROGRESS_BYTES);
            let watch = Watch::new(&authority.memory, layout);
            let storage = Storage {
                exit,
                data: [0; 4],
                position: 0,
                cancelled: cancelled.clone(),
                failure: None,
            };
            let (stage, allocations) =
                allocation_probe(false, || StorageStage::create(scope, storage));
            assert_eq!(
                allocations, 2,
                "only progress and the backend backing allocate"
            );
            let mut stage = stage.unwrap();
            assert_eq!(authority.memory.used(), PROGRESS_BYTES + layout.size());
            watch.live(layout);
            if matches!(exit, Exit::EarlyDrop | Exit::Unwind) {
                let outcome =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(stage)));
                assert_eq!(outcome.is_err(), exit == Exit::Unwind);
                watch.released(layout);
                continue;
            }
            if exit == Exit::WriteFailure {
                assert!(stage.write_all(b"data").is_err());
                assert_eq!(
                    stage.failure(),
                    Some(ResourceError::new(ResourceErrorKind::Storage, 37, 11))
                );
                drop(stage);
                watch.released(layout);
                continue;
            }
            stage.write_all(b"data").unwrap();
            let (prepared, allocations) = allocation_probe(false, || stage.finish());
            assert_eq!(allocations, 0, "sealing reuses the identical backing");
            if exit == Exit::SealFailure {
                assert!(
                    matches!(prepared, Err(FormatError::Resource(error)) if error == ResourceError::new(ResourceErrorKind::Storage, 37, 11))
                );
                watch.released(layout);
                continue;
            }
            let prepared = prepared.unwrap();
            assert_eq!(prepared.len(), 4);
            watch.live(layout);
            assert_eq!(authority.memory.used(), PROGRESS_BYTES + layout.size());
            if exit == Exit::SealedDrop {
                drop(prepared);
            } else {
                let mut destination = Destination {
                    bytes: Cursor::new([0; 4]),
                    exit,
                    cancelled,
                };
                let result = prepared.deliver(&mut destination);
                match exit {
                    Exit::Delivered => {
                        result.unwrap();
                        assert_eq!(destination.bytes.into_inner(), *b"data");
                    }
                    Exit::ReadbackFailure | Exit::CompletionFailure => {
                        let kind = if exit == Exit::ReadbackFailure {
                            ResourceErrorKind::Readback
                        } else {
                            ResourceErrorKind::Storage
                        };
                        assert!(
                            matches!(result, Err(FormatError::Resource(error)) if error == ResourceError::new(kind, 37, 11))
                        );
                    }
                    Exit::CancelAfterSeal | Exit::CancelAfterWrite => {
                        assert!(
                            matches!(result, Err(FormatError::Resource(error)) if error.kind == ResourceErrorKind::Cancelled)
                        );
                        assert_eq!(
                            destination.bytes.position(),
                            if exit == Exit::CancelAfterSeal { 0 } else { 4 }
                        );
                    }
                    Exit::DestinationFailure => assert!(result.is_err()),
                    _ => unreachable!(),
                }
            }
            watch.released(layout);
        }
    }
}
