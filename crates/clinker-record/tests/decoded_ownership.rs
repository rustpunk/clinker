use clinker_record::FieldStr;
use clinker_record::owned_storage::{
    AllocationAuthority, AllocationLease, AllocationResources, AllocationScope, OwnerId,
    ResourceError, ResourceErrorKind, SharedStorage,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

struct FiniteAuthority {
    used: AtomicUsize,
    peak: AtomicUsize,
    cancelled: AtomicBool,
    limit: usize,
}
impl AllocationAuthority for FiniteAuthority {
    fn try_reserve(
        self: Arc<Self>,
        owner: OwnerId,
        layout: Layout,
    ) -> Result<AllocationLease, ResourceError> {
        self.check_cancelled()?;
        let old = self
            .used
            .fetch_update(SeqCst, SeqCst, |used| {
                used.checked_add(layout.size())
                    .filter(|next| *next <= self.limit)
            })
            .map_err(|used| {
                ResourceError::new(ResourceErrorKind::Budget, layout.size(), self.limit - used)
            })?;
        self.peak.fetch_max(old + layout.size(), SeqCst);
        AllocationLease::admitted(self, owner, layout.size())
    }
    fn release(&self, _: OwnerId, bytes: usize) {
        observe(|state| {
            if bytes != 0 {
                state.releases.fetch_add(1, SeqCst);
                state.release_at.store(state.tick(), SeqCst);
            }
        });
        self.used.fetch_sub(bytes, SeqCst);
    }
    fn check_cancelled(&self) -> Result<(), ResourceError> {
        if self.cancelled.load(SeqCst) {
            Err(ResourceError::new(ResourceErrorKind::Cancelled, 0, 0))
        } else {
            Ok(())
        }
    }
}
fn finite(limit: usize) -> (Arc<FiniteAuthority>, AllocationScope) {
    let authority = Arc::new(FiniteAuthority {
        used: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        cancelled: AtomicBool::new(false),
        limit,
    });
    let scope = AllocationResources::new(authority.clone()).scope().unwrap();
    (authority, scope)
}
const LONG: &str = "retained text survives its original row holder — café";

#[derive(Default)]
struct Block {
    pointer: AtomicUsize,
    size: AtomicUsize,
    align: AtomicUsize,
    admitted_at_allocation: AtomicUsize,
    admitted_at_deallocation: AtomicUsize,
    deallocated_size: AtomicUsize,
    deallocated_align: AtomicUsize,
    freed_at: AtomicUsize,
}
struct Observation {
    authority: Arc<FiniteAuthority>,
    blocks: [Block; 64],
    allocations: AtomicUsize,
    refuse_at: usize,
    releases: AtomicUsize,
    release_at: AtomicUsize,
    clock: AtomicUsize,
}
impl Observation {
    fn new(authority: &Arc<FiniteAuthority>, refuse_at: usize) -> Self {
        Self {
            authority: authority.clone(),
            blocks: std::array::from_fn(|_| Block::default()),
            allocations: AtomicUsize::new(0),
            refuse_at,
            releases: AtomicUsize::new(0),
            release_at: AtomicUsize::new(0),
            clock: AtomicUsize::new(0),
        }
    }
    fn tick(&self) -> usize {
        self.clock.fetch_add(1, SeqCst) + 1
    }
}
thread_local! {
    static OBSERVATION: Cell<*const Observation> = const { Cell::new(std::ptr::null()) };
}
struct Observe<'a>(&'a Observation);
impl<'a> Observe<'a> {
    fn start(state: &'a Observation) -> Self {
        OBSERVATION.with(|slot| assert!(slot.replace(state).is_null()));
        Self(state)
    }
}
impl Drop for Observe<'_> {
    fn drop(&mut self) {
        OBSERVATION.with(|slot| assert!(std::ptr::eq(slot.replace(std::ptr::null()), self.0)));
    }
}
fn observe(f: impl FnOnce(&Observation)) {
    let _ = OBSERVATION.try_with(|slot| {
        let pointer = slot.get();
        if !pointer.is_null() {
            // SAFETY: the thread's Observe guard borrows state until clearing
            // this pointer. Callbacks use only atomics and cannot allocate.
            f(unsafe { &*pointer });
        }
    });
}
struct Observer;
// SAFETY: System handles the original allocation layouts; observation only uses
// fixed atomic fields, and a refused allocation returns null to the caller.
unsafe impl GlobalAlloc for Observer {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let mut index = 0;
        let mut refuse = false;
        observe(|state| {
            index = state.allocations.fetch_add(1, SeqCst);
            refuse = index + 1 == state.refuse_at;
        });
        if refuse {
            return std::ptr::null_mut();
        }
        // SAFETY: caller supplies a valid nonzero GlobalAlloc layout.
        let pointer = unsafe { System.alloc(layout) };
        observe(|state| {
            if let Some(block) = state.blocks.get(index) {
                block.pointer.store(pointer as usize, SeqCst);
                block.size.store(layout.size(), SeqCst);
                block.align.store(layout.align(), SeqCst);
                block
                    .admitted_at_allocation
                    .store(state.authority.used.load(SeqCst), SeqCst);
            }
        });
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forward the original pointer and layout to its allocator.
        unsafe {
            System.dealloc(pointer, layout);
        }
        observe(|state| {
            for block in &state.blocks {
                if block.pointer.load(SeqCst) == pointer as usize
                    && block.freed_at.load(SeqCst) == 0
                {
                    block
                        .admitted_at_deallocation
                        .store(state.authority.used.load(SeqCst), SeqCst);
                    block.deallocated_size.store(layout.size(), SeqCst);
                    block.deallocated_align.store(layout.align(), SeqCst);
                    block.freed_at.store(state.tick(), SeqCst);
                }
            }
        });
    }
}
#[global_allocator]
static ALLOCATOR: Observer = Observer;

fn shared_metadata_layout<T>() -> Layout {
    let payload = Layout::new::<T>()
        .extend(Layout::new::<AllocationLease>())
        .unwrap()
        .0
        .pad_to_align();
    Layout::new::<AtomicUsize>()
        .extend(payload)
        .unwrap()
        .0
        .pad_to_align()
}

#[test]
fn shared_metadata_outer_admits_before_allocation_and_retains_final_alias() {
    let (authority, scope) = finite(4096);
    let schema = clinker_record::Schema::new(vec!["name".into()]);
    let expected = shared_metadata_layout::<clinker_record::Schema>();
    let observation = Observation::new(&authority, 0);
    {
        let _guard = Observe::start(&observation);
        let original = SharedStorage::try_new(schema, &scope).unwrap();
        let middle = original.clone();
        let last = middle.clone();
        drop(original);
        drop(middle);
        assert_eq!(last.column_name(0), Some("name"));
        assert_eq!(authority.used.load(SeqCst), expected.size());
        assert_eq!(observation.allocations.load(SeqCst), 1);
        assert_eq!(observation.blocks[0].freed_at.load(SeqCst), 0);
        drop(last);
    }
    let block = &observation.blocks[0];
    assert_eq!(block.size.load(SeqCst), expected.size());
    assert_eq!(block.align.load(SeqCst), expected.align());
    assert_eq!(block.admitted_at_allocation.load(SeqCst), expected.size());
    assert_eq!(block.admitted_at_deallocation.load(SeqCst), expected.size());
    assert!(block.freed_at.load(SeqCst) > 0);
    assert!(block.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
    assert_eq!(observation.releases.load(SeqCst), 1);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn shared_metadata_outer_distinguishes_budget_and_actual_allocator_refusal() {
    for allocator_failure in [false, true] {
        let layout = shared_metadata_layout::<clinker_record::Schema>();
        let limit = if allocator_failure {
            layout.size()
        } else {
            layout.size() - 1
        };
        let (authority, scope) = finite(limit);
        let schema = clinker_record::Schema::new(vec!["name".into()]);
        let observation = Observation::new(&authority, usize::from(allocator_failure));
        let result = {
            let _guard = Observe::start(&observation);
            SharedStorage::try_new(schema, &scope)
        };
        let error = result.unwrap_err();
        assert_eq!(
            error.kind,
            if allocator_failure {
                ResourceErrorKind::Allocation
            } else {
                ResourceErrorKind::Budget
            }
        );
        assert_eq!(error.requested, layout.size());
        assert_eq!(
            observation.allocations.load(SeqCst),
            usize::from(allocator_failure)
        );
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn shared_metadata_identity_and_clone_do_not_allocate() {
    let (authority, scope) = finite(4096);
    for legacy in [false, true] {
        let original = if legacy {
            SharedStorage::from_arc(Arc::new(7u64))
        } else {
            SharedStorage::try_new(7u64, &scope).unwrap()
        };
        let distinct = if legacy {
            SharedStorage::from_arc(Arc::new(7u64))
        } else {
            SharedStorage::try_new(7u64, &scope).unwrap()
        };
        let observation = Observation::new(&authority, 1);
        {
            let _guard = Observe::start(&observation);
            let alias = original.clone();
            assert!(SharedStorage::ptr_eq(&original, &alias));
            assert!(!SharedStorage::ptr_eq(&original, &distinct));
            assert_eq!(*original, *distinct);
            drop(alias);
        }
        assert_eq!(observation.allocations.load(SeqCst), 0);
        assert_eq!(format!("{original:?}"), "7");
    }
    let legacy = SharedStorage::from_arc(Arc::new(()));
    let governed = SharedStorage::try_new((), &scope).unwrap();
    let other = SharedStorage::try_new((), &scope).unwrap();
    assert!(!SharedStorage::ptr_eq(&legacy, &governed));
    assert!(!SharedStorage::ptr_eq(&governed, &other));
    assert!(SharedStorage::ptr_eq(&governed, &governed.clone()));
    drop((legacy, governed, other));
    assert_eq!(authority.used.load(SeqCst), 0);
}

fn assert_shared_metadata_legacy_wrap<T: std::fmt::Debug>(value: T) {
    let (authority, _) = finite(0);
    let arc = Arc::new(value);
    let borrowed_address = std::ptr::from_ref(&*arc);
    let retained = arc.clone();
    let observation = Observation::new(&authority, 1);
    let wrapped = {
        let _guard = Observe::start(&observation);
        SharedStorage::from_arc(arc)
    };
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert!(std::ptr::eq(borrowed_address, &*wrapped));
    assert_eq!(Arc::strong_count(&retained), 2);
    drop(wrapped);
    assert_eq!(Arc::strong_count(&retained), 1);
}

#[test]
fn shared_metadata_legacy_schema_and_document_keep_existing_allocation() {
    assert_shared_metadata_legacy_wrap(clinker_record::Schema::new(vec!["name".into()]));
    let document = clinker_record::DocumentContext::new(
        clinker_record::DocumentId::next(),
        Arc::from("input.csv"),
        clinker_record::EnvelopeRecord::empty(),
    );
    assert_shared_metadata_legacy_wrap(document);
}

fn assert_shared_outer_layout_matches_allocator<T: Copy>(payload: T) {
    let (authority, scope) = finite(4096);
    let (foreign, _) = finite(4096);
    let local_resources = AllocationResources::new(authority.clone());
    let foreign_resources = AllocationResources::new(foreign);
    for governed in [false, true] {
        let observation = Observation::new(&authority, 0);
        let (physical, legacy, local, other) = {
            let _guard = Observe::start(&observation);
            let value = if governed {
                SharedStorage::try_new(payload, &scope).unwrap()
            } else {
                SharedStorage::from_arc(Arc::new(payload))
            };
            let estimates = (
                value.estimated_outer_heap_size(),
                value.legacy_estimated_outer_heap_size(),
                value.unaccounted_outer_heap_size(&local_resources),
                value.unaccounted_outer_heap_size(&foreign_resources),
            );
            let alias = value.clone();
            drop(value);
            assert_eq!(observation.blocks[0].freed_at.load(SeqCst), 0);
            drop(alias);
            estimates
        };
        assert_eq!(observation.allocations.load(SeqCst), 1);
        let block = &observation.blocks[0];
        assert!(block.freed_at.load(SeqCst) > 0);
        let actual_size = block.deallocated_size.load(SeqCst);
        assert_eq!(block.size.load(SeqCst), actual_size);
        assert_eq!(
            block.align.load(SeqCst),
            block.deallocated_align.load(SeqCst)
        );
        assert_eq!(physical, actual_size, "outer estimate must include padding");
        assert_eq!(legacy, if governed { 0 } else { actual_size });
        assert_eq!(local, if governed { 0 } else { actual_size });
        assert_eq!(other, actual_size);
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn shared_outer_small_payload_matches_actual_deallocation_layout() {
    assert_shared_outer_layout_matches_allocator(7u8);
}

#[test]
fn shared_outer_overaligned_payload_matches_actual_deallocation_layout() {
    #[repr(align(64))]
    #[derive(Clone, Copy)]
    struct Aligned(u8);
    let value = Aligned(7);
    assert_eq!(value.0, 7);
    assert_shared_outer_layout_matches_allocator(value);
}

fn assert_shared_metadata_disposal(observation: &Observation, total: usize) {
    assert_eq!(observation.allocations.load(SeqCst), 3);
    let [text, child, outer, ..] = &observation.blocks;
    assert!(outer.freed_at.load(SeqCst) > 0);
    assert!(outer.freed_at.load(SeqCst) < child.freed_at.load(SeqCst));
    assert!(child.freed_at.load(SeqCst) < text.freed_at.load(SeqCst));
    assert!(text.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
    assert_eq!(observation.releases.load(SeqCst), 2);
    for block in [text, child, outer] {
        assert_eq!(block.admitted_at_deallocation.load(SeqCst), total);
    }
    assert_eq!(observation.authority.used.load(SeqCst), 0);
}

#[test]
fn shared_metadata_child_and_outer_survive_last_alias_and_concurrent_drop() {
    for concurrent in [false, true] {
        for _ in 0..32 {
            let (authority, scope) = finite(4096);
            let observation = Observation::new(&authority, 0);
            let original = {
                let _guard = Observe::start(&observation);
                SharedStorage::try_new(FieldStr::try_new(LONG, &scope).unwrap(), &scope).unwrap()
            };
            let total = authority.used.load(SeqCst);
            let alias = original.clone();
            drop(scope);
            if concurrent {
                let barrier = std::sync::Barrier::new(2);
                std::thread::scope(|threads| {
                    for handle in [original, alias] {
                        let observation = &observation;
                        let barrier = &barrier;
                        threads.spawn(move || {
                            barrier.wait();
                            let _guard = Observe::start(observation);
                            drop(handle);
                        });
                    }
                });
            } else {
                let _guard = Observe::start(&observation);
                drop(original);
                assert_eq!(alias.as_str(), LONG);
                assert_eq!(authority.used.load(SeqCst), total);
                assert_eq!(observation.blocks[2].freed_at.load(SeqCst), 0);
                drop(alias);
            }
            assert_shared_metadata_disposal(&observation, total);
        }
    }
}

#[test]
fn shared_metadata_constructor_failure_disposes_existing_governed_children() {
    let inner = LONG.len() + shared_metadata_layout::<Box<str>>().size();
    for kind in [
        ResourceErrorKind::Budget,
        ResourceErrorKind::Allocation,
        ResourceErrorKind::Cancelled,
    ] {
        let (authority, scope) = finite(if kind == ResourceErrorKind::Budget {
            inner
        } else {
            4096
        });
        let observation = Observation::new(
            &authority,
            if kind == ResourceErrorKind::Allocation {
                3
            } else {
                0
            },
        );
        let result = {
            let _guard = Observe::start(&observation);
            let value = FieldStr::try_new(LONG, &scope).unwrap();
            assert_eq!(authority.used.load(SeqCst), inner);
            if kind == ResourceErrorKind::Cancelled {
                authority.cancelled.store(true, SeqCst);
            }
            SharedStorage::try_new(value, &scope)
        };
        assert_eq!(result.unwrap_err().kind, kind);
        assert_eq!(
            observation.allocations.load(SeqCst),
            if kind == ResourceErrorKind::Allocation {
                3
            } else {
                2
            }
        );
        assert_eq!(observation.blocks[2].pointer.load(SeqCst), 0);
        assert!(observation.blocks[1].freed_at.load(SeqCst) > 0);
        assert!(
            observation.blocks[1].freed_at.load(SeqCst)
                < observation.blocks[0].freed_at.load(SeqCst)
        );
        assert!(observation.blocks[0].freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn shared_metadata_preserves_reservation_error_evidence() {
    struct Reject(ResourceError);
    impl AllocationAuthority for Reject {
        fn try_reserve(
            self: Arc<Self>,
            _: OwnerId,
            _: Layout,
        ) -> Result<AllocationLease, ResourceError> {
            Err(self.0)
        }
        fn release(&self, _: OwnerId, _: usize) {
            panic!("no reservation was admitted")
        }
        fn check_cancelled(&self) -> Result<(), ResourceError> {
            Ok(())
        }
    }
    for kind in [
        ResourceErrorKind::Budget,
        ResourceErrorKind::Cancelled,
        ResourceErrorKind::Finalized,
        ResourceErrorKind::Authority,
    ] {
        let error = ResourceError {
            kind,
            requested: 91,
            available: 17,
            field: Some(2),
            offset: Some(4),
        };
        let scope = AllocationResources::new(Arc::new(Reject(error)))
            .scope()
            .unwrap();
        assert_eq!(SharedStorage::try_new(7u64, &scope).unwrap_err(), error);
    }
}

#[test]
fn shared_metadata_aligned_empty_and_handle_layouts() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SharedStorage<clinker_record::Schema>>();
    assert_send_sync::<SharedStorage<clinker_record::DocumentContext>>();
    #[repr(align(256))]
    #[derive(Debug)]
    struct Aligned(u8);
    fn check<T: std::fmt::Debug>(value: T) {
        let (authority, scope) = finite(4096);
        let expected = shared_metadata_layout::<T>();
        let observation = Observation::new(&authority, 0);
        {
            let _guard = Observe::start(&observation);
            let value = SharedStorage::try_new(value, &scope).unwrap();
            assert_eq!(
                std::ptr::from_ref(&*value) as usize % std::mem::align_of::<T>(),
                0
            );
            drop(value);
        }
        assert_eq!(observation.allocations.load(SeqCst), 1);
        assert_eq!(observation.blocks[0].size.load(SeqCst), expected.size());
        assert_eq!(observation.blocks[0].align.load(SeqCst), expected.align());
        assert_eq!(authority.used.load(SeqCst), 0);
    }
    let aligned = Aligned(7);
    assert_eq!(aligned.0, 7);
    check(aligned);
    check(());
    for (size, optional, alignment) in [
        (
            std::mem::size_of::<SharedStorage<clinker_record::Schema>>(),
            std::mem::size_of::<Option<SharedStorage<clinker_record::Schema>>>(),
            std::mem::align_of::<SharedStorage<clinker_record::Schema>>(),
        ),
        (
            std::mem::size_of::<SharedStorage<clinker_record::DocumentContext>>(),
            std::mem::size_of::<Option<SharedStorage<clinker_record::DocumentContext>>>(),
            std::mem::align_of::<SharedStorage<clinker_record::DocumentContext>>(),
        ),
    ] {
        assert_eq!(size, 2 * std::mem::size_of::<usize>());
        assert_eq!(optional, size);
        assert_eq!(alignment, std::mem::align_of::<usize>());
    }
}

#[test]
fn shared_metadata_payload_panic_still_destroys_children_then_outer_lease() {
    struct Panics(FieldStr);
    impl Drop for Panics {
        fn drop(&mut self) {
            assert_eq!(self.0.as_str(), LONG);
            panic!("injected metadata destructor panic");
        }
    }
    let (authority, scope) = finite(4096);
    let observation = Observation::new(&authority, 0);
    let original = {
        let _guard = Observe::start(&observation);
        SharedStorage::try_new(Panics(FieldStr::try_new(LONG, &scope).unwrap()), &scope).unwrap()
    };
    let total = authority.used.load(SeqCst);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = Observe::start(&observation);
        drop(original);
    }));
    assert!(result.is_err());
    // Panic machinery may allocate after the three tracked storage blocks; the
    // bounded observer preserves their first completed deallocation events.
    let [text, child, outer, ..] = &observation.blocks;
    assert!(outer.freed_at.load(SeqCst) > 0);
    assert!(outer.freed_at.load(SeqCst) < child.freed_at.load(SeqCst));
    assert!(child.freed_at.load(SeqCst) < text.freed_at.load(SeqCst));
    assert!(text.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
    assert_eq!(observation.releases.load(SeqCst), 2);
    for block in [text, child, outer] {
        assert_eq!(block.admitted_at_deallocation.load(SeqCst), total);
    }
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn field_str_shared_alias_keeps_original_charge() {
    let (authority, scope) = finite(4096);
    let original = FieldStr::try_new(LONG, &scope).unwrap();
    let charged = authority.used.load(SeqCst);
    assert!(charged > LONG.len());
    let alias = original.clone();
    assert_eq!(original.as_ptr(), alias.as_ptr());
    drop(original);
    drop(scope);
    assert_eq!(alias.as_str(), LONG);
    assert_eq!(authority.used.load(SeqCst), charged);
    drop(alias);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn field_str_unique_copy_has_distinct_legacy_storage() {
    let (authority, scope) = finite(4096);
    let original = FieldStr::try_new_unique(LONG, &scope).unwrap();
    let copied = original.clone();
    assert_ne!(original.as_ptr(), copied.as_ptr());
    assert_eq!(original.legacy_heap_size(), 0);
    assert_eq!(copied.legacy_heap_size(), LONG.len());
    drop(original);
    assert_eq!(authority.used.load(SeqCst), 0);
    assert_eq!(copied.as_str(), LONG);
}

#[test]
fn field_str_inline_and_refusal_leave_no_charge() {
    let (authority, scope) = finite(0);
    for text in ["", "short", "ééé"] {
        assert_eq!(FieldStr::try_new(text, &scope).unwrap().as_str(), text);
        assert_eq!(
            FieldStr::try_new_unique(text, &scope).unwrap().as_str(),
            text
        );
    }
    assert_eq!(
        FieldStr::try_new(LONG, &scope).unwrap_err().kind,
        ResourceErrorKind::Budget
    );
    assert_eq!(authority.used.load(SeqCst), 0);
    authority.cancelled.store(true, SeqCst);
    assert_eq!(
        FieldStr::try_new("", &scope).unwrap_err().kind,
        ResourceErrorKind::Cancelled
    );
}

#[test]
fn field_str_actual_layout_and_release_order() {
    for unique in [false, true] {
        let (authority, scope) = finite(4096);
        let observation = Observation::new(&authority, 0);
        let charged;
        {
            let _guard = Observe::start(&observation);
            let text = if unique {
                FieldStr::try_new_unique(LONG, &scope)
            } else {
                FieldStr::try_new(LONG, &scope)
            }
            .unwrap();
            charged = authority.used.load(SeqCst);
            assert_eq!(text.heap_size(), charged);
            assert_eq!(
                text.as_ptr() as usize,
                observation.blocks[0].pointer.load(SeqCst)
            );
            drop(text);
        }
        assert_eq!(observation.allocations.load(SeqCst), 2);
        let [bytes, holder, ..] = &observation.blocks;
        assert_eq!(bytes.size.load(SeqCst), LONG.len());
        assert_eq!(bytes.align.load(SeqCst), 1);
        assert_eq!(bytes.size.load(SeqCst) + holder.size.load(SeqCst), charged);
        // OwnedText is Box<str> followed by the actual inline lease. The sized
        // shared holder additionally contains the library atomic count.
        let payload = Layout::new::<Box<str>>()
            .extend(Layout::new::<AllocationLease>())
            .unwrap()
            .0
            .pad_to_align();
        let expected = if unique {
            payload
        } else {
            Layout::new::<AtomicUsize>()
                .extend(payload)
                .unwrap()
                .0
                .pad_to_align()
        };
        assert_eq!(holder.size.load(SeqCst), expected.size());
        assert_eq!(holder.align.load(SeqCst), expected.align());
        for block in [bytes, holder] {
            assert_eq!(block.admitted_at_allocation.load(SeqCst), charged);
            assert_eq!(block.admitted_at_deallocation.load(SeqCst), charged);
        }
        assert!(holder.freed_at.load(SeqCst) > 0);
        assert!(holder.freed_at.load(SeqCst) < bytes.freed_at.load(SeqCst));
        assert!(bytes.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
        assert_eq!(observation.releases.load(SeqCst), 1);
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn field_str_actual_null_at_each_allocation_releases_after_owned_bytes() {
    for unique in [false, true] {
        for refuse_at in [1, 2] {
            let (authority, scope) = finite(4096);
            let observation = Observation::new(&authority, refuse_at);
            let error;
            {
                let _guard = Observe::start(&observation);
                error = if unique {
                    FieldStr::try_new_unique(LONG, &scope)
                } else {
                    FieldStr::try_new(LONG, &scope)
                }
                .unwrap_err();
            }
            assert_eq!(error.kind, ResourceErrorKind::Allocation);
            assert_eq!(observation.allocations.load(SeqCst), refuse_at);
            assert_eq!(observation.releases.load(SeqCst), 1);
            assert_eq!(authority.used.load(SeqCst), 0);
            if refuse_at == 2 {
                let text = &observation.blocks[0];
                assert!(text.admitted_at_deallocation.load(SeqCst) > LONG.len());
                assert!(text.freed_at.load(SeqCst) > 0);
                assert!(text.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
            }
        }
    }
}

#[test]
fn field_str_inline_legacy_and_shared_clone_allocation_shapes() {
    let (authority, scope) = finite(4096);
    for text in ["", "short", "12345678901234567890123"] {
        let observation = Observation::new(&authority, 1);
        {
            let _guard = Observe::start(&observation);
            drop(FieldStr::new(text));
            drop(FieldStr::try_new(text, &scope).unwrap());
            drop(FieldStr::try_new_unique(text, &scope).unwrap());
        }
        assert_eq!(observation.allocations.load(SeqCst), 0);
        assert_eq!(authority.used.load(SeqCst), 0);
    }
    for unique in [false, true] {
        for text in ["", "short", LONG] {
            let observation = Observation::new(&authority, 0);
            {
                let _guard = Observe::start(&observation);
                let original = if unique {
                    FieldStr::new_unique(text)
                } else {
                    FieldStr::new(text)
                };
                drop(original.clone());
                drop(original);
            }
            let expected = if unique && !text.is_empty() {
                2
            } else {
                usize::from(text.len() > 23)
            };
            assert_eq!(observation.allocations.load(SeqCst), expected);
        }
    }
    let original = FieldStr::try_new(LONG, &scope).unwrap();
    let observation = Observation::new(&authority, 1);
    {
        let _guard = Observe::start(&observation);
        let alias = original.clone();
        drop(original);
        assert_eq!(alias.as_str(), LONG);
        drop(alias);
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn field_str_conversion_retains_old_and_new_allocations() {
    let (authority, scope) = finite(4096);
    let original = FieldStr::try_new(LONG, &scope).unwrap();
    let old_charge = authority.used.load(SeqCst);
    let copy = FieldStr::try_new(original.as_str(), &scope).unwrap();
    assert_ne!(copy.as_ptr(), original.as_ptr());
    assert_eq!(authority.peak.load(SeqCst), old_charge * 2);
    drop(original);
    assert_eq!(authority.used.load(SeqCst), old_charge);
    assert_eq!(copy.as_str(), LONG);
    drop(copy);
    assert_eq!(authority.used.load(SeqCst), 0);

    let (authority, scope) = finite(old_charge * 2 - 1);
    let original = FieldStr::try_new(LONG, &scope).unwrap();
    let observation = Observation::new(&authority, 1);
    {
        let _guard = Observe::start(&observation);
        assert_eq!(
            FieldStr::try_new(original.as_str(), &scope)
                .unwrap_err()
                .kind,
            ResourceErrorKind::Budget
        );
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert_eq!(authority.used.load(SeqCst), old_charge);
    assert_eq!(original.as_str(), LONG);
    drop(original);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn field_str_nested_aliases_and_drop_permutations() {
    use clinker_record::Value;
    for dropped_first in 0..3 {
        let (authority, scope) = finite(4096);
        let string = FieldStr::try_new(LONG, &scope).unwrap();
        let original_pointer = string.as_ptr();
        let nested = Value::Array(clinker_record::owned_storage::OwnedValues::from_vec(vec![
            Value::String(string),
        ]));
        let cloned = nested.clone();
        let Value::Array(values) = &cloned else {
            unreachable!()
        };
        let Value::String(detached) = &values[0] else {
            unreachable!()
        };
        let detached = detached.clone();
        assert_eq!(detached.as_ptr(), original_pointer);
        let mut aliases = [Some(nested), Some(cloned), Some(Value::String(detached))];
        let charge = authority.used.load(SeqCst);
        drop(aliases[dropped_first].take());
        assert_eq!(authority.used.load(SeqCst), charge);
        for alias in &mut aliases {
            drop(alias.take());
        }
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn field_str_final_two_threads_release_once_after_deallocation() {
    for _ in 0..32 {
        let (authority, scope) = finite(4096);
        let observation = Observation::new(&authority, 0);
        let original = {
            let _guard = Observe::start(&observation);
            FieldStr::try_new(LONG, &scope).unwrap()
        };
        let alias = original.clone();
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for text in [original, alias] {
                let observation = &observation;
                let barrier = &barrier;
                threads.spawn(move || {
                    barrier.wait();
                    let _guard = Observe::start(observation);
                    drop(text);
                });
            }
        });
        assert_eq!(observation.allocations.load(SeqCst), 2);
        assert_eq!(observation.releases.load(SeqCst), 1);
        let [text, holder, ..] = &observation.blocks;
        assert!(holder.freed_at.load(SeqCst) > 0);
        assert!(holder.freed_at.load(SeqCst) < text.freed_at.load(SeqCst));
        assert!(text.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn field_str_wire_hash_order_and_compiled_layout_match_legacy() {
    use clinker_record::Value;
    use std::hash::{Hash, Hasher};
    fn hash(text: &FieldStr) -> u64 {
        let mut state = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut state);
        state.finish()
    }
    assert_eq!(std::mem::size_of::<FieldStr>(), 24);
    assert_eq!(std::mem::size_of::<Value>(), 32);
    assert_eq!(
        std::mem::align_of::<FieldStr>(),
        std::mem::align_of::<usize>()
    );
    let (authority, scope) = finite(4096);
    for text in ["", "short", LONG, "雪雪雪雪雪雪雪雪雪雪"] {
        let strings = [
            FieldStr::new(text),
            FieldStr::new_unique(text),
            FieldStr::try_new(text, &scope).unwrap(),
            FieldStr::try_new_unique(text, &scope).unwrap(),
        ];
        for candidate in &strings {
            assert_eq!(candidate, &strings[0]);
            assert_eq!(candidate.cmp(&strings[0]), std::cmp::Ordering::Equal);
            assert_eq!(hash(candidate), hash(&strings[0]));
            let value = Value::String(candidate.clone());
            let legacy = Value::String(strings[0].clone());
            assert_eq!(
                std::mem::discriminant(&value),
                std::mem::discriminant(&legacy)
            );
            assert_eq!(
                clinker_record::value_to_group_key(&value, "text", 0).unwrap(),
                clinker_record::value_to_group_key(&legacy, "text", 0).unwrap()
            );
            assert_eq!(
                serde_json::to_vec(&value).unwrap(),
                serde_json::to_vec(&legacy).unwrap()
            );
            let encoded = postcard::to_allocvec(&value).unwrap();
            assert_eq!(encoded, postcard::to_allocvec(&legacy).unwrap());
            assert_eq!(postcard::from_bytes::<Value>(&encoded).unwrap(), value);
            let mut map = std::collections::HashMap::new();
            map.insert(candidate.clone(), 7);
            assert_eq!(map.get(text), Some(&7));
        }
    }
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_containers_iterator_keeps_backing_and_detached_leaf() {
    use clinker_record::Value;
    use clinker_record::owned_storage::OwnedValues;
    let (authority, scope) = finite(8192);
    let text = FieldStr::try_new(LONG, &scope).unwrap();
    let text_charge = authority.used.load(SeqCst);
    let mut values = OwnedValues::try_with_capacity(2, &scope).unwrap();
    values.try_push(Value::String(text), &scope).unwrap();
    values.try_push(Value::Integer(7), &scope).unwrap();
    let total = authority.used.load(SeqCst);
    assert!(total > text_charge);
    let mut iter = values.into_iter();
    let detached = iter.next().unwrap();
    assert_eq!(authority.used.load(SeqCst), total);
    drop(iter);
    assert_eq!(authority.used.load(SeqCst), text_charge);
    assert_eq!(detached, Value::String(FieldStr::new(LONG)));
    drop(detached);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_containers_map_key_and_value_escape_independently() {
    use clinker_record::Value;
    use clinker_record::owned_storage::{OwnedKey, OwnedMap};
    let (authority, scope) = finite(8192);
    let key = OwnedKey::try_new("key", &scope).unwrap();
    let key_charge = authority.used.load(SeqCst);
    let text = FieldStr::try_new(LONG, &scope).unwrap();
    let children = authority.used.load(SeqCst);
    let mut map = OwnedMap::try_with_capacity(1, &scope).unwrap();
    map.try_insert(key, Value::String(text), &scope).unwrap();
    let total = authority.used.load(SeqCst);
    let mut iter = map.into_iter();
    let (key, value) = iter.next().unwrap();
    assert_eq!(authority.used.load(SeqCst), total);
    drop(iter);
    assert_eq!(authority.used.load(SeqCst), children);
    drop(value);
    assert_eq!(authority.used.load(SeqCst), key_charge);
    assert_eq!(key.as_str(), "key");
    drop(key);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_containers_map_bound_covers_actual_small_and_power_transitions() {
    use clinker_record::owned_storage::OwnedMap;
    for capacity in (0..=16).chain([
        27, 28, 29, 55, 56, 57, 111, 112, 113, 127, 128, 129, 255, 256, 257,
    ]) {
        let (authority, scope) = finite(1024 * 1024);
        let observation = Observation::new(&authority, 0);
        let charged;
        {
            let _guard = Observe::start(&observation);
            let map = OwnedMap::try_with_capacity(capacity, &scope).unwrap();
            assert_eq!(map.capacity(), capacity);
            assert!(map.as_map().capacity() >= capacity);
            charged = authority.used.load(SeqCst);
            assert_eq!(map.heap_size(), charged);
            assert_eq!(map.legacy_heap_size(), 0);
            drop(map);
        }
        let allocations = observation.allocations.load(SeqCst);
        assert_eq!(allocations, if capacity == 0 { 1 } else { 3 });
        let blocks = &observation.blocks[..allocations];
        let actual = blocks
            .iter()
            .map(|block| block.size.load(SeqCst))
            .sum::<usize>();
        assert!(
            actual <= charged,
            "capacity {capacity}: allocated {actual}, admitted {charged}"
        );
        for block in blocks {
            assert_eq!(block.admitted_at_allocation.load(SeqCst), charged);
            assert_eq!(block.admitted_at_deallocation.load(SeqCst), charged);
            assert!(block.freed_at.load(SeqCst) > 0);
            assert!(block.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
        }
        if capacity != 0 {
            let buckets: usize = match capacity {
                1..=3 => 4,
                4..=7 => 8,
                8..=14 => 16,
                n => (8 * n / 7).next_power_of_two(),
            };
            let control_alignment = std::mem::align_of::<usize>().max(16);
            let aligned_indices = (buckets * std::mem::size_of::<usize>() + control_alignment - 1)
                & !(control_alignment - 1);
            assert!(blocks[0].size.load(SeqCst) <= aligned_indices + buckets + 16);
            let alignment = std::mem::align_of::<usize>()
                .max(std::mem::align_of::<clinker_record::owned_storage::OwnedKey>())
                .max(std::mem::align_of::<clinker_record::Value>());
            let entry_bound = std::mem::size_of::<usize>()
                + std::mem::size_of::<clinker_record::owned_storage::OwnedKey>()
                + std::mem::size_of::<clinker_record::Value>()
                + 3 * (alignment - 1);
            let entry_bound = (entry_bound + alignment - 1) & !(alignment - 1);
            assert!(blocks[1].size.load(SeqCst) <= capacity * entry_bound);
            let holder_freed = blocks[2].freed_at.load(SeqCst);
            assert!(holder_freed < blocks[0].freed_at.load(SeqCst));
            assert!(holder_freed < blocks[1].freed_at.load(SeqCst));
        }
        assert_eq!(observation.releases.load(SeqCst), 1);
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn owned_containers_map_partial_null_failures_release_after_allocated_blocks() {
    use clinker_record::owned_storage::OwnedMap;
    for refuse_at in 1..=3 {
        let (authority, scope) = finite(8192);
        let observation = Observation::new(&authority, refuse_at);
        let error;
        {
            let _guard = Observe::start(&observation);
            error = OwnedMap::try_with_capacity(7, &scope).unwrap_err();
        }
        assert_eq!(error.kind, ResourceErrorKind::Allocation);
        assert_eq!(observation.allocations.load(SeqCst), refuse_at);
        for block in &observation.blocks[..refuse_at - 1] {
            assert!(block.admitted_at_deallocation.load(SeqCst) >= block.size.load(SeqCst));
            assert!(block.freed_at.load(SeqCst) > 0);
            assert!(block.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
        }
        assert_eq!(observation.releases.load(SeqCst), 1);
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn owned_containers_vector_and_key_nulls_keep_charge_until_deallocation() {
    use clinker_record::owned_storage::{OwnedKey, OwnedValues};
    for key in [false, true] {
        for refuse_at in 1..=2 {
            let (authority, scope) = finite(8192);
            let observation = Observation::new(&authority, refuse_at);
            let error;
            {
                let _guard = Observe::start(&observation);
                error = if key {
                    OwnedKey::try_new("key", &scope).unwrap_err()
                } else {
                    OwnedValues::try_with_capacity(4, &scope).unwrap_err()
                };
            }
            assert_eq!(error.kind, ResourceErrorKind::Allocation);
            assert_eq!(observation.allocations.load(SeqCst), refuse_at);
            if refuse_at == 2 {
                let block = &observation.blocks[0];
                assert!(block.admitted_at_deallocation.load(SeqCst) >= block.size.load(SeqCst));
                assert!(block.freed_at.load(SeqCst) > 0);
                assert!(block.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
            }
            assert_eq!(authority.used.load(SeqCst), 0);
        }
    }
}

#[test]
fn owned_containers_legacy_shape_move_and_clone() {
    use clinker_record::Value;
    use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues};
    assert_eq!(
        std::mem::size_of::<OwnedValues>(),
        std::mem::size_of::<Vec<Value>>()
    );
    assert_eq!(
        std::mem::align_of::<OwnedValues>(),
        std::mem::align_of::<Vec<Value>>()
    );
    assert_eq!(
        std::mem::size_of::<OwnedKey>(),
        std::mem::size_of::<Box<str>>()
    );
    assert!(std::mem::size_of::<OwnedMap>() <= std::mem::size_of::<OwnedValues>());
    assert_eq!(
        std::mem::align_of::<OwnedMap>(),
        std::mem::align_of::<usize>()
    );
    let (authority, scope) = finite(8192);
    let observation = Observation::new(&authority, 1);
    {
        let _guard = Observe::start(&observation);
        drop(OwnedValues::from_vec(Vec::new()).clone());
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    let values = vec![Value::Integer(1), Value::Integer(2)];
    let pointer = values.as_ptr();
    let observation = Observation::new(&authority, 1);
    let moved = {
        let _guard = Observe::start(&observation);
        OwnedValues::from_vec(values)
    };
    assert_eq!(moved.as_slice().as_ptr(), pointer);
    assert_eq!(observation.allocations.load(SeqCst), 0);

    let mut map = indexmap::IndexMap::new();
    map.insert(
        OwnedKey::from("a"),
        Value::String(FieldStr::try_new(LONG, &scope).unwrap()),
    );
    let map = OwnedMap::from_map(map);
    let observation = Observation::new(&authority, 0);
    let copy = {
        let _guard = Observe::start(&observation);
        map.clone()
    };
    // One independent key, index table, entry vector and exactly one map Box.
    assert_eq!(observation.allocations.load(SeqCst), 4);
    let (key, value) = map.as_map().get_index(0).unwrap();
    let (copied_key, copied_value) = copy.as_map().get_index(0).unwrap();
    assert_ne!(key.as_ptr(), copied_key.as_ptr());
    let (Value::String(original), Value::String(copied)) = (value, copied_value) else {
        unreachable!()
    };
    assert_eq!(original.as_ptr(), copied.as_ptr());
    drop(map);
    assert!(authority.used.load(SeqCst) > 0);
    drop(copy);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_containers_growth_preserves_order_and_old_new_overlap() {
    use clinker_record::Value;
    use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues};
    let (authority, scope) = finite(16384);
    let mut values = OwnedValues::try_with_capacity(1, &scope).unwrap();
    values.try_push(Value::Integer(1), &scope).unwrap();
    let old = authority.used.load(SeqCst);
    values.try_push(Value::Integer(2), &scope).unwrap();
    assert_eq!(values.capacity(), 2);
    let new = authority.used.load(SeqCst);
    assert_eq!(authority.peak.load(SeqCst), old + new);
    assert_eq!(values.as_slice(), &[Value::Integer(1), Value::Integer(2)]);
    drop(values);
    assert_eq!(authority.used.load(SeqCst), 0);

    let (authority, scope) = finite(16384);
    let mut map = OwnedMap::try_with_capacity(1, &scope).unwrap();
    map.try_insert(OwnedKey::from("a"), Value::Integer(1), &scope)
        .unwrap();
    let old = authority.used.load(SeqCst);
    map.try_insert(OwnedKey::from("b"), Value::Integer(2), &scope)
        .unwrap();
    let new = authority.used.load(SeqCst);
    assert_eq!(authority.peak.load(SeqCst), old + new);
    assert_eq!(map.capacity(), 2);
    assert_eq!(map.as_map().get_index(0).unwrap().0.as_str(), "a");
    assert_eq!(map.as_map().get_index(1).unwrap().0.as_str(), "b");
    let key = OwnedKey::from("a");
    let observation = Observation::new(&authority, 1);
    {
        let _guard = Observe::start(&observation);
        assert_eq!(
            map.try_insert(key, Value::Integer(3), &scope).unwrap(),
            Some(Value::Integer(1))
        );
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert_eq!(map.len(), 2);
    assert_eq!(authority.used.load(SeqCst), new);
    drop(map);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_containers_failed_growth_returns_elements_and_preserves_source() {
    use clinker_record::Value;
    use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues};
    for refuse_at in 1..=3 {
        let (authority, scope) = finite(16384);
        let mut map = OwnedMap::try_with_capacity(1, &scope).unwrap();
        map.try_insert(OwnedKey::from("old"), Value::Integer(1), &scope)
            .unwrap();
        let key = OwnedKey::try_new("new", &scope).unwrap();
        let value = Value::String(FieldStr::try_new(LONG, &scope).unwrap());
        let charge = authority.used.load(SeqCst);
        let observation = Observation::new(&authority, refuse_at);
        let (error, key, value) = {
            let _guard = Observe::start(&observation);
            map.try_insert(key, value, &scope).unwrap_err()
        };
        assert_eq!(error.kind, ResourceErrorKind::Allocation);
        assert_eq!(key.as_str(), "new");
        assert_eq!(value, Value::String(FieldStr::new(LONG)));
        assert_eq!(map.len(), 1);
        assert_eq!(map.capacity(), 1);
        assert_eq!(authority.used.load(SeqCst), charge);
        drop((map, key, value));
        assert_eq!(authority.used.load(SeqCst), 0);
    }
    for refuse_at in 1..=2 {
        let (authority, scope) = finite(16384);
        let mut values = OwnedValues::try_with_capacity(1, &scope).unwrap();
        values.try_push(Value::Integer(1), &scope).unwrap();
        let value = Value::String(FieldStr::try_new(LONG, &scope).unwrap());
        let charge = authority.used.load(SeqCst);
        let observation = Observation::new(&authority, refuse_at);
        let (error, value) = {
            let _guard = Observe::start(&observation);
            values.try_push(value, &scope).unwrap_err()
        };
        assert_eq!(error.kind, ResourceErrorKind::Allocation);
        assert_eq!(values.as_slice(), &[Value::Integer(1)]);
        assert_eq!(authority.used.load(SeqCst), charge);
        drop((values, value));
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn owned_containers_overflow_and_budget_refuse_before_allocation() {
    use clinker_record::owned_storage::{OwnedMap, OwnedValues};
    let (authority, scope) = finite(0);
    let observation = Observation::new(&authority, 1);
    {
        let _guard = Observe::start(&observation);
        for capacity in [usize::MAX, usize::MAX / 2, isize::MAX as usize] {
            assert_eq!(
                OwnedMap::try_with_capacity(capacity, &scope)
                    .unwrap_err()
                    .kind,
                ResourceErrorKind::Layout
            );
            assert_eq!(
                OwnedValues::try_with_capacity(capacity, &scope)
                    .unwrap_err()
                    .kind,
                ResourceErrorKind::Layout
            );
        }
        assert_eq!(
            OwnedMap::try_with_capacity(1, &scope).unwrap_err().kind,
            ResourceErrorKind::Budget
        );
        assert_eq!(
            OwnedValues::try_with_capacity(1, &scope).unwrap_err().kind,
            ResourceErrorKind::Budget
        );
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_containers_recursive_legacy_contribution_and_clone() {
    use clinker_record::Value;
    use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues};
    let (authority, scope) = finite(16384);
    let text = FieldStr::try_new(LONG, &scope).unwrap();
    let text_charge = authority.used.load(SeqCst);
    let nested = Value::Array(clinker_record::owned_storage::OwnedValues::from_vec(vec![
        Value::String(text),
    ]));
    assert_eq!(nested.legacy_heap_size(), std::mem::size_of::<Value>());
    let mut values = OwnedValues::try_with_capacity(2, &scope).unwrap();
    values.try_push(nested, &scope).unwrap();
    values
        .try_push(Value::String(FieldStr::new_unique(LONG)), &scope)
        .unwrap();
    let legacy = std::mem::size_of::<Value>() + LONG.len();
    assert_eq!(values.legacy_heap_size(), legacy);
    let copy = values.clone();
    assert_ne!(values.as_slice().as_ptr(), copy.as_slice().as_ptr());
    assert_eq!(
        copy.legacy_heap_size(),
        legacy + 2 * std::mem::size_of::<Value>()
    );
    drop(values);
    assert_eq!(authority.used.load(SeqCst), text_charge);
    drop(copy);
    assert_eq!(authority.used.load(SeqCst), 0);

    let mut map = OwnedMap::try_with_capacity(1, &scope).unwrap();
    let key = OwnedKey::try_new("key", &scope).unwrap();
    let value = FieldStr::try_new(LONG, &scope).unwrap();
    map.try_insert(key, Value::String(value), &scope).unwrap();
    assert_eq!(map.legacy_heap_size(), 0);
    let copy = map.clone();
    assert!(copy.legacy_heap_size() > "key".len());
    drop(map);
    assert_eq!(authority.used.load(SeqCst), text_charge);
    drop(copy);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_containers_iterator_unwind_destroys_remaining_owned_storage() {
    use clinker_record::Value;
    use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues};
    let (authority, scope) = finite(16384);
    let mut values = OwnedValues::try_with_capacity(2, &scope).unwrap();
    values
        .try_push(
            Value::String(FieldStr::try_new(LONG, &scope).unwrap()),
            &scope,
        )
        .unwrap();
    values.try_push(Value::Integer(7), &scope).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let mut iter = values.into_iter();
        assert_eq!(iter.next_back(), Some(Value::Integer(7)));
        panic!("consumer unwinds with a live iterator");
    }));
    assert!(result.is_err());
    assert_eq!(authority.used.load(SeqCst), 0);
    let mut map = OwnedMap::try_with_capacity(2, &scope).unwrap();
    for key in ["a", "b"] {
        map.try_insert(
            OwnedKey::try_new(key, &scope).unwrap(),
            Value::String(FieldStr::try_new(LONG, &scope).unwrap()),
            &scope,
        )
        .unwrap();
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let mut iter = map.into_iter();
        drop(iter.next());
        panic!("consumer unwinds with remaining map entries");
    }));
    assert!(result.is_err());
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_containers_iterator_physical_backing_precedes_release() {
    use clinker_record::Value;
    use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues};
    let (authority, scope) = finite(16384);
    let observation = Observation::new(&authority, 0);
    {
        let _guard = Observe::start(&observation);
        let mut values = OwnedValues::try_with_capacity(2, &scope).unwrap();
        values.try_push(Value::Integer(1), &scope).unwrap();
        values.try_push(Value::Integer(2), &scope).unwrap();
        let charge = authority.used.load(SeqCst);
        let mut iter = values.into_iter();
        assert!(observation.blocks[1].freed_at.load(SeqCst) > 0);
        assert_eq!(observation.blocks[0].freed_at.load(SeqCst), 0);
        assert_eq!(iter.next(), Some(Value::Integer(1)));
        assert_eq!(authority.used.load(SeqCst), charge);
        drop(iter);
    }
    assert_eq!(observation.allocations.load(SeqCst), 2);
    assert!(observation.blocks[0].freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
    assert_eq!(authority.used.load(SeqCst), 0);

    let keys = [OwnedKey::from("a"), OwnedKey::from("b")];
    let observation = Observation::new(&authority, 0);
    {
        let _guard = Observe::start(&observation);
        let mut map = OwnedMap::try_with_capacity(2, &scope).unwrap();
        for key in keys {
            map.try_insert(key, Value::Integer(1), &scope).unwrap();
        }
        let charge = authority.used.load(SeqCst);
        let mut iter = map.into_iter();
        assert!(observation.blocks[2].freed_at.load(SeqCst) > 0);
        assert!(observation.blocks[0].freed_at.load(SeqCst) > 0);
        assert_eq!(observation.blocks[1].freed_at.load(SeqCst), 0);
        drop(iter.next());
        assert_eq!(authority.used.load(SeqCst), charge);
        drop(iter);
    }
    assert_eq!(observation.allocations.load(SeqCst), 3);
    for block in &observation.blocks[..3] {
        assert!(block.freed_at.load(SeqCst) > 0);
        assert!(block.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
    }
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_containers_map_admitted_capacity_inserts_without_allocation() {
    use clinker_record::Value;
    use clinker_record::owned_storage::{OwnedKey, OwnedMap};
    for capacity in (0..=16).chain([28, 29, 56, 57, 112, 113]) {
        let (authority, scope) = finite(1024 * 1024);
        let mut map = OwnedMap::try_with_capacity(capacity, &scope).unwrap();
        let keys = (0..capacity)
            .map(|n| OwnedKey::from_box(n.to_string().into_boxed_str()))
            .collect::<Vec<_>>();
        let observation = Observation::new(&authority, 1);
        {
            let _guard = Observe::start(&observation);
            for key in keys {
                map.try_insert(key, Value::Null, &scope).unwrap();
            }
        }
        assert_eq!(observation.allocations.load(SeqCst), 0);
        assert_eq!(map.len(), capacity);
        assert_eq!(map.capacity(), capacity);
        drop(map);
        assert_eq!(authority.used.load(SeqCst), 0);
    }
}

#[test]
fn admitted_metadata_tracer() {
    use clinker_record::owned_storage::{OwnedKey, OwnedValues};
    use clinker_record::{
        AdmittedSchemaBuilder, DocumentContext, DocumentId, EnvelopeRecord, FieldMetadata, Record,
    };
    let (authority, scope) = finite(1 << 20);
    let mut builder = AdmittedSchemaBuilder::try_with_capacity(1, &scope).unwrap();
    builder
        .try_push(
            OwnedKey::try_new("café", &scope).unwrap(),
            Some(
                FieldMetadata::source_correlation("origin")
                    .try_clone_in(&scope)
                    .unwrap(),
            ),
            &scope,
        )
        .unwrap();
    let schema = builder.finish(&scope).unwrap();
    let observation = Observation::new(&authority, 0);
    let leaf = {
        let _observe = Observe::start(&observation);
        FieldStr::try_new(LONG, &scope).unwrap()
    };
    let mut values = OwnedValues::try_with_capacity(1, &scope).unwrap();
    values
        .try_push(clinker_record::Value::String(leaf.clone()), &scope)
        .unwrap();
    let envelope = EnvelopeRecord::from_owned_values(schema.clone(), values).unwrap();
    let context =
        DocumentContext::try_new(DocumentId::next(), Arc::from("file"), envelope, &scope).unwrap();
    let mut values = OwnedValues::try_with_capacity(1, &scope).unwrap();
    values
        .try_push(clinker_record::Value::String(leaf.clone()), &scope)
        .unwrap();
    let mut record = Record::from_owned_values(schema.clone(), values).unwrap();
    record.set_doc_ctx(context.clone());
    assert_eq!(schema.index("café"), Some(0));
    drop((record, context, schema));
    assert_eq!(leaf.as_str(), LONG);
    assert!(authority.used.load(SeqCst) > 0);
    {
        let _observe = Observe::start(&observation);
        drop(leaf);
    }
    for block in &observation.blocks[..observation.allocations.load(SeqCst)] {
        assert!(block.freed_at.load(SeqCst) > 0);
        assert!(block.freed_at.load(SeqCst) < observation.release_at.load(SeqCst));
        assert!(block.admitted_at_deallocation.load(SeqCst) >= block.size.load(SeqCst));
    }
    assert_eq!(authority.used.load(SeqCst), 0);
}

fn admitted_schema(
    scope: &AllocationScope,
) -> Result<SharedStorage<clinker_record::Schema>, ResourceError> {
    use clinker_record::owned_storage::OwnedKey;
    use clinker_record::{AdmittedSchemaBuilder, FieldMetadata};
    let mut builder = AdmittedSchemaBuilder::try_with_capacity(2, scope)?;
    builder.try_push(
        OwnedKey::try_new("café", scope)?,
        Some(FieldMetadata::SourceCorrelation {
            source_field: OwnedKey::try_new("source", scope)?,
        }),
        scope,
    )?;
    builder.try_push(
        OwnedKey::try_new("café", scope)?,
        Some(FieldMetadata::AggregateGroupIndex {
            aggregate_name: OwnedKey::try_new("aggregate", scope)?,
        }),
        scope,
    )?;
    builder.finish(scope)
}

#[test]
fn admitted_metadata_every_allocator_refusal_releases_destroyed_backings() {
    let (authority, scope) = finite(1 << 20);
    let observation = Observation::new(&authority, 0);
    {
        let _observe = Observe::start(&observation);
        let schema = admitted_schema(&scope).unwrap();
        assert_eq!(schema.index("café"), Some(1));
        assert_eq!(schema.column_count(), 2);
        assert_eq!(schema.legacy_estimated_heap_size(), 0);
        drop(schema);
    }
    let allocations = observation.allocations.load(SeqCst);
    assert!(allocations >= 18 && allocations <= observation.blocks.len());
    assert_eq!(authority.used.load(SeqCst), 0);
    for block in &observation.blocks[..allocations] {
        assert!(block.freed_at.load(SeqCst) > 0);
        assert!(block.admitted_at_allocation.load(SeqCst) >= block.size.load(SeqCst));
        assert!(block.admitted_at_deallocation.load(SeqCst) >= block.size.load(SeqCst));
    }
    for refuse_at in 1..=allocations {
        let (authority, scope) = finite(1 << 20);
        let observation = Observation::new(&authority, refuse_at);
        let result;
        {
            let _observe = Observe::start(&observation);
            result = admitted_schema(&scope);
        }
        assert_eq!(
            result.unwrap_err().kind,
            ResourceErrorKind::Allocation,
            "allocation {refuse_at}"
        );
        assert_eq!(authority.used.load(SeqCst), 0, "allocation {refuse_at}");
        for block in &observation.blocks[..refuse_at - 1] {
            assert!(block.freed_at.load(SeqCst) > 0, "allocation {refuse_at}");
            assert!(
                block.admitted_at_deallocation.load(SeqCst) >= block.size.load(SeqCst),
                "allocation {refuse_at}"
            );
        }
    }
}

#[test]
fn admitted_metadata_parallel_growth_failure_preserves_lengths_and_owners() {
    use clinker_record::owned_storage::OwnedKey;
    use clinker_record::{AdmittedSchemaBuilder, FieldMetadata};
    let (authority, scope) = finite(1 << 20);
    let mut builder = AdmittedSchemaBuilder::try_with_capacity(1, &scope).unwrap();
    builder
        .try_push(
            OwnedKey::try_new("first", &scope).unwrap(),
            Some(FieldMetadata::SourceFile),
            &scope,
        )
        .unwrap();
    let name = OwnedKey::try_new("second", &scope).unwrap();
    // First vector replacement takes two allocations; refuse the second vector backing.
    let observation = Observation::new(&authority, 3);
    let result;
    {
        let _observe = Observe::start(&observation);
        result = builder.try_push(name, None, &scope);
    }
    assert_eq!(result.unwrap_err().kind, ResourceErrorKind::Allocation);
    let schema = builder.finish(&scope).unwrap();
    assert_eq!(schema.column_count(), 1);
    assert_eq!(schema.column_name(0), Some("first"));
    assert_eq!(schema.field_metadata(0), Some(&FieldMetadata::SourceFile));
    assert_eq!(schema.index("second"), None);
    drop(schema);
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn owned_mutation_guards_and_removals_preserve_backing_and_detached_children() {
    use clinker_record::{
        Value,
        owned_storage::{OwnedKey, OwnedMap, OwnedValues},
    };
    let (authority, scope) = finite(1 << 20);
    let leaf = FieldStr::try_new(LONG, &scope).unwrap();
    let leaf_charge = authority.used.load(SeqCst);
    let mut map = OwnedMap::try_with_capacity(4, &scope).unwrap();
    map.try_insert(
        OwnedKey::try_new("first", &scope).unwrap(),
        Value::String(leaf.clone()),
        &scope,
    )
    .unwrap();
    map.try_insert(
        OwnedKey::try_new("second", &scope).unwrap(),
        Value::Integer(2),
        &scope,
    )
    .unwrap();
    let capacity = map.capacity();
    let charge = authority.used.load(SeqCst);
    let observation = Observation::new(&authority, 1);
    {
        let _observe = Observe::start(&observation);
        assert!(map.legacy_mut().is_none());
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert_eq!(authority.used.load(SeqCst), charge);
    let key_charge = map.get_index(0).unwrap().0.heap_size();
    let detached = map.shift_remove("first").unwrap();
    assert_eq!(map.capacity(), capacity);
    assert_eq!(map.get_index(0).unwrap().0.as_str(), "second");
    assert_eq!(authority.used.load(SeqCst), charge - key_charge);
    assert!(map.shift_remove("absent").is_none());
    drop(map);
    drop(leaf);
    assert_eq!(authority.used.load(SeqCst), leaf_charge);
    let mut values = OwnedValues::try_with_capacity(4, &scope).unwrap();
    values.try_push(detached, &scope).unwrap();
    values.try_push(Value::Integer(2), &scope).unwrap();
    let charge = authority.used.load(SeqCst);
    let capacity = values.capacity();
    let detached = values.remove(0).unwrap();
    assert_eq!(values.as_slice(), &[Value::Integer(2)]);
    assert_eq!(values.capacity(), capacity);
    assert_eq!(authority.used.load(SeqCst), charge);
    assert!(values.remove(1).is_none());
    drop(values);
    assert_eq!(authority.used.load(SeqCst), leaf_charge);
    drop(detached);
    assert_eq!(authority.used.load(SeqCst), 0);

    let mut legacy = indexmap::IndexMap::with_capacity(4);
    legacy.insert(OwnedKey::from("original"), Value::Integer(1));
    let mut legacy = OwnedMap::from_map(legacy);
    let address = legacy.get_index(0).unwrap().1 as *const Value;
    let key = OwnedKey::from("added");
    let observation = Observation::new(&authority, 1);
    {
        let _observe = Observe::start(&observation);
        let mut guard = legacy.legacy_mut().unwrap();
        guard.get_or_insert_with(key, || Value::Integer(2));
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert_eq!(legacy.get_index(0).unwrap().1 as *const Value, address);
    assert_eq!(legacy.get("added"), Some(&Value::Integer(2)));
}

#[test]
fn admitted_record_width_and_mixed_metadata_traversals_are_truthful() {
    use clinker_record::{
        AdmittedSchemaBuilder, FieldMetadata, Record, RecordWidthError, Value,
        owned_storage::{OwnedKey, OwnedValues},
    };
    let (authority, scope) = finite(1 << 20);
    let mut builder = AdmittedSchemaBuilder::try_with_capacity(1, &scope).unwrap();
    let name = OwnedKey::from_box("legacy-name".into());
    let address = name.as_ptr();
    builder
        .try_push(
            name,
            Some(FieldMetadata::source_correlation("legacy-meta")),
            &scope,
        )
        .unwrap();
    let schema = builder.finish(&scope).unwrap();
    assert_eq!(schema.columns()[0].as_ptr(), address);
    assert_eq!(
        schema.legacy_estimated_heap_size(),
        "legacy-name".len() + "legacy-meta".len()
    );
    let charge = authority.used.load(SeqCst);
    let values = OwnedValues::try_with_capacity(4, &scope).unwrap();
    assert_eq!(
        Record::from_owned_values(schema.clone(), values).unwrap_err(),
        RecordWidthError {
            expected: 1,
            actual: 0
        }
    );
    assert_eq!(authority.used.load(SeqCst), charge);
    let mut values = OwnedValues::try_with_capacity(4, &scope).unwrap();
    values
        .try_push(Value::String(FieldStr::new_unique(LONG)), &scope)
        .unwrap();
    let mut record = Record::from_owned_values(schema.clone(), values).unwrap();
    assert!(record.values_are_governed());
    assert_eq!(record.legacy_estimated_heap_size(), LONG.len());
    let charge = authority.used.load(SeqCst);
    record.set("legacy-name", Value::Integer(7));
    assert_eq!(record.legacy_estimated_heap_size(), 0);
    assert_eq!(authority.used.load(SeqCst), charge);
    assert!(SharedStorage::ptr_eq(
        record.doc_ctx(),
        &clinker_record::synthetic_document_context()
    ));
    drop((record, schema));
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn relative_accounting_classifies_each_owner_against_the_live_ledger() {
    use clinker_record::{
        DocumentContext, DocumentId, EnvelopeRecord, Record, Value,
        owned_storage::{OwnedKey, OwnedMap, OwnedValues},
    };
    let (local, local_scope) = finite(1 << 20);
    let (foreign, foreign_scope) = finite(1 << 20);
    let local_resources = AllocationResources::new(local.clone());
    let foreign_resources = AllocationResources::new(foreign.clone());
    let local_text = FieldStr::try_new(LONG, &local_scope).unwrap();
    let foreign_text = FieldStr::try_new_unique(LONG, &foreign_scope).unwrap();
    assert_eq!(local_text.unaccounted_heap_size(&local_resources), 0);
    assert_eq!(
        local_text.unaccounted_heap_size(&foreign_resources),
        local_text.heap_size()
    );
    assert_eq!(
        foreign_text.unaccounted_heap_size(&local_resources),
        foreign_text.heap_size()
    );
    assert_eq!(foreign_text.unaccounted_heap_size(&foreign_resources), 0);
    let foreign_text_bytes = foreign_text.heap_size();
    let mut local_values = OwnedValues::try_with_capacity(2, &local_scope).unwrap();
    local_values
        .try_push(Value::String(foreign_text), &local_scope)
        .unwrap();
    local_values
        .try_push(Value::String(local_text.clone()), &local_scope)
        .unwrap();
    assert!(local_values.is_accounted_by(&local_resources));
    assert!(!local_values.is_accounted_by(&foreign_resources));
    assert_eq!(
        local_values.unaccounted_heap_size(&local_resources),
        foreign_text_bytes
    );
    assert_eq!(
        local_values.unaccounted_heap_size(&foreign_resources),
        local_values.heap_size() - foreign_text_bytes
    );
    let mut foreign_map = OwnedMap::try_with_capacity(2, &foreign_scope).unwrap();
    foreign_map
        .try_insert(
            OwnedKey::try_new("local-key", &local_scope).unwrap(),
            Value::Array(local_values),
            &foreign_scope,
        )
        .unwrap();
    assert_eq!(
        foreign_map.unaccounted_heap_size(&local_resources),
        foreign.used.load(SeqCst)
    );
    assert_eq!(
        foreign_map.unaccounted_heap_size(&foreign_resources),
        foreign_map.heap_size() - foreign.used.load(SeqCst)
    );
    let legacy = OwnedValues::from_vec(vec![Value::String(local_text.clone())]);
    assert_eq!(
        legacy.unaccounted_heap_size(&local_resources),
        std::mem::size_of::<Value>()
    );
    assert_eq!(
        legacy.unaccounted_heap_size(&foreign_resources),
        legacy.heap_size()
    );
    let schema = admitted_schema(&local_scope).unwrap();
    assert_eq!(schema.unaccounted_heap_size(&local_resources), 0);
    assert_eq!(schema.unaccounted_outer_heap_size(&local_resources), 0);
    assert_eq!(
        schema.unaccounted_heap_size(&foreign_resources),
        schema.estimated_heap_size()
    );
    assert_eq!(
        schema.unaccounted_outer_heap_size(&foreign_resources),
        schema.estimated_outer_heap_size()
    );
    let schema_alias = schema.clone();
    let mut slots = OwnedValues::try_with_capacity(2, &foreign_scope).unwrap();
    slots
        .try_push(Value::Map(foreign_map), &foreign_scope)
        .unwrap();
    slots.try_push(Value::Null, &foreign_scope).unwrap();
    let record = Record::from_owned_values(schema.clone(), slots).unwrap();
    assert!(!record.values_are_accounted_by(&local_resources));
    assert!(record.values_are_accounted_by(&foreign_resources));
    assert_eq!(
        record.unaccounted_heap_size(&local_resources),
        foreign.used.load(SeqCst)
    );
    let document = DocumentContext::try_new(
        DocumentId::next(),
        Arc::from("file"),
        EnvelopeRecord::empty(),
        &foreign_scope,
    )
    .unwrap();
    assert_eq!(document.unaccounted_outer_heap_size(&foreign_resources), 0);
    assert_eq!(
        document.unaccounted_outer_heap_size(&local_resources),
        document.estimated_outer_heap_size()
    );
    let local_charge = local.used.load(SeqCst);
    let foreign_charge = foreign.used.load(SeqCst);
    let observation = Observation::new(&local, 1);
    {
        let _observe = Observe::start(&observation);
        std::hint::black_box(record.unaccounted_heap_size(&local_resources));
        std::hint::black_box(schema_alias.unaccounted_heap_size(&foreign_resources));
        std::hint::black_box(document.unaccounted_heap_size(&local_resources));
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert_eq!(local.used.load(SeqCst), local_charge);
    assert_eq!(foreign.used.load(SeqCst), foreign_charge);
    drop((
        local_scope,
        foreign_scope,
        local_resources,
        foreign_resources,
    ));
    drop((record, schema, schema_alias, document, legacy, local_text));
    assert_eq!(local.used.load(SeqCst), 0);
    assert_eq!(foreign.used.load(SeqCst), 0);
}

#[test]
fn relative_accounting_accepts_distinct_adapters_for_the_same_ledger() {
    struct Adapter(Arc<FiniteAuthority>);
    impl AllocationAuthority for Adapter {
        fn identity(&self) -> usize {
            self.0.identity()
        }
        fn try_reserve(
            self: Arc<Self>,
            owner: OwnerId,
            layout: Layout,
        ) -> Result<AllocationLease, ResourceError> {
            self.0.clone().try_reserve(owner, layout)
        }
        fn release(&self, owner: OwnerId, bytes: usize) {
            self.0.release(owner, bytes);
        }
        fn check_cancelled(&self) -> Result<(), ResourceError> {
            self.0.check_cancelled()
        }
    }
    let (authority, scope) = finite(4096);
    let a = Arc::new(Adapter(authority.clone()));
    let b = Arc::new(Adapter(authority.clone()));
    assert!(!Arc::ptr_eq(&a, &b));
    let resources = AllocationResources::new(b);
    let text = FieldStr::try_new(LONG, &AllocationResources::new(a).scope().unwrap()).unwrap();
    let lease = scope.reserve(Layout::new::<u64>()).unwrap();
    assert!(lease.is_accounted_by(&resources));
    assert_eq!(text.unaccounted_heap_size(&resources), 0);
    assert_eq!(authority.used.load(SeqCst), text.heap_size() + 8);
    drop((lease, text));
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn legacy_array_guard_preserves_growth_shape_and_child_ownership() {
    use clinker_record::{Value, owned_storage::OwnedValues};
    let (authority, scope) = finite(4096);
    let leaf = FieldStr::try_new(LONG, &scope).unwrap();
    let charge = authority.used.load(SeqCst);
    let mut backing = Vec::with_capacity(2);
    backing.push(Value::Null);
    let address = backing.as_ptr();
    let mut values = OwnedValues::from_vec(backing);
    let observation = Observation::new(&authority, 1);
    {
        let _observe = Observe::start(&observation);
        values.legacy_mut().unwrap().push(Value::String(leaf));
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert_eq!(values.as_ptr(), address);
    let growth = Observation::new(&authority, 0);
    {
        let _observe = Observe::start(&growth);
        values.legacy_mut().unwrap().push(Value::Integer(3));
    }
    assert_eq!(growth.allocations.load(SeqCst), 1);
    assert_eq!(authority.used.load(SeqCst), charge);
    let mut governed = OwnedValues::try_with_capacity(2, &scope).unwrap();
    governed.try_push(Value::Integer(1), &scope).unwrap();
    let charge = authority.used.load(SeqCst);
    let observation = Observation::new(&authority, 1);
    {
        let _observe = Observe::start(&observation);
        assert!(governed.legacy_mut().is_none());
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    assert_eq!(governed.capacity(), 2);
    assert_eq!(governed.as_slice(), &[Value::Integer(1)]);
    assert_eq!(authority.used.load(SeqCst), charge);
    drop((governed, values));
    assert_eq!(authority.used.load(SeqCst), 0);
}

#[test]
fn metadata_legacy_moves_keep_allocation_shape_and_compiled_slot_widths() {
    use clinker_record::{
        FieldMetadata, Schema, Value,
        owned_storage::{OwnedKey, OwnedValues},
    };
    assert_eq!(
        std::mem::size_of::<OwnedKey>(),
        std::mem::size_of::<Box<str>>()
    );
    assert_eq!(std::mem::size_of::<FieldMetadata>(), 24);
    assert_eq!(std::mem::size_of::<Option<FieldMetadata>>(), 24);
    assert_eq!(std::mem::size_of::<Value>(), 32);
    assert_eq!(
        std::mem::size_of::<OwnedValues>(),
        std::mem::size_of::<Vec<Value>>()
    );
    assert_eq!(std::mem::size_of::<SharedStorage<Schema>>(), 16);
    let (authority, _) = finite(0);
    let mut columns = Vec::with_capacity(8);
    columns.push(OwnedKey::from("café"));
    columns.push(OwnedKey::from("other"));
    let column_address = columns.as_ptr();
    let name_address = columns[0].as_ptr();
    let observation = Observation::new(&authority, 0);
    let schema;
    {
        let _observe = Observe::start(&observation);
        schema = Schema::new(columns);
    }
    // Metadata slots, one legacy hash table and its two independent key copies.
    assert_eq!(observation.allocations.load(SeqCst), 4);
    assert_eq!(schema.columns().as_ptr(), column_address);
    assert_eq!(schema.columns()[0].as_ptr(), name_address);
    assert_eq!(schema.index("café"), Some(0));
    let cloned = schema.clone();
    assert_ne!(cloned.columns().as_ptr(), column_address);
    assert_ne!(cloned.columns()[0].as_ptr(), name_address);
    assert_eq!(cloned.columns(), schema.columns());
}

#[test]
fn admitted_metadata_constants_empty_and_cancelled_builder_keep_semantics() {
    use clinker_record::{AdmittedSchemaBuilder, FieldMetadata, owned_storage::OwnedKey};
    let (authority, scope) = finite(1 << 20);
    let variants = [
        FieldMetadata::WidenedSidecar,
        FieldMetadata::SourceFile,
        FieldMetadata::SourceName,
        FieldMetadata::SourceEventTime,
        FieldMetadata::ReshapeAudit,
    ];
    let observation = Observation::new(&authority, 1);
    {
        let _observe = Observe::start(&observation);
        for value in &variants {
            assert_eq!(value.try_clone_in(&scope).unwrap(), *value);
        }
    }
    assert_eq!(observation.allocations.load(SeqCst), 0);
    let empty = AdmittedSchemaBuilder::try_with_capacity(0, &scope)
        .unwrap()
        .finish(&scope)
        .unwrap();
    assert!(empty.columns().is_empty());
    assert_eq!(empty.index("absent"), None);
    drop(empty);
    let mut builder = AdmittedSchemaBuilder::try_with_capacity(1, &scope).unwrap();
    builder
        .try_push(OwnedKey::try_new("retained", &scope).unwrap(), None, &scope)
        .unwrap();
    authority.cancelled.store(true, SeqCst);
    assert_eq!(
        builder
            .try_push("rejected".into(), None, &scope)
            .unwrap_err()
            .kind,
        ResourceErrorKind::Cancelled
    );
    authority.cancelled.store(false, SeqCst);
    let schema = builder.finish(&scope).unwrap();
    assert_eq!(schema.column_count(), 1);
    assert_eq!(schema.column_name(0), Some("retained"));
    drop(schema);
    assert_eq!(authority.used.load(SeqCst), 0);
}
