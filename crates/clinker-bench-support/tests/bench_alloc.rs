//! Integration tests for `AccountingAlloc`.
//!
//! Exact region deltas use separate allocator instances so concurrent test
//! harness allocations cannot contaminate them. A global-wiring smoke test
//! separately proves that ordinary heap allocations reach the global instance.
//!
//!     cargo test -p clinker-bench-support --features bench-alloc

#![cfg(feature = "bench-alloc")]

use clinker_bench_support::alloc::{AccountingAlloc, Region};
use std::alloc::{GlobalAlloc, Layout, handle_alloc_error};
use std::ptr::NonNull;

#[global_allocator]
static ALLOC: AccountingAlloc = AccountingAlloc::new();

/// Owns a real allocation made through the chosen counter instance.
struct Allocation {
    allocator: &'static AccountingAlloc,
    ptr: NonNull<u8>,
    layout: Layout,
}

impl Allocation {
    fn new(allocator: &'static AccountingAlloc, size: usize, zeroed: bool) -> Self {
        assert!(size > 0);
        let layout = Layout::array::<u8>(size).unwrap();
        // SAFETY: the layout is nonzero and valid. This owner retains the
        // allocator and matching layout for every resize and final deallocation.
        let ptr = unsafe {
            if zeroed {
                allocator.alloc_zeroed(layout)
            } else {
                allocator.alloc(layout)
            }
        };
        let ptr = NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout));
        Self {
            allocator,
            ptr,
            layout,
        }
    }

    fn resize(&mut self, size: usize) {
        assert!(size > 0);
        let layout = Layout::array::<u8>(size).unwrap();
        // SAFETY: ptr is live and came from this allocator with self.layout;
        // the new nonzero layout has the same alignment and a valid size.
        let ptr = unsafe {
            self.allocator
                .realloc(self.ptr.as_ptr(), self.layout, layout.size())
        };
        self.ptr = NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout));
        self.layout = layout;
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        // SAFETY: this owner uniquely holds the live pointer and its current
        // layout; no allocation escapes or is freed by another owner.
        unsafe { self.allocator.dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

#[test]
fn global_allocator_observes_ordinary_heap_allocations() {
    let region = Region::new(&ALLOC);
    let bytes = std::hint::black_box(Vec::<u8>::with_capacity(1024));
    let change = region.change();
    // Other test-harness allocations may contribute to global totals.
    assert!(change.allocs >= 1);
    assert!(change.bytes_alloc >= bytes.capacity());
    drop(bytes);
}

#[test]
fn test_accounting_alloc_counts_alloc() {
    static COUNTERS: AccountingAlloc = AccountingAlloc::new();
    let region = Region::new(&COUNTERS);
    let allocation = Allocation::new(&COUNTERS, 1024, false);
    let change = region.change();
    assert_eq!(change.allocs, 1);
    assert_eq!(change.bytes_alloc, 1024);
    drop(allocation);
}

#[test]
fn test_accounting_alloc_counts_dealloc() {
    static COUNTERS: AccountingAlloc = AccountingAlloc::new();
    let region = Region::new(&COUNTERS);
    drop(Allocation::new(&COUNTERS, 1024, false));
    let change = region.change();
    assert_eq!(change.deallocs, 1);
    assert_eq!(change.bytes_dealloc, 1024);
}

#[test]
fn test_accounting_alloc_region_baseline_excluded() {
    static COUNTERS: AccountingAlloc = AccountingAlloc::new();
    let _pre = Allocation::new(&COUNTERS, 4096, false);
    let region = Region::new(&COUNTERS);
    let _allocation = Allocation::new(&COUNTERS, 256, false);
    let change = region.change();
    assert_eq!(change.allocs, 1);
    assert_eq!(change.bytes_alloc, 256);
}

#[test]
fn test_accounting_alloc_realloc_growth() {
    static COUNTERS: AccountingAlloc = AccountingAlloc::new();
    let region = Region::new(&COUNTERS);
    let mut allocation = Allocation::new(&COUNTERS, 16, false);
    allocation.resize(1024);
    let change = region.change();
    assert_eq!(change.reallocs, 1);
    assert_eq!(change.bytes_realloc, 1024 - 16);
    assert_eq!(change.bytes_alloc, 1024);
    drop(allocation);
    assert_eq!(region.change().net_bytes(), 0);
}

#[test]
fn test_accounting_alloc_alloc_zeroed() {
    static COUNTERS: AccountingAlloc = AccountingAlloc::new();
    let region = Region::new(&COUNTERS);
    let allocation = Allocation::new(&COUNTERS, 1024, true);
    // SAFETY: alloc_zeroed initialized every byte in this live allocation.
    let bytes =
        unsafe { std::slice::from_raw_parts(allocation.ptr.as_ptr(), allocation.layout.size()) };
    assert!(bytes.iter().all(|byte| *byte == 0));
    let change = region.change();
    assert_eq!(change.allocs, 1);
    assert_eq!(change.bytes_alloc, 1024);
}

#[test]
fn test_accounting_alloc_net_bytes() {
    static COUNTERS: AccountingAlloc = AccountingAlloc::new();
    let region = Region::new(&COUNTERS);
    let allocation = Allocation::new(&COUNTERS, 1024, false);
    assert_eq!(region.change().net_bytes(), 1024);
    drop(allocation);
    assert_eq!(region.change().net_bytes(), 0);
}

#[test]
fn test_region_nested() {
    static COUNTERS: AccountingAlloc = AccountingAlloc::new();
    let outer = Region::new(&COUNTERS);
    let _a = Allocation::new(&COUNTERS, 512, false);
    let inner = Region::new(&COUNTERS);
    let _b = Allocation::new(&COUNTERS, 256, false);
    let inner_change = inner.change();
    let outer_change = outer.change();
    assert_eq!(inner_change.allocs, 1);
    assert_eq!(inner_change.bytes_alloc, 256);
    assert_eq!(outer_change.allocs, 2);
    assert_eq!(outer_change.bytes_alloc, 768);
}
