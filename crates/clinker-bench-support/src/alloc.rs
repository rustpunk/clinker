//! Scoped heap accounting for benchmarks.
//!
//! Feature-gated behind `bench-alloc`. Wraps `std::alloc::System` with
//! atomic counters. Counters are monotonic totals (never reset); use
//! [`Region`] to measure deltas over a scope.
//!
//! `#[global_allocator]` annotation goes in each benchmark binary:
//!
//! ```ignore
//! use clinker_bench_support::alloc::AccountingAlloc;
//!
//! #[global_allocator]
//! static ALLOC: AccountingAlloc = AccountingAlloc::new();
//! ```
//!
//! # Thread safety
//!
//! All counters use `Relaxed` ordering — they are independent monotonic
//! accumulators with no inter-counter invariants. `Relaxed` is correct
//! and avoids unnecessary dmb barriers on ARM.
//!
//! # Multi-thread accounting
//!
//! 1. Sequential execution paths produce clean per-stage deltas.
//! 2. Parallel paths (rayon two-pass) report total allocation across all
//!    workers — correct as an aggregate.
//! 3. **`bench-alloc` degrades parallel wall-clock timing** (~110 ns
//!    contention per alloc). Timings with this feature enabled are not
//!    representative of production throughput.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

/// Global instance for benchmark binaries.
///
/// Benchmark binaries must declare `#[global_allocator] static ALLOC: AccountingAlloc = ...`
/// at crate root. This static is used by `StageTimer` when the `bench-alloc` feature is enabled.
pub static ALLOC: AccountingAlloc = AccountingAlloc::new();

/// Instrumenting global allocator wrapping `System`.
///
/// All counters use `Relaxed` ordering — they are independent monotonic
/// accumulators with no inter-counter invariants.
pub struct AccountingAlloc {
    allocs: AtomicUsize,
    deallocs: AtomicUsize,
    reallocs: AtomicUsize,
    bytes_alloc: AtomicUsize,
    bytes_dealloc: AtomicUsize,
    bytes_realloc: AtomicIsize,
}

impl AccountingAlloc {
    pub const fn new() -> Self {
        Self {
            allocs: AtomicUsize::new(0),
            deallocs: AtomicUsize::new(0),
            reallocs: AtomicUsize::new(0),
            bytes_alloc: AtomicUsize::new(0),
            bytes_dealloc: AtomicUsize::new(0),
            bytes_realloc: AtomicIsize::new(0),
        }
    }

    pub fn snapshot(&self) -> Stats {
        Stats {
            allocs: self.allocs.load(Ordering::Relaxed),
            deallocs: self.deallocs.load(Ordering::Relaxed),
            reallocs: self.reallocs.load(Ordering::Relaxed),
            bytes_alloc: self.bytes_alloc.load(Ordering::Relaxed),
            bytes_dealloc: self.bytes_dealloc.load(Ordering::Relaxed),
            bytes_realloc: self.bytes_realloc.load(Ordering::Relaxed),
        }
    }
}

// SAFETY: All operations forward to `System` after updating atomic counters.
// `alloc_zeroed` is overridden to prevent double-counting (the default impl
// calls `self.alloc()`). Realloc failure (null return) still counts the
// attempt — acceptable for benchmarks.
unsafe impl GlobalAlloc for AccountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_alloc.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.deallocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_dealloc
            .fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_alloc.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        self.reallocs.fetch_add(1, Ordering::Relaxed);
        if new_size > layout.size() {
            self.bytes_alloc
                .fetch_add(new_size - layout.size(), Ordering::Relaxed);
        } else if new_size < layout.size() {
            self.bytes_dealloc
                .fetch_add(layout.size() - new_size, Ordering::Relaxed);
        }
        self.bytes_realloc.fetch_add(
            new_size.wrapping_sub(layout.size()) as isize,
            Ordering::Relaxed,
        );
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

// SAFETY: AccountingAlloc only contains atomics and forwards to System.
unsafe impl Send for AccountingAlloc {}
unsafe impl Sync for AccountingAlloc {}

/// Allocator statistics snapshot.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub allocs: usize,
    pub deallocs: usize,
    pub reallocs: usize,
    pub bytes_alloc: usize,
    pub bytes_dealloc: usize,
    pub bytes_realloc: isize,
}

impl Stats {
    /// Net live bytes = total allocated − total deallocated.
    pub fn net_bytes(&self) -> isize {
        self.bytes_alloc as isize - self.bytes_dealloc as isize
    }
}

/// Scoped allocation region for delta measurement.
///
/// Captures a snapshot at creation. [`change()`](Region::change) returns
/// the delta between creation and the current state.
pub struct Region {
    baseline: Stats,
    alloc: &'static AccountingAlloc,
}

impl Region {
    pub fn new(alloc: &'static AccountingAlloc) -> Self {
        Self {
            baseline: alloc.snapshot(),
            alloc,
        }
    }

    pub fn change(&self) -> Stats {
        let now = self.alloc.snapshot();
        Stats {
            allocs: now.allocs - self.baseline.allocs,
            deallocs: now.deallocs - self.baseline.deallocs,
            reallocs: now.reallocs - self.baseline.reallocs,
            bytes_alloc: now.bytes_alloc - self.baseline.bytes_alloc,
            bytes_dealloc: now.bytes_dealloc - self.baseline.bytes_dealloc,
            bytes_realloc: now.bytes_realloc - self.baseline.bytes_realloc,
        }
    }
}
