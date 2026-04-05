//! Integration tests for `AccountingAlloc`.
//!
//! This binary sets `#[global_allocator]` so that `Region` deltas
//! reflect real heap activity. Run with:
//!
//!     cargo test -p clinker-bench-support --features bench-alloc

#![cfg(feature = "bench-alloc")]

use clinker_bench_support::alloc::{AccountingAlloc, Region};

#[global_allocator]
static ALLOC: AccountingAlloc = AccountingAlloc::new();

#[test]
fn test_accounting_alloc_counts_alloc() {
    let region = Region::new(&ALLOC);
    let v: Vec<u8> = Vec::with_capacity(1024);
    let change = region.change();
    assert!(change.allocs >= 1);
    assert!(change.bytes_alloc >= 1024);
    drop(v);
}

#[test]
fn test_accounting_alloc_counts_dealloc() {
    let region = Region::new(&ALLOC);
    {
        let _v: Vec<u8> = Vec::with_capacity(1024);
    } // v dropped here
    let change = region.change();
    assert!(change.deallocs >= 1);
    assert!(change.bytes_dealloc >= 1024);
}

#[test]
fn test_accounting_alloc_region_baseline_excluded() {
    // Allocations before region creation are excluded
    let _pre: Vec<u8> = Vec::with_capacity(4096);
    let region = Region::new(&ALLOC);
    let v: Vec<u8> = Vec::with_capacity(256);
    let change = region.change();
    // Only the 256-byte allocation should be in the delta
    assert!(change.bytes_alloc < 4096);
    drop(v);
}

#[test]
fn test_accounting_alloc_realloc_growth() {
    let region = Region::new(&ALLOC);
    let mut v: Vec<u8> = Vec::with_capacity(16);
    for _ in 0..1024 {
        v.push(0);
    }
    let change = region.change();
    assert!(change.reallocs >= 1);
    assert!(change.bytes_realloc > 0);
    drop(v);
}

#[test]
fn test_accounting_alloc_alloc_zeroed() {
    let region = Region::new(&ALLOC);
    let v: Vec<u8> = vec![0u8; 1024]; // may use alloc_zeroed
    let change = region.change();
    assert!(change.allocs >= 1);
    assert!(change.bytes_alloc >= 1024);
    drop(v);
}

#[test]
fn test_accounting_alloc_net_bytes() {
    let region = Region::new(&ALLOC);
    let v: Vec<u8> = Vec::with_capacity(1024);
    let change = region.change();
    assert!(change.net_bytes() > 0); // allocated but not freed
    drop(v);
}

#[test]
fn test_region_nested() {
    let outer = Region::new(&ALLOC);
    let _a: Vec<u8> = Vec::with_capacity(512);
    let inner = Region::new(&ALLOC);
    let _b: Vec<u8> = Vec::with_capacity(256);
    let inner_change = inner.change();
    let outer_change = outer.change();
    // Inner sees only 256-byte alloc
    assert!(inner_change.bytes_alloc >= 256);
    assert!(inner_change.bytes_alloc < 512);
    // Outer sees both
    assert!(outer_change.bytes_alloc >= 768);
}
