//! Enum-dispatched accumulators for GROUP BY aggregation.
//!
//! 7 built-in variants: Sum, Count, Avg, Min, Max, Collect, WeightedAvg.
//! Enum dispatch avoids per-group Box<dyn> heap allocations (2.3-5x faster
//! than trait objects). Compile-time type matching in merge().
//!
//! Streaming: all variants are O(1) memory except Collect (O(n) per group).
//! Blocking: hash aggregation buffers one accumulator set per group.

#[cfg(test)]
mod tests;
