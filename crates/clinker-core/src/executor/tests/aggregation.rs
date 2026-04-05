//! Aggregation engine tests for Phase 16.
//!
//! Covers: hash aggregation (basic, multi-key, NULL keys, spill, memory tracking),
//! streaming aggregation (sorted input, sort verification, strategy selection),
//! and plan-time integration (PlanNode::Aggregation, output schema).

use super::*;

// ---------- Hash aggregation ----------

#[test]
fn test_hash_agg_basic_sum_count() {
    // GROUP BY dept, SUM(salary), COUNT(*) → correct per-group (set-equality)
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_null_group_key() {
    // NULL dept → all NULLs in one group
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_multi_column_key() {
    // GROUP BY (dept, region) → correct groups
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_single_group() {
    // All records same key → one output record
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_all_unique() {
    // N records → N output records
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_memory_delta_tracking() {
    // value_heap_bytes increases with CollectAccumulator adds, Sum adds return 0 delta
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_resize_aware_spill() {
    // allocation_size() * 3 exceeds budget → spill before resize, no OOM
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_spill_triggered() {
    // Low budget → spill occurs, correct results via merge
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_spill_flush_merge() {
    // Multiple spills + final flush → all merged via StreamingAggregator, correct results
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_empty_input() {
    // Zero records → zero output
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_factory_clone() {
    // Factory prototype clone produces independent accumulators
    todo!("Task 16.3")
}

#[test]
fn test_hash_agg_output_schema() {
    // Output schema = group-by fields ++ emit fields, derived at plan time
    todo!("Task 16.3")
}

#[test]
fn test_accumulator_enum_serde_roundtrip() {
    // Serialize + deserialize AccumulatorEnum (all 7 variants) → identical state
    todo!("Task 16.3")
}

#[test]
fn test_group_key_to_value_roundtrip() {
    // GroupByKey::to_value() produces correct Value for all variants
    todo!("Task 16.3")
}

#[test]
fn test_group_key_sort_order_consistency() {
    // GroupByKey → Value → encode_sort_key() order matches direct GroupByKey comparison
    todo!("Task 16.3")
}

#[test]
fn test_plan_node_aggregation() {
    // PlanNode::Aggregation constructed with correct output schema at plan time
    todo!("Task 16.3")
}

#[test]
fn test_aggregation_input_enum() {
    // AggregateInput::RawRecord and SpilledState both processed correctly
    todo!("Task 16.3")
}

// ---------- Streaming aggregation ----------

#[test]
fn test_streaming_agg_sorted_input() {
    // Sorted by dept → streaming mode, correct results (set-equality)
    todo!("Task 16.4")
}

#[test]
fn test_streaming_agg_vs_hash_same_results() {
    // Same input: streaming and hash produce identical output (set-equality)
    todo!("Task 16.4")
}

#[test]
fn test_streaming_agg_o1_memory() {
    // 1M records, 10 groups → peak memory constant (fixed-size accumulators)
    todo!("Task 16.4")
}

#[test]
fn test_streaming_agg_last_group_flushed() {
    // Last group emitted after flush()
    todo!("Task 16.4")
}

#[test]
fn test_streaming_agg_single_group() {
    // All records same key → one output
    todo!("Task 16.4")
}

#[test]
fn test_streaming_agg_key_boundary_all_types() {
    // Int, Str, Float, Null key types → correct boundaries
    todo!("Task 16.4")
}

#[test]
fn test_streaming_agg_null_key() {
    // NULL key records grouped together
    todo!("Task 16.4")
}

// ---------- Sort verification ----------

#[test]
fn test_sort_order_violation_hard_error() {
    // Out-of-order key at boundary → PipelineError::SortOrderViolation
    todo!("Task 16.4")
}

#[test]
fn test_sort_order_violation_message_contains_keys() {
    // Error message includes offending key values
    todo!("Task 16.4")
}

#[test]
fn test_sort_order_first_group_no_false_positive() {
    // First group transition (None → Some) does not trigger violation
    todo!("Task 16.4")
}

// ---------- Plan-time strategy selection ----------

#[test]
fn test_no_sort_order_uses_hash() {
    // No sort_order declared → AggregateStrategy::Hash (no auto-detection)
    todo!("Task 16.4")
}

#[test]
fn test_sort_order_prefix_match_streaming() {
    // sort_order [dept, region, name] + group_by [dept, region] → Streaming
    todo!("Task 16.4")
}

#[test]
fn test_sort_order_partial_prefix_uses_hash() {
    // sort_order [dept] + group_by [dept, region] → Hash fallback
    todo!("Task 16.4")
}

#[test]
fn test_group_by_reordered_to_match_sort() {
    // sort_order [region, dept] + group_by [dept, region] → Streaming with reordered [region, dept]
    todo!("Task 16.4")
}

#[test]
fn test_global_fold_always_streaming() {
    // group_by: [] → Streaming regardless of sort_order
    todo!("Task 16.4")
}

#[test]
fn test_disjoint_sort_fields_uses_hash() {
    // sort_order [name, age] + group_by [dept, region] → Hash
    todo!("Task 16.4")
}

// ---------- Merge mode ----------

#[test]
fn test_merge_mode_streaming() {
    // StreamingAggregator<MergeState> produces correct results from spilled AccumulatorRow inputs
    todo!("Task 16.4")
}

#[test]
fn test_merge_mode_key_boundary() {
    // Key boundary detection in merge mode identical to raw mode
    todo!("Task 16.4")
}
