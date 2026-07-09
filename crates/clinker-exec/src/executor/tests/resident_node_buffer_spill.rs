//! White-box coverage for the resident-slot spill sweep
//! [`crate::executor::dispatch::service_pending_node_buffer_spills`].
//!
//! When the arbitrator elects an already-resident `node_buffers` slot as a
//! spill victim it only flips that slot's [`ConsumerHandle`] spill-request
//! flag — the servicing half is this sweep. The gap it closes (a flagged
//! resident slot that used to free zero bytes) cannot be exercised through a
//! live pipeline run: forcing `should_spill` to transition false→true
//! between two admissions needs either the RSS arm (uncontrollable at
//! byte granularity) or a charged-byte sum large enough to cross a soft
//! limit that also stays above the test process's real RSS (billions of
//! records). So the sweep's core takes its inputs directly and this test
//! drives it deterministically: two resident slots, one flagged, and the
//! assertion that only the flagged spillable slot transitions
//! `Memory → Spilled` with its charge discharged, its bytes recorded against
//! the disk quota under its own stage name, and its rows intact on drain.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::service_pending_node_buffer_spills;
use crate::executor::node_buffer::{NodeBuffer, NodeBufferConsumer, record_byte_cost};
use crate::pipeline::memory::{ConsumerHandle, ConsumerId, MemoryArbitrator, NoOpPolicy};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec!["id".into(), "v".into()]))
}

fn rec(s: &Arc<Schema>, id: i64, v: &str) -> Record {
    Record::new(
        Arc::clone(s),
        vec![Value::Integer(id), Value::String(v.into())],
    )
}

fn memory_slot(s: &Arc<Schema>, rows: &[(i64, &str, u64)]) -> NodeBuffer {
    NodeBuffer::memory_from_records(
        rows.iter()
            .map(|(id, v, rn)| (rec(s, *id, v), *rn))
            .collect(),
    )
}

/// Register a `NodeBufferConsumer` sharing `handle` and return its id, so
/// the sweep's `consumer_ids` map mirrors the production registration shape.
fn register(arb: &MemoryArbitrator, handle: &Arc<ConsumerHandle>) -> ConsumerId {
    arb.register_consumer(Arc::new(NodeBufferConsumer::new(handle.clone(), false)))
}

#[test]
fn flagged_resident_memory_slot_spills_and_discharges_via_sweep() {
    let arb = MemoryArbitrator::with_policy(64 * 1024 * 1024, 0.80, 0.70, Box::new(NoOpPolicy));
    let spill_dir = tempfile::tempdir().expect("temp dir");
    let s = schema();

    let idx_a = NodeIndex::new(0);
    let idx_b = NodeIndex::new(1);

    // Slot A: three records, elected as victim. Slot B: one record, never
    // flagged — its presence proves the sweep touches only flagged slots.
    let mut node_buffers: HashMap<NodeIndex, NodeBuffer> = HashMap::new();
    node_buffers.insert(
        idx_a,
        memory_slot(&s, &[(1, "a", 10), (2, "b", 11), (3, "c", 12)]),
    );
    node_buffers.insert(idx_b, memory_slot(&s, &[(9, "z", 99)]));

    let handle_a = ConsumerHandle::new();
    handle_a.set_bytes(record_byte_cost(2) * 3);
    let id_a = register(&arb, &handle_a);
    let handle_b = ConsumerHandle::new();
    handle_b.set_bytes(record_byte_cost(2));
    let id_b = register(&arb, &handle_b);

    let mut consumer_ids: HashMap<NodeIndex, (ConsumerId, Arc<ConsumerHandle>)> = HashMap::new();
    consumer_ids.insert(idx_a, (id_a, handle_a.clone()));
    consumer_ids.insert(idx_b, (id_b, handle_b.clone()));

    // The arbitration round elected A and flipped its spill-request flag; the
    // sweep is the half that actually frees the bytes.
    handle_a.request_spill();

    service_pending_node_buffer_spills(
        &mut node_buffers,
        &consumer_ids,
        &arb,
        spill_dir.path(),
        clinker_plan::config::CompressMode::On,
        2048,
        |_idx| true,
        |idx| {
            if idx == idx_a {
                "stage_a".to_string()
            } else {
                "stage_b".to_string()
            }
        },
    )
    .expect("resident-slot spill sweep");

    // A transitioned Memory → Spilled and its in-memory charge is discharged.
    assert!(
        matches!(node_buffers.get(&idx_a), Some(NodeBuffer::Spilled { .. })),
        "the elected resident slot must spill to disk"
    );
    assert_eq!(
        handle_a.bytes(),
        0,
        "the spilled slot's in-memory charge is discharged to zero"
    );
    // B was never flagged: still resident, still charged.
    assert!(
        matches!(node_buffers.get(&idx_b), Some(NodeBuffer::Memory(_))),
        "an unflagged slot must stay resident"
    );
    assert!(handle_b.bytes() > 0, "the unflagged slot keeps its charge");

    // The disk quota recorded A's spill under A's own stage name — not B's.
    assert!(
        arb.cumulative_spill_bytes() > 0,
        "the sweep recorded the spilled file against the disk quota"
    );
    let per_stage = arb.per_stage_spill_bytes();
    assert!(
        per_stage.get("stage_a").copied().unwrap_or(0) > 0,
        "the spill is attributed to the spilling stage"
    );
    assert!(
        !per_stage.contains_key("stage_b"),
        "the unflagged stage recorded no spill"
    );

    // A's records round-trip from disk in arrival order with values intact.
    let a = node_buffers.remove(&idx_a).expect("slot A present");
    let (records, _puncts) = a.drain_split().expect("drain spilled slot");
    let rns: Vec<u64> = records.iter().map(|(_, rn)| *rn).collect();
    assert_eq!(
        rns,
        vec![10, 11, 12],
        "row numbers survive the spill round-trip"
    );
    assert_eq!(
        records[0].0.values(),
        rec(&s, 1, "a").values(),
        "record values survive the spill round-trip"
    );
}

#[test]
fn flagged_non_spillable_slot_is_skipped_by_sweep() {
    let arb = MemoryArbitrator::with_policy(64 * 1024 * 1024, 0.80, 0.70, Box::new(NoOpPolicy));
    let spill_dir = tempfile::tempdir().expect("temp dir");
    let s = schema();
    let idx = NodeIndex::new(0);

    // A fan-out / composition-port slot: its consumer would reach
    // `clone_memory_only`, which panics on a spill-backed variant, so the
    // sweep must never spill it even when the arbitrator flags it.
    let mut node_buffers: HashMap<NodeIndex, NodeBuffer> = HashMap::new();
    node_buffers.insert(idx, memory_slot(&s, &[(1, "a", 1), (2, "b", 2)]));

    let handle = ConsumerHandle::new();
    handle.set_bytes(record_byte_cost(2) * 2);
    let id = register(&arb, &handle);
    let mut consumer_ids: HashMap<NodeIndex, (ConsumerId, Arc<ConsumerHandle>)> = HashMap::new();
    consumer_ids.insert(idx, (id, handle.clone()));

    handle.request_spill();

    service_pending_node_buffer_spills(
        &mut node_buffers,
        &consumer_ids,
        &arb,
        spill_dir.path(),
        clinker_plan::config::CompressMode::On,
        2048,
        |_idx| false,
        |_idx| "fanout".to_string(),
    )
    .expect("sweep skips non-spillable slots without error");

    assert!(
        matches!(node_buffers.get(&idx), Some(NodeBuffer::Memory(_))),
        "a non-spillable slot must stay resident (spilling it would later panic)"
    );
    assert!(handle.bytes() > 0, "its charge is untouched");
    assert_eq!(
        arb.cumulative_spill_bytes(),
        0,
        "nothing is recorded against the disk quota for a skipped slot"
    );
    // The flag was consumed so a still-pressured arbitrator's re-election is
    // what re-flags it, rather than the same stale flag spinning every sweep.
    assert!(
        !handle.take_spill_request(),
        "the spill-request flag is cleared even when the slot is not spilled"
    );
}
