//! A reader that materializes a shared node-buffer slot (one later readers
//! still need) must reserve its copy while the slot is still a reclaim
//! victim, and take its cursor only once the reservation is granted.
//!
//! A cursor shares the slot's backing, and a backing a cursor shares cannot
//! spill. A reader that took its cursor first would leave the reclaim pass
//! its own reservation starts nothing to free but the slot it is reading, so
//! the copy and the backing would have to fit together. These tests build the
//! walk reclaim set directly, with the slot already admitted and charged
//! through its registration, so the order is observed without a slot
//! admission's own spill decision in the way.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use clinker_plan::config::CompressMode;
use clinker_plan::error::PipelineError;
use clinker_plan::runtime_error::{ConsumerLabel, MemorySurface};
use clinker_record::owned_storage::SharedStorage;
use clinker_record::{Record, Schema, Value};
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{NodeBufferKey, shared_node_buffer_read};
use crate::executor::node_buffer::{NodeBuffer, NodeBufferConsumer};
use crate::executor::stream_event::SourceRowId;
use crate::pipeline::memory::walk::{
    SlotSpill, WalkContextGuard, WalkReclaimSet, WalkSpillSettings,
};
use crate::pipeline::memory::{
    BackPressurePreferred, ConsumerHandle, ConsumerId, MemoryArbitrator, Priority,
};

const PRODUCER: &str = "fanout";
const READER: &str = "reader_a";
const ROWS: u64 = 64;

fn schema() -> SharedStorage<Schema> {
    SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "v".into()])))
}

fn rows(s: &SharedStorage<Schema>) -> Vec<(Record, SourceRowId)> {
    (0..ROWS)
        .map(|i| {
            let record = Record::new(
                s.clone(),
                vec![
                    Value::Integer(i as i64),
                    Value::String(format!("row-{i}").into()),
                ],
            );
            (record, SourceRowId::from(i + 1))
        })
        .collect()
}

/// The rows a read yields, as comparable values and row ids.
fn read_rows(buffer: NodeBuffer) -> Vec<(Vec<Value>, SourceRowId)> {
    let (records, _puncts) = buffer.drain_split().expect("the read drains");
    records
        .into_iter()
        .map(|(record, row)| (record.values().to_vec(), row))
        .collect()
}

/// The copy a materializing reader of the slot reserves.
fn copy_bytes() -> u64 {
    NodeBuffer::memory_from_records(rows(&schema())).estimated_materialized_bytes()
}

/// The production default policy, so a pass elects victims as a run does.
fn arbitrator(capacity: u64) -> Arc<MemoryArbitrator> {
    let arbitrator = Arc::new(MemoryArbitrator::with_policy(
        64 * 1024 * 1024,
        0.80,
        0.70,
        Box::new(BackPressurePreferred::wrapping(Priority)),
    ));
    arbitrator.set_test_capacity(capacity);
    arbitrator
}

fn reclaim_set(spill_root: &std::path::Path) -> Rc<RefCell<WalkReclaimSet>> {
    Rc::new(RefCell::new(WalkReclaimSet::new(WalkSpillSettings {
        spill_root: Arc::from(spill_root),
        spill_compress: CompressMode::Auto,
        batch_size: 1024,
    })))
}

/// Publish `buffer` at `key` for two readers, registered as a node-buffer
/// slot whose handle holds `charge`.
fn publish_shared_slot(
    arbitrator: &MemoryArbitrator,
    set: &Rc<RefCell<WalkReclaimSet>>,
    key: &NodeBufferKey,
    buffer: NodeBuffer,
    charge: u64,
) -> Arc<ConsumerHandle> {
    let handle = ConsumerHandle::new();
    let id = arbitrator.register_node_consumer(
        Arc::new(NodeBufferConsumer::new(handle.clone())),
        handle.clone(),
        ConsumerLabel {
            node: PRODUCER.to_string(),
            surface: MemorySurface::BufferedRows {
                from: PRODUCER.to_string(),
                to: READER.to_string(),
            },
        },
    );
    handle.try_grow(charge).expect("the slot itself fits");
    let mut set = set.borrow_mut();
    let slots = set.slots_mut();
    slots
        .readers_mut()
        .publish(key.clone(), 2, PRODUCER)
        .expect("two readers");
    slots.register(
        key.clone(),
        (id, handle.clone()),
        SlotSpill {
            spill_allowed: true,
            node_name: PRODUCER.into(),
        },
    );
    slots.insert_buffer(key.clone(), buffer);
    handle
}

fn remaining_readers(set: &Rc<RefCell<WalkReclaimSet>>, key: &NodeBufferKey) -> Option<usize> {
    set.borrow().slots().readers().remaining(key)
}

#[test]
fn shared_slot_reader_reserves_before_it_pins_the_backing() {
    let s = schema();
    let expected = read_rows(NodeBuffer::memory_from_records(rows(&s)));
    let slot = NodeBuffer::memory_from_records(rows(&s));
    let slot_bytes = slot.estimated_memory_bytes();
    let copy = copy_bytes();
    assert_eq!(
        slot_bytes, copy,
        "the premise: the copy is as large as the slot"
    );
    // Room for the slot, or for the reader's copy, but not for both at once.
    let capacity = slot_bytes + copy / 2;
    let arbitrator = arbitrator(capacity);
    let spill_dir = tempfile::tempdir().expect("spill dir");
    let set = reclaim_set(spill_dir.path());
    let _walk = WalkContextGuard::install(&arbitrator, Rc::clone(&set));
    let key = NodeBufferKey::from(NodeIndex::new(0));
    let slot_handle = publish_shared_slot(&arbitrator, &set, &key, slot, slot_bytes);

    let (cursor, reservation) = shared_node_buffer_read(&set, key.clone(), READER)
        .and_then(|input| input.into_materialized_parts(&arbitrator, READER))
        .expect("the pass the reader's reservation starts spills the shared slot");

    assert_eq!(
        slot_handle.bytes(),
        0,
        "the shared slot's charge was released"
    );
    assert!(
        arbitrator
            .per_stage_spill_bytes_written()
            .get(PRODUCER)
            .is_some_and(|bytes| *bytes > 0),
        "the shared slot spilled to disk"
    );
    assert!(
        !set.borrow()
            .slots()
            .buffer(&key)
            .expect("the slot stays published for its other reader")
            .is_resident_memory(),
        "the slot the other reader will read is on disk"
    );
    assert_eq!(
        reservation.as_ref().map(|r| r.bytes()),
        Some(copy),
        "the reader holds a reservation for its whole copy"
    );
    assert_eq!(
        read_rows(cursor),
        expected,
        "the first read equals a resident read"
    );
    drop(reservation);
    assert_eq!(
        remaining_readers(&set, &key),
        Some(1),
        "the first read is counted once it has its cursor"
    );

    // The slot's last reader reads the spilled slot from disk.
    let last = set
        .borrow_mut()
        .slots_mut()
        .remove_buffer(&key)
        .expect("the slot is still published")
        .into_authoritative();
    assert_eq!(
        read_rows(last),
        expected,
        "the read from disk equals a resident read"
    );
}

/// A charged-only consumer: its bytes are what its handle holds, and it has
/// no state a spill can free.
struct Held(Arc<ConsumerHandle>);

impl crate::pipeline::memory::MemoryConsumer for Held {
    fn current_usage(&self) -> u64 {
        self.0.bytes()
    }

    fn spill_priority(&self) -> i32 {
        i32::MAX
    }

    fn try_spill(
        &self,
        target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        Err(crate::pipeline::memory::ConsumerSpillError::BelowTarget {
            target: target_bytes,
            freed: 0,
        })
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

fn register_held(arbitrator: &MemoryArbitrator, bytes: u64) -> (ConsumerId, Arc<ConsumerHandle>) {
    let handle = ConsumerHandle::new();
    let id = arbitrator.register_consumer(
        Arc::new(Held(handle.clone())),
        handle.clone(),
        ConsumerLabel {
            node: "held".to_string(),
            surface: MemorySurface::GroupState,
        },
    );
    handle.try_grow(bytes).expect("the held bytes fit");
    (id, handle)
}

#[test]
fn refused_shared_read_leaves_the_slot_and_its_reader_count_unchanged() {
    let s = schema();
    let expected = read_rows(NodeBuffer::memory_from_records(rows(&s)));
    let spill_dir = tempfile::tempdir().expect("spill dir");
    let (slot, _file_bytes) = NodeBuffer::memory_from_records(rows(&s))
        .spill_resident_memory(Some(spill_dir.path()), false)
        .expect("the slot spills before it is published");
    assert!(!slot.is_resident_memory());
    let copy = copy_bytes();
    let capacity = 2 * copy;
    let arbitrator = arbitrator(capacity);
    let set = reclaim_set(spill_dir.path());
    let _walk = WalkContextGuard::install(&arbitrator, Rc::clone(&set));
    let key = NodeBufferKey::from(NodeIndex::new(0));
    publish_shared_slot(&arbitrator, &set, &key, slot, 0);
    // Everything but half a copy is held by state no spill can free.
    let (_held_id, held) = register_held(&arbitrator, capacity - copy / 2);
    let consumers_before = arbitrator.consumer_count();
    let charged_before = arbitrator.charged_bytes();

    match shared_node_buffer_read(&set, key.clone(), READER)
        .and_then(|input| input.into_materialized_parts(&arbitrator, READER))
    {
        Err(PipelineError::MemoryBudgetExceeded { node, .. }) => assert_eq!(node, READER),
        Ok(_) => panic!("nothing can free the copy, so the read must be refused"),
        Err(other) => panic!("expected the reader's E310; got {other:?}"),
    }
    {
        let set = set.borrow();
        let slots = set.slots();
        assert!(slots.contains_buffer(&key), "the slot is still published");
        assert!(slots.is_registered(&key), "the slot keeps its registration");
        assert!(
            !slots.buffer(&key).expect("published").is_resident_memory(),
            "the slot is as it was"
        );
        assert_eq!(
            slots.readers().remaining(&key),
            Some(2),
            "a refused read is not counted"
        );
    }
    assert_eq!(arbitrator.consumer_count(), consumers_before);
    assert_eq!(arbitrator.charged_bytes(), charged_before);

    held.shrink(held.bytes());
    let (cursor, reservation) = shared_node_buffer_read(&set, key.clone(), READER)
        .and_then(|input| input.into_materialized_parts(&arbitrator, READER))
        .expect("the retried read fits once the held bytes are released");
    assert_eq!(read_rows(cursor), expected);
    drop(reservation);
    assert_eq!(remaining_readers(&set, &key), Some(1));
}
