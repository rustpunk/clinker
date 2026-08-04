use std::collections::{BTreeSet, HashSet};
use std::fs::OpenOptions;
use std::sync::Arc;

use clinker_exec::executor::{OutputDeliveryId, SourceRowId};
use clinker_exec::pipeline::spill::SpillWriter;
use clinker_plan::plan::{EntityRef, PlanNodeId};
use clinker_record::{Record, SchemaBuilder, Value};

fn source(index: usize) -> PlanNodeId {
    PlanNodeId::new(index)
}

#[test]
fn identity_distinguishes_sources_at_the_same_ordinal() {
    let left = SourceRowId::new(source(1), 7);
    let right = SourceRowId::new(source(2), 7);

    assert_ne!(left, right);
    assert_eq!(HashSet::from([left, right]).len(), 2);
    assert_eq!(BTreeSet::from([left, right]).len(), 2);
}

#[test]
fn identity_round_trips_every_ordinal_bit_and_rejects_exhaustion() {
    for ordinal in [0, 1, u64::MAX - 1, u64::MAX] {
        let identity = SourceRowId::new(source(17), ordinal);
        let encoded = postcard::to_stdvec(&identity).expect("identity encodes");
        let decoded: SourceRowId = postcard::from_bytes(&encoded).expect("identity decodes");

        assert_eq!(decoded, identity);
        assert_eq!(decoded.source(), source(17));
        assert_eq!(decoded.ordinal(), ordinal);
    }

    assert_eq!(
        SourceRowId::new(source(17), u64::MAX - 1).checked_next(),
        Some(SourceRowId::new(source(17), u64::MAX))
    );
    assert_eq!(SourceRowId::new(source(17), u64::MAX).checked_next(), None);
}

#[test]
fn identity_delivery_adds_consumer_scope_without_changing_the_row() {
    let row = SourceRowId::new(source(4), 29);
    let first = OutputDeliveryId::new(row, source(8));
    let second = OutputDeliveryId::new(row, source(9));

    assert_ne!(first, second);
    assert_eq!(first.row(), row);
    assert_eq!(second.row(), row);
    assert_eq!(first.consumer(), source(8));
    assert_eq!(second.consumer(), source(9));
    assert_eq!(HashSet::from([first, second]).len(), 2);
    assert_eq!(HashSet::from([first.row(), second.row()]).len(), 1);
}

#[test]
fn identity_attempt_local_sequences_can_restart_without_shared_membership() {
    let source = source(3);
    let first_attempt = [
        SourceRowId::new(source, 0),
        SourceRowId::new(source, 1),
        SourceRowId::new(source, 2),
    ];
    let mut first_successes = HashSet::from(first_attempt);

    let second_attempt = [
        SourceRowId::new(source, 0),
        SourceRowId::new(source, 1),
        SourceRowId::new(source, 2),
    ];
    let second_successes = HashSet::from(second_attempt);

    assert_eq!(first_attempt, second_attempt);
    assert_eq!(first_successes, second_successes);
    first_successes.remove(&first_attempt[0]);
    assert_ne!(first_successes, second_successes);
}

fn record(schema: &Arc<clinker_record::Schema>, value: i64) -> Record {
    Record::new(Arc::clone(schema), vec![Value::Integer(value)])
}

#[test]
fn transport_attempt_sequence_does_not_reset_at_file_boundaries() {
    let source = source(6);
    let first_file = [
        SourceRowId::first(source),
        SourceRowId::first(source)
            .checked_next()
            .expect("ordinal 2"),
    ];
    let second_file = [first_file[1].checked_next().expect("ordinal 3")];

    assert_eq!(first_file.map(SourceRowId::ordinal), [1, 2]);
    assert_eq!(second_file.map(SourceRowId::ordinal), [3]);
    assert_eq!(SourceRowId::first(source), first_file[0]);
}

#[test]
fn transport_spill_round_trip_preserves_source_scope_and_ordinal_bits() {
    let schema = Arc::new(SchemaBuilder::new().with_field("value").build());
    let expected = [
        (record(&schema, 10), SourceRowId::new(source(1), 1)),
        (record(&schema, 20), SourceRowId::new(source(2), 1)),
        (record(&schema, 30), SourceRowId::new(source(1), u64::MAX)),
    ];
    let mut writer = SpillWriter::<SourceRowId>::new(Arc::clone(&schema), None, false)
        .expect("spill writer opens");
    for (record, identity) in &expected {
        writer
            .write_pair(record, identity)
            .expect("typed identity writes");
    }
    let spill = writer.finish().expect("spill finishes");

    let actual: Vec<_> = spill
        .reader()
        .expect("spill reader opens")
        .map(|pair| pair.expect("typed identity decodes"))
        .collect();
    let actual_identities: Vec<_> = actual.into_iter().map(|(_, identity)| identity).collect();
    let expected_identities: Vec<_> = expected.into_iter().map(|(_, identity)| identity).collect();

    assert_eq!(actual_identities, expected_identities);
}

#[test]
fn transport_truncated_identity_payload_is_rejected() {
    let schema = Arc::new(SchemaBuilder::new().with_field("value").build());
    let mut writer = SpillWriter::<SourceRowId>::new(Arc::clone(&schema), None, false)
        .expect("spill writer opens");
    writer
        .write_pair(&record(&schema, 10), &SourceRowId::new(source(4), u64::MAX))
        .expect("typed identity writes");
    let spill = writer.finish().expect("spill finishes");
    let file = OpenOptions::new()
        .write(true)
        .open(spill.path())
        .expect("spill reopens for corruption fixture");
    let len = file.metadata().expect("spill metadata").len();
    file.set_len(len - 1).expect("truncate identity payload");

    let mut reader = spill.reader().expect("header remains readable");
    assert!(
        reader.next().is_some_and(|result| result.is_err()),
        "a truncated typed identity frame must be rejected, not treated as clean EOF"
    );
}
