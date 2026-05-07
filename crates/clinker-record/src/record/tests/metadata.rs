//! Per-record `$meta.*` key-value map tests.
//!
//! Tests: get_meta/set_meta round-trip, lazy init, clone independence,
//! multiple keys, overwrite, Send+Sync.

use super::super::*;

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec!["id".into(), "name".into()]))
}

fn test_record() -> Record {
    Record::new(
        test_schema(),
        vec![Value::Integer(1), Value::String("Alice".into())],
    )
}

#[test]
fn test_record_metadata_get_set() {
    let mut record = test_record();
    record
        .set_meta("tier", Value::String("high".into()))
        .unwrap();
    assert_eq!(record.get_meta("tier"), Some(&Value::String("high".into())));
}

#[test]
fn test_record_metadata_lazy_init() {
    let record = test_record();
    assert!(record.metadata.is_none());
}

#[test]
fn test_record_metadata_multiple_keys() {
    let mut record = test_record();
    record
        .set_meta("tier", Value::String("high".into()))
        .unwrap();
    record
        .set_meta("region", Value::String("us-east".into()))
        .unwrap();
    record.set_meta("priority", Value::Integer(1)).unwrap();
    assert_eq!(record.get_meta("tier"), Some(&Value::String("high".into())));
    assert_eq!(
        record.get_meta("region"),
        Some(&Value::String("us-east".into()))
    );
    assert_eq!(record.get_meta("priority"), Some(&Value::Integer(1)));
}

#[test]
fn test_record_metadata_overwrite() {
    let mut record = test_record();
    record
        .set_meta("tier", Value::String("low".into()))
        .unwrap();
    record
        .set_meta("tier", Value::String("high".into()))
        .unwrap();
    assert_eq!(record.get_meta("tier"), Some(&Value::String("high".into())));
}

#[test]
fn test_record_metadata_clone() {
    let mut record = test_record();
    record
        .set_meta("tier", Value::String("high".into()))
        .unwrap();

    let mut cloned = record.clone();

    // Mutating cloned metadata does not affect original (deep clone)
    cloned
        .set_meta("tier", Value::String("low".into()))
        .unwrap();
    assert_eq!(record.get_meta("tier"), Some(&Value::String("high".into())));
    assert_eq!(cloned.get_meta("tier"), Some(&Value::String("low".into())));
}

#[test]
fn test_record_metadata_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Record>();
}

#[test]
fn test_record_metadata_has_meta() {
    let mut record = test_record();
    assert!(!record.has_meta());
    record.set_meta("k", Value::Null).unwrap();
    assert!(record.has_meta());
}

#[test]
fn test_record_metadata_iter_meta() {
    let mut record = test_record();
    record.set_meta("a", Value::Integer(1)).unwrap();
    record.set_meta("b", Value::Integer(2)).unwrap();
    let entries: Vec<_> = record.iter_meta().collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, "a");
    assert_eq!(entries[1].0, "b");
}

#[test]
fn test_record_metadata_get_unset_returns_none() {
    let record = test_record();
    assert_eq!(record.get_meta("nonexistent"), None);
}

#[test]
fn test_record_metadata_cap_enforced() {
    let mut record = test_record();
    for i in 0..64 {
        record
            .set_meta(&format!("key_{i}"), Value::Integer(i as i64))
            .unwrap();
    }
    // 65th key should fail
    let result = record.set_meta("key_64", Value::Integer(64));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("cap exceeded"));
}

#[test]
fn test_record_metadata_cap_allows_overwrite() {
    let mut record = test_record();
    for i in 0..64 {
        record
            .set_meta(&format!("key_{i}"), Value::Integer(i as i64))
            .unwrap();
    }
    // Overwriting existing key should succeed even at cap
    record.set_meta("key_0", Value::Integer(999)).unwrap();
    assert_eq!(record.get_meta("key_0"), Some(&Value::Integer(999)));
}

#[test]
fn test_record_var_resolves_via_field_resolver() {
    // Phase D-3 / Item 5: record-scope state-node writes go to the
    // dedicated `record_vars` channel (independent of `$meta.*`).
    // `FieldResolver::resolve("$record.<key>")` strips the namespace
    // prefix and looks up via `get_record_var`.
    let mut record = test_record();
    record
        .set_record_var("fuzzy_score", Value::Float(0.85))
        .unwrap();
    assert_eq!(
        record.resolve("$record.fuzzy_score"),
        Some(&Value::Float(0.85)),
        "$record.<key> should resolve via the dedicated record_vars channel"
    );
    // Sanity: $meta.* and $record.<key> live in independent maps.
    assert!(
        record.resolve("$meta.fuzzy_score").is_none(),
        "$meta.<suffix> must not silently match record-var storage"
    );
}

#[test]
fn test_record_var_independent_64_key_budget() {
    // Item 5: `$meta.*` and `$record.<key>` have INDEPENDENT 64-key
    // budgets. Filling one to capacity must not affect the other.
    let mut record = test_record();
    for i in 0..64 {
        record
            .set_meta(&format!("meta_{i}"), Value::Integer(i as i64))
            .unwrap();
    }
    // 64 record-vars must still succeed even though metadata is full.
    for i in 0..64 {
        record
            .set_record_var(&format!("rv_{i}"), Value::Integer(i as i64))
            .unwrap();
    }
    // 65th of either kind exceeds its own cap.
    let meta_overflow = record.set_meta("meta_64", Value::Integer(64));
    assert!(meta_overflow.is_err(), "65th $meta.* should error");
    let rv_overflow = record.set_record_var("rv_64", Value::Integer(64));
    assert!(rv_overflow.is_err(), "65th $record.<key> should error");
}
