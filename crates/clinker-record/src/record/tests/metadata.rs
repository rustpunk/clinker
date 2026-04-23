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
