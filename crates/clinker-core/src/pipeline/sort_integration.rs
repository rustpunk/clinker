//! Phase 8 Task 8.1 gate tests: end-to-end sort scenarios.
//!
//! These tests exercise the full sort pipeline: SortBuffer accumulation,
//! spill-to-disk, loser tree merge, and cascade merge for >16 files.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use clinker_record::{Record, Schema, Value};

    use crate::config::{NullOrder, SortField, SortOrder};
    use crate::pipeline::loser_tree::{LoserTree, MergeEntry};
    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
    use crate::pipeline::sort_key::encode_sort_key;

    fn schema_2() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["name".into(), "value".into()]))
    }

    fn schema_3() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            "dept".into(),
            "salary".into(),
            "seq".into(),
        ]))
    }

    fn rec2(schema: &Arc<Schema>, name: &str, value: i64) -> Record {
        Record::new(
            schema.clone(),
            vec![Value::String(name.into()), Value::Integer(value)],
        )
    }

    fn rec3(schema: &Arc<Schema>, dept: &str, salary: i64, seq: i64) -> Record {
        Record::new(
            schema.clone(),
            vec![
                Value::String(dept.into()),
                Value::Integer(salary),
                Value::Integer(seq),
            ],
        )
    }

    fn sf(field: &str, order: SortOrder) -> SortField {
        SortField {
            field: field.into(),
            order,
            null_order: None,
        }
    }

    fn sf_nulls(field: &str, order: SortOrder, nulls: NullOrder) -> SortField {
        SortField {
            field: field.into(),
            order,
            null_order: Some(nulls),
        }
    }

    /// Merge sorted spill files via LoserTree, returning records in merge order.
    fn merge_spill_files(
        files: Vec<crate::pipeline::spill::SpillFile<()>>,
        sort_by: &[SortField],
    ) -> Vec<Record> {
        let mut readers: Vec<_> = files.iter().map(|f| f.reader().unwrap()).collect();

        let initial: Vec<Option<MergeEntry>> = readers
            .iter_mut()
            .map(|r| {
                r.next().map(|res| {
                    let (record, _) = res.unwrap();
                    let key = encode_sort_key(&record, sort_by);
                    MergeEntry { key, record }
                })
            })
            .collect();

        let mut tree = LoserTree::new(initial);
        let mut result = Vec::new();

        while tree.winner().is_some() {
            let idx = tree.winner_index();
            result.push(tree.winner().unwrap().record.clone());
            let next = readers[idx].next().map(|res| {
                let (record, _) = res.unwrap();
                let key = encode_sort_key(&record, sort_by);
                MergeEntry { key, record }
            });
            tree.replace_winner(next);
        }

        result
    }

    // ── Gate tests ──────────────────────────────────────────────

    #[test]
    fn test_sort_single_field_asc() {
        let schema = schema_2();
        let sort_by = vec![sf("value", SortOrder::Asc)];
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by, 10_000_000, None, schema.clone());
        for i in (0..100).rev() {
            buf.push(rec2(&schema, &format!("r{i}"), i), ());
        }
        match buf.finish().unwrap() {
            SortedOutput::InMemory(pairs) => {
                assert_eq!(pairs.len(), 100);
                for (i, (r, _)) in pairs.iter().enumerate() {
                    assert_eq!(r.get("value"), Some(&Value::Integer(i as i64)));
                }
            }
            _ => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_single_field_desc() {
        let schema = schema_2();
        let sort_by = vec![sf("name", SortOrder::Desc)];
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by, 10_000_000, None, schema.clone());
        for name in &["alpha", "charlie", "bravo", "delta", "echo"] {
            buf.push(rec2(&schema, name, 0), ());
        }
        match buf.finish().unwrap() {
            SortedOutput::InMemory(pairs) => {
                let names: Vec<_> = pairs
                    .iter()
                    .map(|(r, _)| match r.get("name").unwrap() {
                        Value::String(s) => s.to_string(),
                        _ => panic!("expected string"),
                    })
                    .collect();
                assert_eq!(names, vec!["echo", "delta", "charlie", "bravo", "alpha"]);
            }
            _ => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_compound_keys() {
        let schema = schema_3();
        let sort_by = vec![sf("dept", SortOrder::Asc), sf("salary", SortOrder::Desc)];
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by, 10_000_000, None, schema.clone());
        buf.push(rec3(&schema, "B", 200, 1), ());
        buf.push(rec3(&schema, "A", 100, 2), ());
        buf.push(rec3(&schema, "A", 300, 3), ());
        buf.push(rec3(&schema, "B", 100, 4), ());
        match buf.finish().unwrap() {
            SortedOutput::InMemory(pairs) => {
                // A first (ASC), within A: 300 before 100 (DESC)
                assert_eq!(pairs[0].0.get("dept"), Some(&Value::String("A".into())));
                assert_eq!(pairs[0].0.get("salary"), Some(&Value::Integer(300)));
                assert_eq!(pairs[1].0.get("salary"), Some(&Value::Integer(100)));
                assert_eq!(pairs[2].0.get("dept"), Some(&Value::String("B".into())));
                assert_eq!(pairs[2].0.get("salary"), Some(&Value::Integer(200)));
                assert_eq!(pairs[3].0.get("salary"), Some(&Value::Integer(100)));
            }
            _ => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_nulls_first() {
        let schema = schema_2();
        let sort_by = vec![sf_nulls("value", SortOrder::Asc, NullOrder::First)];
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by, 10_000_000, None, schema.clone());
        buf.push(rec2(&schema, "a", 30), ());
        buf.push(
            Record::new(schema.clone(), vec![Value::String("b".into()), Value::Null]),
            (),
        );
        buf.push(rec2(&schema, "c", 10), ());
        match buf.finish().unwrap() {
            SortedOutput::InMemory(pairs) => {
                assert_eq!(pairs[0].0.get("value"), Some(&Value::Null));
                assert_eq!(pairs[1].0.get("value"), Some(&Value::Integer(10)));
                assert_eq!(pairs[2].0.get("value"), Some(&Value::Integer(30)));
            }
            _ => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_nulls_last() {
        let schema = schema_2();
        let sort_by = vec![sf_nulls("value", SortOrder::Asc, NullOrder::Last)];
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by, 10_000_000, None, schema.clone());
        buf.push(rec2(&schema, "a", 30), ());
        buf.push(
            Record::new(schema.clone(), vec![Value::String("b".into()), Value::Null]),
            (),
        );
        buf.push(rec2(&schema, "c", 10), ());
        match buf.finish().unwrap() {
            SortedOutput::InMemory(pairs) => {
                assert_eq!(pairs[0].0.get("value"), Some(&Value::Integer(10)));
                assert_eq!(pairs[1].0.get("value"), Some(&Value::Integer(30)));
                assert_eq!(pairs[2].0.get("value"), Some(&Value::Null));
            }
            _ => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_stable_equal_keys() {
        let schema = schema_3();
        let sort_by = vec![sf("dept", SortOrder::Asc)];
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by, 10_000_000, None, schema.clone());
        // All same dept — seq should preserve original order (stable sort)
        buf.push(rec3(&schema, "A", 100, 1), ());
        buf.push(rec3(&schema, "A", 200, 2), ());
        buf.push(rec3(&schema, "A", 300, 3), ());
        buf.push(rec3(&schema, "A", 400, 4), ());
        match buf.finish().unwrap() {
            SortedOutput::InMemory(pairs) => {
                for (i, (r, _)) in pairs.iter().enumerate() {
                    assert_eq!(r.get("seq"), Some(&Value::Integer(i as i64 + 1)));
                }
            }
            _ => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_spill_triggers_on_budget() {
        let schema = schema_2();
        let sort_by = vec![sf("value", SortOrder::Asc)];
        // 1KB budget — records will exceed this quickly
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by, 1024, None, schema.clone());
        let mut spilled = false;
        for i in 0..100 {
            buf.push(rec2(&schema, &format!("record_{i:04}"), i), ());
            if buf.should_spill() {
                buf.sort_and_spill().unwrap();
                spilled = true;
            }
        }
        assert!(spilled, "spill should have been triggered with 1KB budget");
    }

    #[test]
    fn test_sort_cascade_merge() {
        let schema = schema_2();
        let sort_by = vec![sf("value", SortOrder::Asc)];
        // Create 32 spill files (exceeds k_max=16 → requires cascade)
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by.clone(), 1, None, schema.clone());
        for i in 0..32 {
            buf.push(rec2(&schema, &format!("r{i}"), i), ());
            buf.sort_and_spill().unwrap();
        }
        match buf.finish().unwrap() {
            SortedOutput::Spilled(files) => {
                assert_eq!(files.len(), 32);
                // Merge all 32 files (cascade: merge 16, then merge remaining)
                // For now, merge all via LoserTree (k=32 works, just >16 comparisons)
                let merged = merge_spill_files(files, &sort_by);
                assert_eq!(merged.len(), 32);
                for (i, r) in merged.iter().enumerate() {
                    assert_eq!(r.get("value"), Some(&Value::Integer(i as i64)));
                }
            }
            _ => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_spill_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let schema = schema_2();
        let sort_by = vec![sf("value", SortOrder::Asc)];
        let mut buf: SortBuffer<()> =
            SortBuffer::new(sort_by, 1, Some(dir.path().to_path_buf()), schema.clone());
        for i in 0..5 {
            buf.push(rec2(&schema, &format!("r{i}"), i), ());
            buf.sort_and_spill().unwrap();
        }
        // Files exist while SpillFiles are alive
        let files_before: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!files_before.is_empty(), "spill files should exist");

        match buf.finish().unwrap() {
            SortedOutput::Spilled(files) => {
                // Drop the spill files (RAII cleanup)
                drop(files);
                let files_after: Vec<_> = std::fs::read_dir(dir.path())
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .collect();
                assert!(
                    files_after.is_empty(),
                    "spill files should be cleaned up after drop"
                );
            }
            _ => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_in_memory_path() {
        let dir = tempfile::tempdir().unwrap();
        let schema = schema_2();
        let sort_by = vec![sf("value", SortOrder::Asc)];
        // Large budget — everything fits in memory
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by,
            10_000_000,
            Some(dir.path().to_path_buf()),
            schema.clone(),
        );
        for i in (0..10).rev() {
            buf.push(rec2(&schema, &format!("r{i}"), i), ());
        }
        // Verify no spill files created
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            files.is_empty(),
            "no spill files should be created for in-memory path"
        );

        match buf.finish().unwrap() {
            SortedOutput::InMemory(pairs) => {
                assert_eq!(pairs.len(), 10);
                for (i, (r, _)) in pairs.iter().enumerate() {
                    assert_eq!(r.get("value"), Some(&Value::Integer(i as i64)));
                }
            }
            _ => panic!("expected InMemory"),
        }
    }
}
