//! Phase 8 Task 8.1 gate tests: end-to-end sort scenarios.
//!
//! These tests exercise the full sort pipeline: SortBuffer accumulation,
//! spill-to-disk, loser tree merge, and cascade merge for >16 files.

#[cfg(test)]
mod tests {
    fn test_allocation_resources() -> clinker_record::owned_storage::AllocationResources {
        clinker_format::preparation::MemoryOnlyResources::new(
            std::num::NonZeroUsize::new(1024 * 1024 * 1024).unwrap(),
        )
        .resources()
        .allocation()
        .clone()
    }

    use clinker_record::owned_storage::SharedStorage;
    use std::sync::Arc;

    use clinker_record::{Record, Schema, Value};

    use crate::pipeline::loser_tree::{LoserTree, MergeEntry};
    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
    use crate::pipeline::sort_key::encode_sort_key;
    use clinker_plan::config::{NullOrder, SortField, SortOrder};

    fn schema_2() -> SharedStorage<Schema> {
        SharedStorage::from_arc(Arc::new(Schema::new(vec!["name".into(), "value".into()])))
    }

    fn schema_3() -> SharedStorage<Schema> {
        SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "dept".into(),
            "salary".into(),
            "seq".into(),
        ])))
    }

    fn rec2(schema: &SharedStorage<Schema>, name: &str, value: i64) -> Record {
        Record::new(
            schema.clone(),
            vec![Value::String(name.into()), Value::Integer(value)],
        )
    }

    fn rec3(schema: &SharedStorage<Schema>, dept: &str, salary: i64, seq: i64) -> Record {
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
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by,
            10_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        for i in (0..100).rev() {
            buf.push(rec2(&schema, &format!("r{i}"), i), ());
        }
        match buf.finish().unwrap().0 {
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
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by,
            10_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        for name in &["alpha", "charlie", "bravo", "delta", "echo"] {
            buf.push(rec2(&schema, name, 0), ());
        }
        match buf.finish().unwrap().0 {
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
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by,
            10_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        buf.push(rec3(&schema, "B", 200, 1), ());
        buf.push(rec3(&schema, "A", 100, 2), ());
        buf.push(rec3(&schema, "A", 300, 3), ());
        buf.push(rec3(&schema, "B", 100, 4), ());
        match buf.finish().unwrap().0 {
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
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by,
            10_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        buf.push(rec2(&schema, "a", 30), ());
        buf.push(
            Record::new(schema.clone(), vec![Value::String("b".into()), Value::Null]),
            (),
        );
        buf.push(rec2(&schema, "c", 10), ());
        match buf.finish().unwrap().0 {
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
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by,
            10_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        buf.push(rec2(&schema, "a", 30), ());
        buf.push(
            Record::new(schema.clone(), vec![Value::String("b".into()), Value::Null]),
            (),
        );
        buf.push(rec2(&schema, "c", 10), ());
        match buf.finish().unwrap().0 {
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
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by,
            10_000_000,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        // All same dept — seq should preserve original order (stable sort)
        buf.push(rec3(&schema, "A", 100, 1), ());
        buf.push(rec3(&schema, "A", 200, 2), ());
        buf.push(rec3(&schema, "A", 300, 3), ());
        buf.push(rec3(&schema, "A", 400, 4), ());
        match buf.finish().unwrap().0 {
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
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by,
            1024,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
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
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by.clone(),
            1,
            None,
            true,
            schema.clone(),
            test_allocation_resources(),
        );
        for i in 0..32 {
            buf.push(rec2(&schema, &format!("r{i}"), i), ());
            buf.sort_and_spill().unwrap();
        }
        match buf.finish().unwrap().0 {
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
        let mut buf: SortBuffer<()> = SortBuffer::new(
            sort_by,
            1,
            Some(dir.path().to_path_buf()),
            true,
            schema.clone(),
            test_allocation_resources(),
        );
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

        match buf.finish().unwrap().0 {
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
            true,
            schema.clone(),
            test_allocation_resources(),
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

        match buf.finish().unwrap().0 {
            SortedOutput::InMemory(pairs) => {
                assert_eq!(pairs.len(), 10);
                for (i, (r, _)) in pairs.iter().enumerate() {
                    assert_eq!(r.get("value"), Some(&Value::Integer(i as i64)));
                }
            }
            _ => panic!("expected InMemory"),
        }
    }

    mod resident_and_spilled {
        use super::*;
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use crate::pipeline::sort_key::compare_authored_keys;
        use crate::pipeline::spill_merge::{MergeBudget, merge_sorted_runs};
        use proptest::prelude::*;
        use rust_decimal::Decimal;

        /// Sort-key values weighted so NaNs of both signs, signed zeros,
        /// integers, floats and decimals of equal value, nulls and exact
        /// duplicates all turn up in one batch.
        fn sort_value() -> impl Strategy<Value = Value> {
            prop_oneof![
                2 => Just(Value::Null),
                1 => Just(Value::Float(f64::NAN)),
                1 => Just(Value::Float(-f64::NAN)),
                1 => Just(Value::Float(f64::from_bits(0xFFF0_0000_0000_0042))),
                1 => Just(Value::Float(0.0)),
                1 => Just(Value::Float(-0.0)),
                1 => Just(Value::Float(f64::INFINITY)),
                1 => Just(Value::Float(f64::NEG_INFINITY)),
                1 => Just(Value::Integer((1 << 53) + 1)),
                1 => Just(Value::Float(9_007_199_254_740_992.0)),
                3 => (-3i64..=3).prop_map(Value::Integer),
                3 => (-6i64..=6).prop_map(|halves| Value::Float(halves as f64 / 2.0)),
                3 => (-30i64..=30, 0u32..=2).prop_map(|(m, s)| Value::Decimal(Decimal::new(m, s))),
            ]
        }

        fn schema() -> SharedStorage<Schema> {
            SharedStorage::from_arc(Arc::new(Schema::new(vec!["v".into(), "id".into()])))
        }

        /// A value's identity down to its bits, so a NaN's sign and payload
        /// and a zero's sign count: the property checks that each output row
        /// is the exact record its payload says it is.
        fn bits(v: Option<&Value>) -> String {
            match v {
                Some(Value::Float(f)) => format!("float {:#x}", f.to_bits()),
                other => format!("{other:?}"),
            }
        }

        fn identities(rows: &[(Record, u64)]) -> Vec<(String, u64)> {
            rows.iter()
                .map(|(record, payload)| (bits(record.get("v")), *payload))
                .collect()
        }

        proptest! {
            // 128 cases; each spills up to 40 single-row runs three times
            // over, and the whole property runs in about a second.
            #![proptest_config(ProptestConfig::with_cases(128))]

            /// A Sort's output is the same (record, payload) sequence whether
            /// it stays resident or spills runs of 1, 2 or 7 rows and merges
            /// them, and that sequence is a stable sort by the authored
            /// comparator. A comparator that was not a total order (a NaN
            /// equal to everything) would let run boundaries change it.
            ///
            /// Every record here has the same shape (two inline values, a
            /// `u64` payload), so each push charges the buffer the same
            /// number of bytes, measured below as `row_bytes`. A threshold of
            /// `k × row_bytes` makes `should_spill` report exactly after the
            /// `k`-th push since the last run, so every run but the last holds
            /// `k` rows.
            #[test]
            fn sort_output_is_identical_in_memory_and_spilled(
                values in prop::collection::vec(sort_value(), 0..=40),
                descending in any::<bool>(),
                nulls_first in any::<bool>(),
            ) {
                let schema = schema();
                let sort_by = vec![sf_nulls(
                    "v",
                    if descending { SortOrder::Desc } else { SortOrder::Asc },
                    if nulls_first { NullOrder::First } else { NullOrder::Last },
                )];
                let input: Vec<(Record, u64)> = values
                    .iter()
                    .enumerate()
                    .map(|(id, v)| {
                        let record = Record::new(
                            schema.clone(),
                            vec![v.clone(), Value::Integer(id as i64)],
                        );
                        (record, id as u64)
                    })
                    .collect();

                let mut reference = input.clone();
                reference.sort_by(|(a, _), (b, _)| compare_authored_keys(a, b, &sort_by));
                let reference = identities(&reference);

                let mut resident: SortBuffer<u64> = SortBuffer::new(
                    sort_by.clone(),
                    usize::MAX,
                    None,
                    true,
                    schema.clone(),
                    test_allocation_resources(),
                );
                for (record, payload) in input.iter().cloned() {
                    resident.push(record, payload);
                }
                let resident = match resident.finish().unwrap().0 {
                    SortedOutput::InMemory(rows) => identities(&rows),
                    SortedOutput::Spilled(_) => panic!("an unbounded threshold spilled"),
                };
                prop_assert_eq!(&resident, &reference);

                let Some((first, first_payload)) = input.first().cloned() else {
                    return Ok(());
                };
                let mut probe: SortBuffer<u64> = SortBuffer::new(
                    sort_by.clone(),
                    usize::MAX,
                    None,
                    true,
                    schema.clone(),
                    test_allocation_resources(),
                );
                probe.push(first, first_payload);
                let row_bytes = probe.bytes_used();
                prop_assert!(row_bytes > 0);

                // Charged with every run the merge reads, as a sort's own spills
                // are; `record_spill_bytes` reports whether the disk cap is
                // exceeded, which an uncapped arbitrator never is.
                let arbitrator =
                    MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy));
                for rows_per_run in [1usize, 2, 7] {
                    let mut buffer: SortBuffer<u64> = SortBuffer::new(
                        sort_by.clone(),
                        rows_per_run * row_bytes,
                        None,
                        true,
                        schema.clone(),
                        test_allocation_resources(),
                    );
                    let mut pending = 0usize;
                    for (record, payload) in input.iter().cloned() {
                        let before = buffer.bytes_used();
                        buffer.push(record, payload);
                        prop_assert_eq!(buffer.bytes_used() - before, row_bytes);
                        pending += 1;
                        if buffer.should_spill() {
                            prop_assert_eq!(pending, rows_per_run);
                            let written = buffer.sort_and_spill().unwrap();
                            prop_assert!(!arbitrator.record_spill_bytes("sort", written));
                            pending = 0;
                        }
                    }
                    let (sorted, residue) = buffer.finish().unwrap();
                    let rows = match sorted {
                        SortedOutput::InMemory(rows) => {
                            prop_assert!(input.len() < rows_per_run);
                            rows
                        }
                        SortedOutput::Spilled(files) => {
                            prop_assert_eq!(files.len(), input.len().div_ceil(rows_per_run));
                            prop_assert!(!arbitrator.record_spill_bytes("sort", residue));
                            let budget = MergeBudget {
                                budget: &arbitrator,
                                node: "sort",
                                compress: true,
                                charge_owner: None,
                            };
                            merge_sorted_runs(files, &sort_by, "resident versus spilled", budget)
                                .unwrap()
                        }
                    };
                    prop_assert_eq!(
                        &identities(&rows),
                        &resident,
                        "runs of {} rows changed the output",
                        rows_per_run
                    );
                }
            }
        }
    }
}
