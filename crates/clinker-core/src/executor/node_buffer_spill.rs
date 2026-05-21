//! Producer-side spill helper for `ctx.node_buffers`.
//!
//! When `MemoryArbitrator::should_spill()` trips at admission time, the
//! producer flushes the in-memory `Vec<(Record, u64)>` to disk through
//! `SpillWriter<u64>` and stores the resulting `(SpillFile<u64>, u64)`
//! pair inside `NodeBuffer::Spilled`. Consumer-side streaming is
//! already covered by [`NodeBuffer::drain`] (memory rows first, then
//! per-spill rows via `SpillReader<u64>`), so this module exposes only
//! the producer-side packaging.
//!
//! Goes through the same generic `pipeline/spill.rs` envelope (JSON
//! schema header + length-prefixed postcard + LZ4 frame) that
//! `sort_buffer.rs` writes today. `pipeline/grace_spill.rs` is a
//! distinct, partition-aware format reserved for grace-hash combine
//! and is intentionally not used here.
//!
//! Payload type `u64` carries the source row number alongside each
//! record so the original lineage survives the round-trip — the
//! consumer's drain re-emits the same `(Record, u64)` pairs the
//! producer wrote.

use std::path::Path;
use std::sync::Arc;

use clinker_record::Record;

use crate::error::PipelineError;
use crate::pipeline::spill::{SpillFile, SpillWriter};

/// Spill a `Vec<(Record, u64)>` to disk and return the resulting
/// `(SpillFile<u64>, row_count)` pair. Schema is read from the first
/// record; an empty input is a no-op (`Ok(None)`).
///
/// Errors from `SpillWriter::new` (temp-file creation), `write_pair`
/// (postcard encode + LZ4 write), and `finish` (frame finalize) all
/// surface as `PipelineError::Spill` via the existing
/// `From<SpillError>` conversion.
pub(crate) fn spill_node_buffer(
    rows: Vec<(Record, u64)>,
    spill_dir: Option<&Path>,
) -> Result<Option<(SpillFile<u64>, u64)>, PipelineError> {
    let Some((first, _)) = rows.first() else {
        return Ok(None);
    };
    let schema = Arc::clone(first.schema());
    let mut writer: SpillWriter<u64> = SpillWriter::new(schema, spill_dir)?;
    let count = rows.len() as u64;
    for (record, rn) in &rows {
        writer.write_pair(record, rn)?;
    }
    let file = writer.finish()?;
    Ok(Some((file, count)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{Schema, Value};

    use crate::executor::node_buffer::NodeBuffer;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["id".into(), "v".into()]))
    }

    fn rec(s: &Arc<Schema>, id: i64, v: &str) -> Record {
        Record::new(
            Arc::clone(s),
            vec![Value::Integer(id), Value::String(v.into())],
        )
    }

    #[test]
    fn spill_node_buffer_roundtrip_preserves_rows_and_row_numbers() {
        let s = schema();
        let rows = vec![
            (rec(&s, 1, "a"), 100),
            (rec(&s, 2, "b"), 101),
            (rec(&s, 3, "c"), 102),
        ];

        let (file, count) = spill_node_buffer(rows.clone(), None)
            .expect("spill ok")
            .expect("non-empty input produces a chunk");
        assert_eq!(count, 3);

        let nb = NodeBuffer::Spilled(vec![(file, count)]);
        let drained: Vec<(Record, u64)> = nb.drain().collect::<Result<_, _>>().expect("drain ok");
        assert_eq!(drained.len(), 3);
        for (i, (orig, d)) in rows.iter().zip(drained.iter()).enumerate() {
            assert_eq!(orig.1, d.1, "row_number mismatch at {i}");
            assert_eq!(
                orig.0.values(),
                d.0.values(),
                "record values mismatch at {i}",
            );
        }
    }

    #[test]
    fn spill_node_buffer_empty_input_is_none() {
        let result = spill_node_buffer(Vec::new(), None).expect("spill ok");
        assert!(result.is_none(), "empty input must not create a spill file");
    }

    #[test]
    fn multiple_spill_chunks_drain_in_order() {
        let s = schema();
        let chunk_a = spill_node_buffer(vec![(rec(&s, 1, "a"), 10), (rec(&s, 2, "b"), 11)], None)
            .unwrap()
            .unwrap();
        let chunk_b = spill_node_buffer(vec![(rec(&s, 3, "c"), 12)], None)
            .unwrap()
            .unwrap();
        let chunk_c = spill_node_buffer(vec![(rec(&s, 4, "d"), 13), (rec(&s, 5, "e"), 14)], None)
            .unwrap()
            .unwrap();

        let nb = NodeBuffer::Spilled(vec![chunk_a, chunk_b, chunk_c]);
        let drained: Vec<(Record, u64)> = nb.drain().collect::<Result<_, _>>().expect("drain ok");

        assert_eq!(drained.len(), 5);
        let row_numbers: Vec<u64> = drained.iter().map(|(_, rn)| *rn).collect();
        assert_eq!(row_numbers, vec![10, 11, 12, 13, 14]);
    }

    #[test]
    fn spilled_variant_len_hint_matches_written_rows() {
        let s = schema();
        let rows = vec![(rec(&s, 1, "a"), 1), (rec(&s, 2, "b"), 2)];
        let (file, count) = spill_node_buffer(rows, None).unwrap().unwrap();

        let nb = NodeBuffer::Spilled(vec![(file, count)]);
        assert_eq!(nb.len_hint(), 2);
    }
}
