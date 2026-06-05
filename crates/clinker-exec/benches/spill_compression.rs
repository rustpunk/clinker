//! Spill write+drain throughput across batch sizes for each compression mode.
//!
//! Measures the round-trip cost (postcard encode + optional LZ4 frame, then
//! decode) of one spilled batch through `SpillWriter` / `SpillReader` at a
//! range of per-batch byte sizes. The issue motivating the `compress` knob is
//! that LZ4's per-frame fixed cost dominates on small batches; this bench
//! validates that empirically by sweeping `compress = on` against `off` so the
//! crossover where compression starts paying off is observable, and confirms
//! `auto` tracks the better of the two across the range.
//!
//! Batch sizes target ~256 B, 1 KiB, 4 KiB, 16 KiB, and 64 KiB of payload —
//! the threshold the `auto` heuristic keys on lives at 4 KiB.

use clinker_bench_support::RecordFactory;
use clinker_exec::pipeline::spill::{SpillReader, SpillWriter};
use clinker_plan::config::CompressMode;
use clinker_record::Record;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

/// Approximate on-the-wire bytes one generated record contributes: each field
/// is a `string_len`-byte string plus a few bytes of postcard framing. Used
/// only to size each batch to a target byte budget; the bench measures the
/// real spilled file, not this estimate.
const APPROX_BYTES_PER_RECORD: usize = 24;

/// Target per-batch payload sizes the bench sweeps. 4 KiB is the `auto`
/// heuristic's byte threshold, so the sweep brackets it on both sides.
const TARGET_BATCH_BYTES: [usize; 5] = [256, 1024, 4096, 16 * 1024, 64 * 1024];

/// Generate one batch of records sized to roughly `target_bytes` of payload.
fn make_batch(target_bytes: usize) -> Vec<Record> {
    let row_count = (target_bytes / APPROX_BYTES_PER_RECORD).max(1);
    // One narrow string field keeps per-record bytes predictable so the batch
    // lands near its target size.
    let mut factory = RecordFactory::new(1, 16, 0.0, 42);
    factory.generate(row_count)
}

/// Write the batch to a spill file in `compress` mode, then drain it back,
/// returning the drained row count so the optimizer cannot elide the work.
fn spill_and_drain(records: &[Record], compress: bool) -> usize {
    let schema = records[0].schema().clone();
    let mut writer: SpillWriter<()> =
        SpillWriter::new(schema, None, compress).expect("open spill writer");
    for record in records {
        writer.write_record(record).expect("spill write");
    }
    let file = writer.finish().expect("finish spill");
    let reader: SpillReader<()> = file.reader().expect("open spill reader");
    let mut drained = 0usize;
    for item in reader {
        item.expect("spill read");
        drained += 1;
    }
    drained
}

fn bench_spill_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("spill_compression");

    for target in TARGET_BATCH_BYTES {
        let batch = make_batch(target);
        group.throughput(Throughput::Bytes(target as u64));

        // `auto` resolves once per batch from the schema width and row count,
        // matching the runtime decision; `on` / `off` force the frame choice.
        for mode in [CompressMode::On, CompressMode::Off, CompressMode::Auto] {
            let compress =
                mode.resolve_for_schema(batch[0].schema().column_count(), batch.len() as u64);
            let label = format!("{mode:?}");
            group.bench_with_input(
                BenchmarkId::new(label, target),
                &compress,
                |b, &compress| {
                    b.iter(|| black_box(spill_and_drain(&batch, compress)));
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_spill_compression);
criterion_main!(benches);
