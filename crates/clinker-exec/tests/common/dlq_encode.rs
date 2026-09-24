//! Encode a run's dead-letter entries the way `clinker run` publishes them:
//! routed by the compiled plan's dead-letter layout and written under the
//! header it fixed for the bucket, never a header scanned from the entries.

use clinker_exec::dlq::DlqRowEncoder;
use clinker_exec::executor::DlqEntry;
use clinker_plan::plan::CompiledPlan;

/// The CSV text of the one dead-letter file every entry in `entries` routes
/// to under `plan`'s layout: the compiled header line, then one row per
/// entry in order.
///
/// Panics when the plan has no DLQ block, when the entries route to more
/// than one bucket or to none, or when the encoder refuses a row as outside
/// the compiled header: each is a fixture or planner defect the calling test
/// must surface rather than read past.
pub fn dlq_csv(plan: &CompiledPlan, entries: &[DlqEntry]) -> String {
    let layout = plan
        .dlq_layout()
        .expect("the pipeline declares an error_handling.dlq block");
    let mut routed = entries.iter().map(|entry| {
        layout
            .bucket_for_source(&entry.source_name)
            .expect("every entry has a dead-letter bucket")
    });
    let id = routed.next().expect("at least one dead-letter entry");
    assert!(
        routed.all(|other| other == id),
        "every entry routes to one dead-letter file"
    );
    let bucket = layout.bucket(id);
    let mut encoder = DlqRowEncoder::new();
    let mut bytes = encoder.header(bucket).expect("encode header").to_vec();
    for entry in entries {
        bytes.extend_from_slice(
            encoder
                .row(layout, bucket, entry)
                .expect("the row fits the compiled dead-letter header"),
        );
    }
    String::from_utf8(bytes).expect("dead-letter CSV is UTF-8")
}
