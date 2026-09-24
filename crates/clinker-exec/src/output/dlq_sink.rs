//! The staged-file dead-letter sink: each bucket that receives a row gets
//! one `ArtifactKind::Dlq` file in the run's publication attempt, staged on
//! its first row and written through a fixed buffer.
//!
//! The file is staged, never published, here: a failed or interrupted run
//! promotes nothing, and publication stays with the owner of the attempt.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use clinker_plan::config::IfExistsPolicy;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::dlq_layout::DlqBucketId;

use super::attempt::ArtifactKind;
use super::staging::OutputStagingRegistry;
use crate::dlq::{DlqArtifact, DlqBucketTarget, DlqRowWriter, DlqSink};
use crate::pipeline::shutdown::ShutdownToken;
use crate::telemetry::{DeadLetterSignal, TelemetryProducer};

/// Capacity of each open bucket's write buffer. One buffer per open bucket
/// per writer, and the bucket count is fixed by the compiled plan, so the
/// sink's residency is a constant independent of how many rows fail.
pub(crate) const DLQ_WRITE_BUFFER_BYTES: usize = 64 * 1024;

/// The producer label every dead-letter artifact is staged under; it names
/// the producer in collision diagnostics.
const DLQ_PRODUCER_LABEL: &str = "dead-letter output";

/// A [`DlqSink`] that streams each bucket's rows into a staged
/// `ArtifactKind::Dlq` file of the run's publication attempt.
///
/// A bucket's file is staged on the bucket's first row, so a bucket nothing
/// failed into stages no artifact. Every staged file is admitted through
/// [`OutputStagingRegistry::stage_attempt_output`], which confines it to the
/// compiled destination roots and checks it against the run's collision
/// ledger. The sink holds no rows: writers stream through fixed buffers, and
/// the sink keeps only one file handle per closed bucket until
/// [`DlqSink::finish`].
///
/// With a telemetry producer, each staged bucket file is one dead-letter work
/// unit: started when the file is staged, completed by `finish`, failed on a
/// write or flush error, and, when the file is abandoned without `finish`,
/// interrupted if the run's shutdown token is requested and failed otherwise.
pub struct StagedDlqSink {
    staging: OutputStagingRegistry,
    shared: Arc<Mutex<SinkState>>,
    units: UnitConfig,
}

/// What every bucket file of this sink is opened with.
#[derive(Clone, Default)]
struct UnitConfig {
    telemetry: Option<TelemetryProducer>,
    shutdown: Option<ShutdownToken>,
    #[cfg(feature = "test-utils")]
    fault: Option<WriteFault>,
}

/// State shared between the sink and its writers. Touched when a writer
/// opens or closes and at `finish`, never per row.
#[derive(Default)]
struct SinkState {
    open_writers: usize,
    closed: Vec<ClosedBucket>,
    finished: bool,
}

/// A bucket file a closed writer handed back, flushed. Dropped without
/// `finish`, its work unit reports the file abandoned.
struct ClosedBucket {
    id: DlqBucketId,
    final_path: PathBuf,
    file: File,
    rows: u64,
    unit: BucketUnit,
}

/// One bucket file's telemetry work unit. Dropped before it is completed or
/// failed, it reports the file abandoned: interrupted when the shutdown
/// token is requested, failed otherwise.
struct BucketUnit {
    signal: Option<DeadLetterSignal>,
    shutdown: Option<ShutdownToken>,
}

impl BucketUnit {
    fn start(units: &UnitConfig, id: DlqBucketId) -> Self {
        Self {
            signal: units
                .telemetry
                .clone()
                .map(|producer| DeadLetterSignal::new(producer, id.index())),
            shutdown: units.shutdown.clone(),
        }
    }

    fn record(&mut self, rows: u64, bytes: u64) {
        if let Some(signal) = &mut self.signal {
            signal.record_records(rows);
            signal.record_bytes(bytes);
        }
    }

    fn complete(mut self) {
        if let Some(signal) = self.signal.take() {
            signal.complete();
        }
    }

    fn fail(mut self) {
        if let Some(signal) = self.signal.take() {
            signal.fail();
        }
    }
}

impl Drop for BucketUnit {
    fn drop(&mut self) {
        let Some(signal) = self.signal.take() else {
            return;
        };
        if self
            .shutdown
            .as_ref()
            .is_some_and(ShutdownToken::is_requested)
        {
            signal.interrupt();
        } else {
            signal.fail();
        }
    }
}

/// A write fault injected by [`StagedDlqSink::with_write_fault_for_testing`].
#[cfg(feature = "test-utils")]
#[derive(Clone, Copy)]
struct WriteFault {
    after_bytes: u64,
    kind: std::io::ErrorKind,
}

/// A bucket's staged file behind its write buffer.
struct BucketFile {
    file: File,
    #[cfg(feature = "test-utils")]
    fault: Option<WriteFault>,
    #[cfg(feature = "test-utils")]
    accepted: u64,
}

impl Write for BucketFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        #[cfg(feature = "test-utils")]
        if let Some(fault) = self.fault {
            let room = fault.after_bytes.saturating_sub(self.accepted);
            if room == 0 {
                return Err(std::io::Error::new(
                    fault.kind,
                    "injected dead-letter write fault",
                ));
            }
            let take = usize::try_from(room).map_or(buf.len(), |room| room.min(buf.len()));
            let written = self.file.write(&buf[..take])?;
            self.accepted += written as u64;
            return Ok(written);
        }
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl StagedDlqSink {
    /// A sink staging into `staging`, which must be attached to a run
    /// attempt ([`OutputStagingRegistry::for_run_attempt`]); without one, the
    /// first row fails with [`PipelineError::Internal`].
    ///
    /// `telemetry`, when supplied, receives one dead-letter work unit per
    /// staged bucket file.
    pub fn new(staging: OutputStagingRegistry, telemetry: Option<TelemetryProducer>) -> Self {
        Self {
            staging,
            shared: Arc::new(Mutex::new(SinkState::default())),
            units: UnitConfig {
                telemetry,
                ..UnitConfig::default()
            },
        }
    }

    /// Report a bucket file abandoned while `token` is requested as
    /// interrupted rather than failed. Takes effect for files staged after
    /// this call, so it is set before the sink is handed to a run.
    #[must_use]
    pub fn with_shutdown_token(mut self, token: ShutdownToken) -> Self {
        self.units.shutdown = Some(token);
        self
    }

    /// Fail each bucket file's writes with `kind` once the file has accepted
    /// `after_bytes` bytes, as a destination that fills up does.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_write_fault_for_testing(
        mut self,
        after_bytes: u64,
        kind: std::io::ErrorKind,
    ) -> Self {
        self.units.fault = Some(WriteFault { after_bytes, kind });
        self
    }

    fn state(&self) -> MutexGuard<'_, SinkState> {
        lock(&self.shared)
    }

    fn walk_writer(&self) -> StagedDlqRowWriter {
        self.state().open_writers += 1;
        StagedDlqRowWriter {
            staging: self.staging.clone(),
            shared: Arc::clone(&self.shared),
            units: self.units.clone(),
            buckets: Vec::new(),
            failed: None,
        }
    }
}

impl DlqSink for StagedDlqSink {
    fn open_walk_writer(&self) -> Result<Box<dyn DlqRowWriter>, PipelineError> {
        Ok(Box::new(self.walk_writer()))
    }

    fn finish(&self) -> Result<Vec<DlqArtifact>, PipelineError> {
        let mut state = self.state();
        if state.finished {
            return Err(sink_invariant("finish was called twice"));
        }
        if state.open_writers != 0 {
            return Err(sink_invariant(format!(
                "finish was called while {} writer(s) had not closed, so their rows are not all written",
                state.open_writers
            )));
        }
        state.finished = true;
        let mut closed = std::mem::take(&mut state.closed);
        drop(state);
        closed.sort_by_key(|bucket| bucket.id);
        if let Some(pair) = closed.windows(2).find(|pair| pair[0].id == pair[1].id) {
            return Err(sink_invariant(format!(
                "bucket {} was written by more than one writer",
                pair[0].final_path.display()
            )));
        }
        Ok(closed
            .into_iter()
            .map(|bucket| {
                drop(bucket.file);
                bucket.unit.complete();
                DlqArtifact {
                    bucket: bucket.id,
                    final_path: bucket.final_path,
                    rows: bucket.rows,
                }
            })
            .collect())
    }
}

/// The walk thread's writer of a [`StagedDlqSink`]: one open bucket per
/// slot, indexed by bucket, each behind a [`DLQ_WRITE_BUFFER_BYTES`] buffer.
/// After its first I/O failure it refuses every later row and its close
/// with the same error kind.
struct StagedDlqRowWriter {
    staging: OutputStagingRegistry,
    shared: Arc<Mutex<SinkState>>,
    units: UnitConfig,
    buckets: Vec<Option<OpenBucket>>,
    /// Set by the first write that fails. The failed file is incomplete, so
    /// every later row and the close are refused with the same kind rather
    /// than restaging the bucket or reporting it written.
    failed: Option<std::io::ErrorKind>,
}

struct OpenBucket {
    id: DlqBucketId,
    final_path: PathBuf,
    out: BufWriter<BucketFile>,
    rows: u64,
    unit: BucketUnit,
}

/// Stage `target`'s file through `staging` and write its header.
fn open_bucket(
    staging: &OutputStagingRegistry,
    units: &UnitConfig,
    target: &DlqBucketTarget<'_>,
) -> Result<OpenBucket, PipelineError> {
    let bare = target.path.to_path_buf();
    let (final_path, file) = staging.stage_attempt_output(
        ArtifactKind::Dlq,
        DLQ_PRODUCER_LABEL,
        IfExistsPolicy::Overwrite,
        false,
        move |n| {
            debug_assert!(n.is_none(), "overwrite stages the authored path only");
            Ok(bare.clone())
        },
    )?;
    let mut unit = BucketUnit::start(units, target.id);
    let file = BucketFile {
        file,
        #[cfg(feature = "test-utils")]
        fault: units.fault,
        #[cfg(feature = "test-utils")]
        accepted: 0,
    };
    let mut out = BufWriter::with_capacity(DLQ_WRITE_BUFFER_BYTES, file);
    if let Err(error) = out.write_all(target.header) {
        unit.fail();
        return Err(PipelineError::Io(error));
    }
    unit.record(0, target.header.len() as u64);
    Ok(OpenBucket {
        id: target.id,
        final_path,
        out,
        rows: 0,
        unit,
    })
}

impl DlqRowWriter for StagedDlqRowWriter {
    fn write_row(&mut self, target: &DlqBucketTarget<'_>, row: &[u8]) -> Result<(), PipelineError> {
        if let Some(kind) = self.failed {
            return Err(earlier_write_failed(kind));
        }
        let index = target.id.index();
        if self.buckets.len() <= index {
            self.buckets.resize_with(index + 1, || None);
        }
        let bucket = match &mut self.buckets[index] {
            Some(open) => open,
            slot @ None => match open_bucket(&self.staging, &self.units, target) {
                Ok(open) => slot.insert(open),
                Err(error) => {
                    if let PipelineError::Io(io) = &error {
                        self.failed = Some(io.kind());
                    }
                    return Err(error);
                }
            },
        };
        if let Err(error) = bucket.out.write_all(row) {
            // The file is incomplete from here on: close its unit as failed
            // and drop it, so nothing can report it written.
            if let Some(failed) = self.buckets[index].take() {
                failed.unit.fail();
            }
            self.failed = Some(error.kind());
            return Err(PipelineError::Io(error));
        }
        bucket.rows += 1;
        bucket.unit.record(1, row.len() as u64);
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<(), PipelineError> {
        let Self {
            shared,
            buckets,
            failed,
            ..
        } = *self;
        if let Some(kind) = failed {
            return Err(earlier_write_failed(kind));
        }
        let mut closed = Vec::with_capacity(buckets.iter().flatten().count());
        for bucket in buckets.into_iter().flatten() {
            let OpenBucket {
                id,
                final_path,
                out,
                rows,
                unit,
            } = bucket;
            let file = match out.into_inner() {
                Ok(file) => file.file,
                Err(error) => {
                    unit.fail();
                    return Err(PipelineError::Io(error.into_error()));
                }
            };
            closed.push(ClosedBucket {
                id,
                final_path,
                file,
                rows,
                unit,
            });
        }
        let mut state = lock(&shared);
        state.open_writers -= 1;
        state.closed.extend(closed);
        Ok(())
    }
}

/// The error for a row or close after this writer's first failed write.
fn earlier_write_failed(kind: std::io::ErrorKind) -> PipelineError {
    PipelineError::Io(std::io::Error::new(
        kind,
        "an earlier write to this dead-letter output failed",
    ))
}

fn lock(shared: &Mutex<SinkState>) -> MutexGuard<'_, SinkState> {
    shared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn sink_invariant(detail: impl Into<String>) -> PipelineError {
    PipelineError::Internal {
        op: "dead-letter",
        node: DLQ_PRODUCER_LABEL.to_owned(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use clinker_plan::config::{ClinkerToml, CompileContext, parse_config};
    use clinker_plan::plan::dlq_layout::DlqBucketId;
    use clinker_plan::security::validate_path;

    use super::*;
    use crate::dlq::DiscardingDlqSink;
    use crate::output::attempt::RunAttemptPublication;

    const HEADER: &[u8] = b"_cxl_dlq_id,id\n";

    /// A staging registry attached to a fresh run attempt whose only
    /// destination root is `root`.
    fn attempt_staging(root: &Path) -> OutputStagingRegistry {
        let policy = ClinkerToml::parse(
            "[storage.publication]\nfailed_retention_seconds = 300\nmax_attempt_bytes = \"1MB\"\n",
        )
        .expect("parse publication policy")
        .storage
        .publication
        .resolve(root, 1_024, 8_000_000_000)
        .expect("resolve publication policy");
        let now: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis()
            .try_into()
            .expect("milliseconds fit u64");
        let attempt = RunAttemptPublication::create(
            policy,
            &uuid::Uuid::now_v7().to_string(),
            now,
            now + 300_000,
            vec![validate_path(Path::new("."), root, false).expect("destination root")],
        )
        .expect("create run attempt");
        OutputStagingRegistry::for_run_attempt(attempt)
    }

    /// The two bucket ids a compiled plan assigns when `src_b` has its own
    /// dead-letter path and `src_a` falls through to the pipeline-wide one,
    /// in bucket order. Ids come from the compiler, never made up.
    fn two_bucket_ids() -> (DlqBucketId, DlqBucketId) {
        let plan = parse_config(
            "pipeline:\n  name: dlq_sink\nerror_handling:\n  strategy: continue\n  dlq:\n    path: dlq.csv\n    per_source:\n      src_b:\n        path: dlq_b.csv\n\
nodes:\n- type: source\n  name: src_a\n  config:\n    name: src_a\n    type: csv\n    path: a.csv\n    schema:\n      - { name: id, type: string }\n\
- type: source\n  name: src_b\n  config:\n    name: src_b\n    type: csv\n    path: b.csv\n    schema:\n      - { name: id, type: string }\n\
- type: merge\n  name: m\n  inputs: [src_a, src_b]\n\
- type: sink\n  name: out\n  input: m\n  config:\n    name: out\n    type: csv\n    path: out.csv\n",
        )
        .expect("pipeline parses")
        .compile(&CompileContext::default())
        .expect("pipeline compiles");
        let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
        let ids: Vec<DlqBucketId> = layout.iter().map(|(id, _)| id).collect();
        assert_eq!(ids.len(), 2, "one pipeline-wide and one per-source bucket");
        (ids[0], ids[1])
    }

    fn target(id: DlqBucketId, path: &Path) -> DlqBucketTarget<'_> {
        DlqBucketTarget {
            id,
            path,
            header: HEADER,
        }
    }

    /// The staged bytes of the artifact whose final path is `final_path`.
    fn staged_bytes(staging: &OutputStagingRegistry, final_path: &Path) -> Vec<u8> {
        let partial = staging
            .partials()
            .into_iter()
            .find(|partial| partial.final_path == final_path)
            .expect("bucket was staged");
        std::fs::read(&partial.partial_path).expect("read staged bucket")
    }

    #[test]
    fn staged_sink_writes_header_once_then_rows() {
        let root = tempfile::tempdir().expect("destination root");
        let staging = attempt_staging(root.path());
        let (id, _) = two_bucket_ids();
        let path = root.path().join("dlq.csv");
        let sink = StagedDlqSink::new(staging.clone(), None);

        let mut writer = sink.open_walk_writer().expect("open writer");
        writer
            .write_row(&target(id, &path), b"1,a\n")
            .expect("first row");
        writer
            .write_row(&target(id, &path), b"2,b\n")
            .expect("second row");
        writer.close().expect("close writer");
        let artifacts = sink.finish().expect("finish");

        assert_eq!(artifacts.len(), 1);
        assert_eq!(
            staged_bytes(&staging, &path),
            b"_cxl_dlq_id,id\n1,a\n2,b\n",
            "the header is written once, before the first row"
        );
        assert!(
            !path.exists(),
            "the sink stages; it never publishes the final path"
        );
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn failed_write_refuses_later_rows_and_close() {
        let root = tempfile::tempdir().expect("destination root");
        let staging = attempt_staging(root.path());
        let (id, other) = two_bucket_ids();
        let path = root.path().join("dlq.csv");
        let other_path = root.path().join("dlq_b.csv");
        let sink = StagedDlqSink::new(staging, None)
            .with_write_fault_for_testing(16, std::io::ErrorKind::StorageFull);

        let mut writer = sink.open_walk_writer().expect("open writer");
        // Larger than the write buffer, so it reaches the file and fails.
        let row = vec![b'x'; DLQ_WRITE_BUFFER_BYTES + 1];
        let first = writer
            .write_row(&target(id, &path), &row)
            .expect_err("the destination is full");
        assert!(
            matches!(&first, PipelineError::Io(io) if io.kind() == std::io::ErrorKind::StorageFull),
            "{first:?}"
        );
        for later in [
            writer.write_row(&target(id, &path), b"1,a\n"),
            writer.write_row(&target(other, &other_path), b"1,a\n"),
        ] {
            assert!(
                matches!(&later, Err(PipelineError::Io(io)) if io.kind() == std::io::ErrorKind::StorageFull),
                "a later row is refused, never restaged: {later:?}"
            );
        }
        let closed = writer
            .close()
            .expect_err("close refuses an incomplete file");
        assert!(
            matches!(&closed, PipelineError::Io(io) if io.kind() == std::io::ErrorKind::StorageFull),
            "{closed:?}"
        );
        assert!(
            sink.finish().is_err(),
            "finish cannot report a bucket whose writer never closed"
        );
    }

    #[test]
    fn staged_sink_opens_no_file_for_an_unwritten_bucket() {
        let root = tempfile::tempdir().expect("destination root");
        let staging = attempt_staging(root.path());
        let (wide, own) = two_bucket_ids();
        let wide_path = root.path().join("dlq.csv");
        let own_path = root.path().join("dlq_b.csv");
        let sink = StagedDlqSink::new(staging.clone(), None);

        let mut writer = sink.open_walk_writer().expect("open writer");
        writer
            .write_row(&target(own, &own_path), b"1,a\n")
            .expect("row");
        writer.close().expect("close writer");
        let artifacts = sink.finish().expect("finish");

        let staged: Vec<PathBuf> = staging
            .partials()
            .into_iter()
            .map(|partial| partial.final_path)
            .collect();
        assert_eq!(staged, [own_path], "only the written bucket is staged");
        assert!(artifacts.iter().all(|artifact| artifact.bucket != wide));
        assert!(!wide_path.exists());
    }

    #[test]
    fn staged_sink_buffers_at_least_64_kib() {
        let root = tempfile::tempdir().expect("destination root");
        let staging = attempt_staging(root.path());
        let (id, _) = two_bucket_ids();
        let path = root.path().join("dlq.csv");
        let sink = StagedDlqSink::new(staging, None);

        let mut writer = sink.walk_writer();
        writer.write_row(&target(id, &path), b"1,a\n").expect("row");
        let capacity = writer
            .buckets
            .get(id.index())
            .and_then(Option::as_ref)
            .map(|bucket| bucket.out.capacity());
        assert!(
            capacity.is_some_and(|capacity| capacity >= 65_536),
            "the open bucket's buffer holds at least 64 KiB, got {capacity:?}"
        );
        Box::new(writer).close().expect("close writer");
    }

    #[test]
    fn discarding_sink_accepts_rows_and_stages_nothing() {
        let root = tempfile::tempdir().expect("destination root");
        let (wide, own) = two_bucket_ids();
        let wide_path = root.path().join("dlq.csv");
        let own_path = root.path().join("dlq_b.csv");
        let sink = DiscardingDlqSink;

        let mut writer = sink.open_walk_writer().expect("open writer");
        for _ in 0..3 {
            writer
                .write_row(&target(wide, &wide_path), b"1,a\n")
                .expect("row accepted");
            writer
                .write_row(&target(own, &own_path), b"2,b\n")
                .expect("row accepted");
        }
        writer.close().expect("close writer");

        assert_eq!(sink.finish().expect("finish"), []);
        assert_eq!(
            std::fs::read_dir(root.path())
                .expect("read destination root")
                .count(),
            0,
            "nothing is written under the destination"
        );
    }

    #[test]
    fn finish_returns_one_artifact_per_written_bucket() {
        let root = tempfile::tempdir().expect("destination root");
        let staging = attempt_staging(root.path());
        let (wide, own) = two_bucket_ids();
        let wide_path = root.path().join("dlq.csv");
        let own_path = root.path().join("dlq_b.csv");
        let sink = StagedDlqSink::new(staging.clone(), None);

        // The second bucket is opened first, so bucket order is not the
        // order the writer met them in.
        let mut writer = sink.open_walk_writer().expect("open writer");
        for row in [b"1,a\n", b"2,b\n", b"3,c\n"] {
            writer
                .write_row(&target(own, &own_path), row)
                .expect("per-source row");
        }
        for row in [b"4,d\n", b"5,e\n"] {
            writer
                .write_row(&target(wide, &wide_path), row)
                .expect("pipeline-wide row");
        }
        writer.close().expect("close writer");

        assert_eq!(
            sink.finish().expect("finish"),
            [
                DlqArtifact {
                    bucket: wide,
                    final_path: wide_path.clone(),
                    rows: 2,
                },
                DlqArtifact {
                    bucket: own,
                    final_path: own_path.clone(),
                    rows: 3,
                },
            ]
        );
        assert_eq!(
            staged_bytes(&staging, &wide_path),
            b"_cxl_dlq_id,id\n4,d\n5,e\n"
        );
        assert_eq!(
            staged_bytes(&staging, &own_path),
            b"_cxl_dlq_id,id\n1,a\n2,b\n3,c\n"
        );
        assert!(
            sink.finish().is_err(),
            "a second finish cannot report an artifact set"
        );
    }
}
