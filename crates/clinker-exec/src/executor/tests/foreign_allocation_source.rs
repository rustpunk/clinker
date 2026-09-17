//! Public injected sources may allocate under their own finite authority.
//! The runtime must omit only backing charged to its own run ledger.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use clinker_format::FormatError;
use clinker_format::preparation::MemoryOnlyResources;
use clinker_record::owned_storage::{AllocationResources, AllocationScope, SharedStorage};
use clinker_record::{FieldStr, Record, Schema, Value};
use crossbeam_channel::{Receiver, Sender, bounded};
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(30);
const PREFILL: usize = 2;
const BURST: usize = source_stream::SourceIngestChannel::DEFAULT_CAPACITY / 2;
const ROWS: usize = PREFILL + BURST;
// Two fields must exceed both the registry's 64 KiB buffer and the CSV
// encoder's 8 KiB buffer, so the first body row reaches the blocking writer
// before the source starts its burst.
const TEXT_BYTES: usize = 40 * 1024;
// executor/mod.rs creates a 256-event output channel. With batch_size=1,
// EventBatcher::push_record flushes before appending its argument, so allow
// one writer row, one publishing row, and the next evaluated row beyond it.
const DOWNSTREAM_ROWS: usize = 256 + 1 + 1 + 1;
const MIN_QUEUED_ROWS: usize = ROWS - DOWNSTREAM_ROWS;

#[derive(Clone, Copy, Debug)]
enum Domain {
    Local,
    Foreign,
    Mixed,
}

impl Domain {
    fn local_fields(self) -> u64 {
        match self {
            Self::Local => 2,
            Self::Foreign => 0,
            Self::Mixed => 1,
        }
    }
}

struct LazySource {
    schema: SharedStorage<Schema>,
    domain: Domain,
    local: Arc<Mutex<Option<AllocationResources>>>,
    foreign: AllocationResources,
    scopes: Option<(AllocationScope, AllocationScope)>,
    escaped: Arc<Mutex<Vec<FieldStr>>>,
    produced: usize,
    prefill_sent: bool,
    prefill: Sender<()>,
    start_burst: Receiver<()>,
    queued: Sender<()>,
    finish: Receiver<()>,
    shutdown: crate::pipeline::shutdown::ShutdownToken,
}

impl RecordSource for LazySource {
    fn schema(&mut self) -> Result<SharedStorage<Schema>, FormatError> {
        Ok(self.schema.clone())
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.produced == PREFILL && !self.prefill_sent {
            self.prefill_sent = true;
            self.prefill.send(()).unwrap();
            self.start_burst.recv_timeout(DEADLINE).unwrap();
        }
        if self.produced == ROWS {
            // Reaching the next call proves every preceding row passed its
            // real SourceIngestChannel send, rather than merely being built.
            self.queued.send(()).unwrap();
            self.finish.recv_timeout(DEADLINE).unwrap();
            return Ok(None);
        }
        if self.shutdown.is_requested() {
            return Ok(None);
        }
        if self.scopes.is_none() {
            let local = self.local.lock().unwrap();
            let local = local
                .as_ref()
                .expect("public run resources precede lazy allocation");
            self.scopes = Some((local.scope().unwrap(), self.foreign.scope().unwrap()));
        }
        let (local, foreign) = self.scopes.as_ref().unwrap();
        let (a_scope, b_scope) = match self.domain {
            Domain::Local => (local, local),
            Domain::Foreign => (foreign, foreign),
            Domain::Mixed => (local, foreign),
        };
        let a = FieldStr::try_new(&"a".repeat(TEXT_BYTES), a_scope).unwrap();
        let b = FieldStr::try_new(&"b".repeat(TEXT_BYTES), b_scope).unwrap();
        if self.produced == 0 {
            let mut escaped = self.escaped.lock().unwrap();
            escaped.push(a.clone());
            escaped.push(b.clone());
        }
        let record = Record::new(
            self.schema.clone(),
            vec![
                Value::Integer(self.produced as i64),
                Value::String(a),
                Value::String(b),
            ],
        );
        self.produced += 1;
        Ok(Some(record))
    }

    fn set_shutdown_token(&mut self, token: crate::pipeline::shutdown::ShutdownToken) {
        self.shutdown = token;
    }
}

struct BlockedWriter {
    output: SharedBuffer,
    entered: Option<Sender<()>>,
    release: Receiver<()>,
}

impl Write for BlockedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Some(entered) = self.entered.take() {
            entered.send(()).unwrap();
            self.release.recv_timeout(DEADLINE).unwrap();
        }
        self.output.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}

/// Release all fixture rendezvous even when an assertion fails. A failing test
/// must not leave a source or writer blocked on the coordinator's own gates.
struct ReleaseGates {
    start: Sender<()>,
    finish: Sender<()>,
    writer: Sender<()>,
    shutdown: crate::pipeline::shutdown::ShutdownToken,
}

impl Drop for ReleaseGates {
    fn drop(&mut self) {
        self.shutdown.request();
        let _ = self.start.try_send(());
        let _ = self.finish.try_send(());
        let _ = self.writer.try_send(());
    }
}

const PIPELINE: &str = r#"
pipeline:
  name: source_allocation_domains
  batch_size: 1
  memory:
    limit: 8G
    backpressure: spill
nodes:
  - type: source
    name: ledger
    config:
      name: ledger
      type: csv
      path: placeholder.csv
      schema:
        - { name: id, type: int }
        - { name: a, type: string }
        - { name: b, type: string }
  - type: transform
    name: passthrough
    input: ledger
    config:
      cxl: |
        emit id = id
        emit a = a
        emit b = b
  - type: sink
    name: out
    input: passthrough
    config:
      name: out
      type: csv
      path: out.csv
"#;

fn public_lazy_source(domain: Domain, cancel: bool) {
    let mut config = clinker_plan::config::parse_config(PIPELINE).unwrap();
    for node in &mut config.nodes {
        if let clinker_plan::config::PipelineNode::Source { config, .. } = &mut node.value {
            config.source.path = None;
        }
    }
    let plan = config
        .compile(&clinker_plan::config::CompileContext::default())
        .unwrap();
    let foreign = MemoryOnlyResources::new(NonZeroUsize::new(64 * 1024 * 1024).unwrap());
    let local = Arc::new(Mutex::new(None));
    let escaped = Arc::new(Mutex::new(Vec::new()));
    let shutdown = crate::pipeline::shutdown::ShutdownToken::detached();
    let (startup_tx, startup_rx) = bounded(1);
    let (prefill_tx, prefill_rx) = bounded(1);
    let (start_tx, start_rx) = bounded(1);
    let (queued_tx, queued_rx) = bounded(1);
    let (finish_tx, finish_rx) = bounded(1);
    let (writer_tx, writer_rx) = bounded(1);
    let (release_tx, release_rx) = bounded(1);
    let gates = ReleaseGates {
        start: start_tx,
        finish: finish_tx,
        writer: release_tx,
        shutdown: shutdown.clone(),
    };
    let source = LazySource {
        schema: SharedStorage::from_arc(Arc::new(Schema::new(vec![
            "id".into(),
            "a".into(),
            "b".into(),
        ]))),
        domain,
        local: local.clone(),
        foreign: foreign.resources().allocation().clone(),
        scopes: None,
        escaped: escaped.clone(),
        produced: 0,
        prefill_sent: false,
        prefill: prefill_tx,
        start_burst: start_rx,
        queued: queued_tx,
        finish: finish_rx,
        shutdown: shutdown.clone(),
    };
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".into(),
        Box::new(BlockedWriter {
            output: output.clone(),
            entered: Some(writer_tx),
            release: release_rx,
        }) as Box<dyn Write + Send>,
    )]);
    let readers = HashMap::from([("ledger".into(), SourceInput::Records(Box::new(source)))]);
    let fixture_local = local.clone();
    let (completed_tx, completed_rx) = bounded(1);
    let runner = std::thread::spawn(move || {
        let _observe = RunAllocationObserverGuard::install(Box::new(move |memory, allocation| {
            assert!(
                fixture_local.lock().unwrap().is_none(),
                "startup observation occurs once"
            );
            *fixture_local.lock().unwrap() = Some(allocation.clone());
            startup_tx.send(memory.clone()).unwrap();
        }));
        let result = PipelineExecutor::run_plan_with_readers_writers(
            &plan,
            readers,
            writers,
            &PipelineRunParams {
                shutdown_token: Some(shutdown),
                ..Default::default()
            },
        );
        completed_tx.send(result).unwrap();
    });

    let memory = startup_rx
        .recv_timeout(DEADLINE)
        .expect("public run startup");
    let run_weak = Arc::downgrade(&memory);
    let managed = memory.writer_resource_observer();
    prefill_rx
        .recv_timeout(DEADLINE)
        .expect("source prefill completed");
    writer_rx
        .recv_timeout(DEADLINE)
        .expect("writer blocked before burst");
    let before_local = managed.usage().memory;
    let before_foreign = foreign.used() as u64;
    let leaf_bytes = {
        let leaves = escaped.lock().unwrap();
        assert_eq!(leaves.len(), 2);
        let resources = local.lock().unwrap();
        let resources = resources.as_ref().unwrap();
        let actual_unaccounted = leaves
            .iter()
            .map(|leaf| leaf.unaccounted_heap_size(resources) as u64)
            .sum::<u64>();
        assert_eq!(
            actual_unaccounted,
            (2 - domain.local_fields()) * leaves[0].heap_size() as u64
        );
        assert_eq!(leaves[0].heap_size(), leaves[1].heap_size());
        leaves[0].heap_size() as u64
    };
    assert!(leaf_bytes > TEXT_BYTES as u64);
    gates.start.send(()).unwrap();
    queued_rx
        .recv_timeout(DEADLINE)
        .expect("bounded source burst enqueued");
    let local_bytes = managed.usage().memory;
    let foreign_bytes = foreign.used() as u64;
    assert_eq!(
        local_bytes - before_local,
        BURST as u64 * domain.local_fields() * leaf_bytes
    );
    assert_eq!(
        foreign_bytes - before_foreign,
        BURST as u64 * (2 - domain.local_fields()) * leaf_bytes
    );
    assert_eq!(
        foreign_bytes,
        ROWS as u64 * (2 - domain.local_fields()) * leaf_bytes
    );

    let unaccounted = memory
        .sum_consumer_usage()
        .checked_sub(local_bytes)
        .unwrap();
    let queued_leaf_floor = MIN_QUEUED_ROWS as u64 * leaf_bytes;
    match domain {
        Domain::Local => assert!(
            unaccounted < queued_leaf_floor,
            "same-run leaf storage must not be charged again: {unaccounted} >= {queued_leaf_floor}"
        ),
        Domain::Foreign | Domain::Mixed => assert!(
            unaccounted >= queued_leaf_floor * (2 - domain.local_fields()),
            "queued foreign leaves must remain attributed to the run: {unaccounted}"
        ),
    }
    // This is aggregate nonmanaged attribution, not an exact Source handle
    // sample. At least MIN_QUEUED_ROWS remain in the source queue because the
    // writer is blocked and downstream has only DOWNSTREAM_ROWS row slots.
    // Direct channel tests independently pin exact EWMA/depth arithmetic.
    assert_eq!(memory.backpressureable_consumer_count(), 1);
    if cancel {
        gates.shutdown.request();
    }
    gates.finish.send(()).unwrap();
    gates.writer.send(()).unwrap();
    let report = completed_rx
        .recv_timeout(DEADLINE)
        .expect("public run cleanup")
        .unwrap();
    runner.join().unwrap();
    assert_eq!(report.interrupted, cancel);
    if !cancel {
        assert_eq!(report.counters.total_count, ROWS as u64);
        assert_eq!(report.counters.ok_count, ROWS as u64);
        let a = "a".repeat(TEXT_BYTES);
        let b = "b".repeat(TEXT_BYTES);
        let mut expected = String::from("id,a,b\n");
        for id in 0..ROWS {
            use std::fmt::Write as _;
            writeln!(&mut expected, "{id},{a},{b}").unwrap();
        }
        assert_eq!(output.as_string(), expected);
    }
    assert_eq!(memory.backpressureable_consumer_count(), 0);
    assert_eq!(managed.usage().memory, domain.local_fields() * leaf_bytes);
    assert_eq!(
        foreign.used() as u64,
        (2 - domain.local_fields()) * leaf_bytes
    );
    assert_eq!(memory.sum_consumer_usage(), managed.usage().memory);
    // The coordinator's strong observation handle intentionally kept the run
    // alive. Drop it and the weak allocation capability before testing true
    // run closure; escaped leaves retain release state, never live control.
    drop(local.lock().unwrap().take());
    drop(memory);
    assert!(run_weak.upgrade().is_none());
    assert!(managed.is_closed());
    assert_eq!(managed.usage().memory, domain.local_fields() * leaf_bytes);
    escaped.lock().unwrap().clear();
    assert_eq!(managed.usage().memory, 0);
    assert_eq!(foreign.used(), 0);
}

#[test]
fn public_lazy_source_accounts_local_foreign_and_mixed_ledgers_while_writer_is_blocked() {
    for domain in [Domain::Local, Domain::Foreign, Domain::Mixed] {
        public_lazy_source(domain, false);
    }
}

#[test]
fn public_lazy_foreign_source_cancellation_releases_queues_but_preserves_escaped_leaves() {
    for domain in [Domain::Foreign, Domain::Mixed] {
        public_lazy_source(domain, true);
    }
}
