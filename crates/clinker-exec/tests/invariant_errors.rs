#![cfg(feature = "test-utils")]

use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use clinker_core_types::{FailureCategory, RetryAdvice};
use clinker_exec::executor::{
    DispatchFaultGuard, PipelineExecutor, PipelineRunParams, SourceReaders, WriterRegistry,
    single_file_reader,
};
use clinker_exec::output::attempt::{
    ArtifactKind, ArtifactState, AttemptQuery, AttemptState, CleanupDebtKind, RunAttemptPublication,
};
use clinker_exec::output::staging::OutputStagingRegistry;
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_plan::config::{
    ClinkerToml, CompileContext, IfExistsPolicy, ResolvedPublicationPolicy, load_config_from_str,
};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};
use clinker_plan::security::{ValidatedPath, validate_path};
use fs4::FileExt;
use petgraph::graph::NodeIndex;
use serial_test::serial;

const EXECUTION_ID: &str = "018f47a2-9a41-7a27-b4d6-4f7137e3c159";
const CONTROL_EXECUTION_ID: &str = "018f47a2-9a41-7a27-b4d6-4f7137e3c160";
const PIPELINE_YAML: &str = r#"
pipeline:
  name: invariant_dispatch_mismatch
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
        - { name: label, type: string }
  - type: transform
    name: rename
    input: src
    config:
      cxl: |
        emit id = id
        emit renamed = label
  - type: output
    name: out
    input: rename
    config:
      name: out
      type: csv
      path: output.csv
      include_unmapped: false
"#;

struct DropTrackedReader<R> {
    inner: R,
    dropped: Arc<AtomicBool>,
}

impl<R: Read> Read for DropTrackedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl<R> Drop for DropTrackedReader<R> {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

fn validated(root: &Path, relative: &str) -> ValidatedPath {
    validate_path(Path::new(relative), root, false).expect("fixture path should validate")
}

fn publication_policy(destination_root: &Path) -> ResolvedPublicationPolicy {
    ClinkerToml::parse(
        "[storage.publication]\nfailed_retention_seconds = 300\nmax_attempt_bytes = \"1MB\"\n",
    )
    .expect("parse publication policy")
    .storage
    .publication
    .resolve(destination_root, 1_024, 8_000_000_000)
    .expect("resolve publication policy")
}

fn readers(dropped: Arc<AtomicBool>) -> SourceReaders {
    let reader = DropTrackedReader {
        inner: Cursor::new(b"id,label\n1,alpha\n2,beta\n".to_vec()),
        dropped,
    };
    HashMap::from([(
        "src".to_owned(),
        single_file_reader("input.csv", Box::new(reader)),
    )])
}

fn spill_entries(root: &Path) -> Vec<String> {
    std::fs::read_dir(root)
        .expect("read spill root")
        .map(|entry| {
            entry
                .expect("read spill entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.starts_with("clinker-spill-"))
        .collect()
}

type DispatchMismatchProbe =
    fn(&ExecutionPlanDag, NodeIndex, &PlanNode) -> Result<(), PipelineError>;

struct DispatchMismatchCase {
    dispatcher: &'static str,
    expected_kind: &'static str,
    actual_kind: &'static str,
    node_name: &'static str,
    invoke: DispatchMismatchProbe,
}

#[test]
fn aggregate_and_source_transform_and_composition_dispatch_mismatch_matrix() {
    let plan = load_config_from_str(PIPELINE_YAML)
        .expect("parse pipeline")
        .compile(&CompileContext::default())
        .expect("compile pipeline");
    let dag = plan.dag();
    let cases = [
        DispatchMismatchCase {
            dispatcher: "dispatch_aggregation",
            expected_kind: "aggregation",
            actual_kind: "transform",
            node_name: "rename",
            invoke: DispatchFaultGuard::dispatch_aggregation_mismatch_for_testing,
        },
        DispatchMismatchCase {
            dispatcher: "dispatch_source",
            expected_kind: "source",
            actual_kind: "transform",
            node_name: "rename",
            invoke: DispatchFaultGuard::dispatch_source_mismatch_for_testing,
        },
        DispatchMismatchCase {
            dispatcher: "dispatch_transform",
            expected_kind: "transform",
            actual_kind: "source",
            node_name: "src",
            invoke: DispatchFaultGuard::dispatch_transform_mismatch_for_testing,
        },
        DispatchMismatchCase {
            dispatcher: "dispatch_composition",
            expected_kind: "composition",
            actual_kind: "source",
            node_name: "src",
            invoke: DispatchFaultGuard::dispatch_composition_mismatch_for_testing,
        },
    ];

    for case in cases {
        let (node_idx, node) = dag
            .graph
            .node_indices()
            .map(|idx| (idx, &dag.graph[idx]))
            .find(|(_, node)| node.name() == case.node_name)
            .unwrap_or_else(|| panic!("compiled {} exists", case.node_name));
        let returned = catch_unwind(AssertUnwindSafe(|| (case.invoke)(dag, node_idx, node)))
            .unwrap_or_else(|_| panic!("{} mismatch must return", case.dispatcher));
        let error = match returned {
            Ok(()) => panic!("{} must reject a {}", case.dispatcher, case.actual_kind),
            Err(error) => error,
        };
        let PipelineError::DispatchMismatch {
            dispatcher,
            expected_kind,
            actual_kind,
            node,
        } = &error
        else {
            panic!("{} returned unexpected error: {error}", case.dispatcher);
        };
        assert_eq!(*dispatcher, case.dispatcher);
        assert_eq!(*expected_kind, case.expected_kind);
        assert_eq!(*actual_kind, case.actual_kind);
        assert_eq!(node, case.node_name);

        let classification = error
            .failure_classification()
            .expect("dispatch mismatches have a shared classification");
        assert_eq!(classification.code(), "runtime.invariant.dispatch_mismatch");
        assert_eq!(
            classification.category(),
            FailureCategory::InternalInvariant
        );
        assert_eq!(classification.retry_advice(), RetryAdvice::PolicyRequired);
    }
}

#[test]
#[serial]
fn route_tracer_dispatch_mismatch_is_returned_and_attempt_is_retained() {
    let destination = tempfile::tempdir().expect("create destination root");
    let spill_root = tempfile::tempdir().expect("create spill root");
    let final_path = destination.path().join("output.csv");
    std::fs::write(&final_path, b"existing-final\n").expect("seed visible final");

    let plan = load_config_from_str(PIPELINE_YAML)
        .expect("parse pipeline")
        .compile(&CompileContext::default())
        .expect("compile pipeline");
    let policy = publication_policy(destination.path());
    let run_attempt = RunAttemptPublication::create_for_testing(
        policy.clone(),
        EXECUTION_ID,
        1_000,
        301_000,
        vec![validated(destination.path(), ".")],
    )
    .expect("create run attempt");
    let output_staging = OutputStagingRegistry::for_run_attempt(run_attempt.clone());
    let staged_final = final_path.clone();
    let (_, staged_writer) = output_staging
        .stage_attempt_output(
            ArtifactKind::Primary,
            "out",
            IfExistsPolicy::Overwrite,
            false,
            move |_| Ok(staged_final.clone()),
        )
        .expect("stage attempt-owned output");
    let writers = WriterRegistry {
        single: HashMap::from([(
            "out".to_owned(),
            Box::new(staged_writer) as Box<dyn Write + Send>,
        )]),
        output_staging: output_staging.clone(),
        auto_commit_staged: false,
        ..WriterRegistry::default()
    };
    let source_dropped = Arc::new(AtomicBool::new(false));
    let params = PipelineRunParams {
        execution_id: EXECUTION_ID.to_owned(),
        batch_id: "dispatch-mismatch".to_owned(),
        spill_root_dir: Some(spill_root.path().to_path_buf()),
        ..PipelineRunParams::default()
    };
    let _fault =
        DispatchFaultGuard::route_mismatch_once("rename").expect("arm one dispatch mismatch");

    let run = catch_unwind(AssertUnwindSafe(|| {
        PipelineExecutor::run_plan_with_readers_writers(
            &plan,
            readers(Arc::clone(&source_dropped)),
            writers,
            &params,
        )
    }))
    .expect("dispatch mismatch must return instead of unwinding");
    let error = run.expect_err("armed non-Route dispatch must fail");
    let classification = error
        .failure_classification()
        .expect("dispatch mismatch has a shared classification");
    assert_eq!(classification.code(), "runtime.invariant.dispatch_mismatch");
    assert_eq!(
        classification.category(),
        FailureCategory::InternalInvariant
    );
    assert_eq!(classification.retry_advice(), RetryAdvice::PolicyRequired);
    assert!(
        matches!(
            error,
            PipelineError::DispatchMismatch {
                dispatcher: "dispatch_route",
                expected_kind: "route",
                actual_kind: "transform",
                ref node,
            } if node == "rename"
        ),
        "unexpected typed dispatch error: {error}",
    );
    assert!(
        source_dropped.load(Ordering::Acquire),
        "source reader must be released before the dispatch error returns",
    );
    assert!(
        spill_entries(spill_root.path()).is_empty(),
        "failed dispatch must release and remove its run spill directory",
    );
    assert_eq!(
        std::fs::read(&final_path).expect("read visible final"),
        b"existing-final\n",
        "failed dispatch must not mutate the visible final",
    );

    run_attempt.abandon().expect("retain failed attempt");
    drop(output_staging);
    drop(run_attempt);

    let query = AttemptQuery::new(&plan, &policy, vec![validated(destination.path(), ".")])
        .expect("construct attempt query");
    let root_id = query
        .owned_root_ids()
        .into_iter()
        .next()
        .expect("query has one owned root");
    let retained = query
        .list(root_id, 2_000, None)
        .expect("inspect retained attempt");
    assert!(retained.cleanup_debt().is_empty());
    assert_eq!(retained.entries().len(), 1);
    let inspection = retained.entries()[0].inspection();
    assert_eq!(inspection.execution_id(), EXECUTION_ID);
    assert_eq!(inspection.state(), Some(AttemptState::Abandoned));
    assert_eq!(inspection.eligible_after_unix_ms(), Some(301_000));
    assert!(!inspection.is_eligible());
    assert!(
        inspection
            .cleanup_debt()
            .iter()
            .all(|debt| debt.kind() != CleanupDebtKind::LiveAttempt),
        "terminal attempt must not retain a live writer lock",
    );
    assert!(
        inspection
            .artifact_states()
            .iter()
            .all(|(_, state)| *state == ArtifactState::Unpublished),
    );
    let attempt_root = destination
        .path()
        .join(".clinker-attempts")
        .join(EXECUTION_ID);
    for artifact_id in inspection.artifact_ids() {
        assert!(
            attempt_root.join(artifact_id).is_file(),
            "retained manifest artifact must remain quarantined",
        );
    }
    let live_lock = std::fs::File::open(attempt_root.join("live.lock"))
        .expect("open retained attempt live lock");
    FileExt::try_lock(&live_lock).expect("failed attempt live lock must be released");
    FileExt::unlock(&live_lock).expect("unlock retained attempt live lock");

    let control_final = destination.path().join("control.csv");
    let control_attempt = RunAttemptPublication::create_for_testing(
        policy,
        CONTROL_EXECUTION_ID,
        2_000,
        302_000,
        vec![validated(destination.path(), ".")],
    )
    .expect("create control run attempt");
    let control_staging = OutputStagingRegistry::for_run_attempt(control_attempt.clone());
    let staged_control_final = control_final.clone();
    let (_, control_writer) = control_staging
        .stage_attempt_output(
            ArtifactKind::Primary,
            "out",
            IfExistsPolicy::Overwrite,
            false,
            move |_| Ok(staged_control_final.clone()),
        )
        .expect("stage control attempt output");
    let control_writers = WriterRegistry {
        single: HashMap::from([(
            "out".to_owned(),
            Box::new(control_writer) as Box<dyn Write + Send>,
        )]),
        output_staging: control_staging.clone(),
        auto_commit_staged: false,
        ..WriterRegistry::default()
    };
    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers(Arc::new(AtomicBool::new(false))),
        control_writers,
        &PipelineRunParams {
            execution_id: CONTROL_EXECUTION_ID.to_owned(),
            batch_id: "dispatch-control".to_owned(),
            spill_root_dir: Some(spill_root.path().to_path_buf()),
            ..PipelineRunParams::default()
        },
    )
    .expect("unarmed control run must succeed");
    control_attempt
        .mark_all_ready()
        .expect("ready control attempt");
    let publication = control_attempt
        .publish_run(&control_staging, &ShutdownToken::detached())
        .expect("publish control attempt")
        .expect("control publication gate must win");
    assert!(publication.is_complete());
    assert_eq!(
        std::fs::read_to_string(&control_final).expect("read published control final"),
        "id,renamed\n1,alpha\n2,beta\n",
        "one-shot fault must not affect the control run",
    );
    drop(control_staging);
    drop(control_attempt);
    assert!(
        !destination
            .path()
            .join(".clinker-attempts")
            .join(CONTROL_EXECUTION_ID)
            .exists(),
        "successful control publication must remove its completed attempt",
    );
}
