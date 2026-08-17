//! Runtime consumption of sealed composition-body Source activation groups.

use std::collections::BTreeMap;
use std::sync::Arc;

use clinker_plan::error::PipelineError;
use clinker_plan::plan::composition_body::BodyScopeId;
use clinker_plan::plan::execution::{
    CompiledSourceInstanceId, CompiledSourceScope, ExecutionPlanDag, PlanNode,
    SourceActivationGroupId, SourceActivationPlan,
};

use super::capabilities::{ActiveActivationGroup, AdmittedRunCapabilities, RunCapabilityError};
use super::context::SourceRuntimePolicy;
use super::ingest::{IngestTaskOutcome, ingest_source_body};
use super::source_stream::{SourceConsumer, SourceIngestChannel, SourceStreamEvent};
use crate::pipeline::memory::{ConsumerHandle, ConsumerId, MemoryArbitrator};
use crate::telemetry::{
    MetricKey, SpanFact, SpanName, SpanStatus, TelemetryProducer, unix_nanos_now,
};

type SourceReceiver = crossbeam_channel::Receiver<SourceStreamEvent>;
type SourceConsumerRegistration = (ConsumerId, Arc<ConsumerHandle>);
const SOURCE_LIFECYCLE_SCOPE: &str = "source";

struct ActiveGroupRuntime {
    scope: BodyScopeId,
    group: ActiveActivationGroup,
    workers: Vec<std::thread::JoinHandle<Result<IngestTaskOutcome, PipelineError>>>,
}

/// Receivers and memory registrations produced by one atomic group activation.
pub(crate) struct ActivatedSourceGroup {
    pub(crate) receivers: Vec<(String, SourceReceiver)>,
    pub(crate) consumers: Vec<(String, SourceConsumerRegistration)>,
}

/// Run-local controller for the planner's sealed activation inventory.
///
/// The controller indexes only immutable configuration-derived identities. It
/// never resolves a catalog entry, physical path, profile, or credential. Live
/// input-growing state resides in the bounded Source channels registered with
/// the run's [`MemoryArbitrator`].
pub(crate) struct SourceActivationController {
    plan: SourceActivationPlan,
    capabilities: AdmittedRunCapabilities,
    active: BTreeMap<SourceActivationGroupId, ActiveGroupRuntime>,
    source_runtime: SourceRuntimePolicy,
}

impl SourceActivationController {
    pub(crate) fn new(
        plan: SourceActivationPlan,
        capabilities: AdmittedRunCapabilities,
        source_runtime: SourceRuntimePolicy,
    ) -> Self {
        Self {
            plan,
            capabilities,
            active: BTreeMap::new(),
            source_runtime,
        }
    }

    pub(crate) fn source_runtime(&self) -> SourceRuntimePolicy {
        self.source_runtime.clone()
    }

    /// Activate the complete group containing `instance`, exactly once.
    ///
    /// The pre-admitted group lease transfers before any opener runs. Every
    /// opener completes before a Source channel is registered or a worker is
    /// spawned, so partial open failure cannot start downstream work.
    pub(crate) fn activate(
        &mut self,
        instance: CompiledSourceInstanceId,
        dag: &ExecutionPlanDag,
        logical_prefix: &str,
        memory: &Arc<MemoryArbitrator>,
        shutdown: Option<crate::pipeline::shutdown::ShutdownToken>,
        telemetry: Option<&TelemetryProducer>,
    ) -> Result<Option<ActivatedSourceGroup>, PipelineError> {
        let CompiledSourceScope::CompositionBody(scope) = instance.scope else {
            return Ok(None);
        };
        let group = self
            .plan
            .groups()
            .iter()
            .find(|group| group.members().contains(&instance))
            .ok_or_else(|| PipelineError::Internal {
                op: "source-activation",
                node: String::new(),
                detail: "compiled body Source has no sealed activation group".to_string(),
            })?;
        let group_id = group.id();
        if self.active.contains_key(&group_id) {
            return Ok(None);
        }
        let members: Box<[_]> = group.members().into();

        let mut active = self
            .capabilities
            .take_group(group_id)
            .map_err(capability_error)?;
        for member in members.iter().copied() {
            let source_name = source_name_for(dag, member)?;
            let logical_node = if logical_prefix.is_empty() {
                source_name.to_string()
            } else {
                format!("{logical_prefix}.{source_name}")
            };
            observe_open(telemetry, &logical_node, || active.open(member))?;
        }

        let mut prepared = Vec::with_capacity(members.len());
        for member in members.iter().copied() {
            let body = source_body_for(dag, member)?.clone();
            let input = active.take_source_input(member).map_err(capability_error)?;
            prepared.push((member, body, input));
        }

        let mut activated = ActivatedSourceGroup {
            receivers: Vec::with_capacity(prepared.len()),
            consumers: Vec::with_capacity(prepared.len()),
        };
        let mut workers = Vec::with_capacity(prepared.len());
        for (member, body, input) in prepared {
            let source_name = body.source.name.clone();
            let logical_source_name = if logical_prefix.is_empty() {
                source_name.clone()
            } else {
                format!("{logical_prefix}.{source_name}")
            };
            let handle = ConsumerHandle::new();
            let (stream, receiver) = SourceIngestChannel::new(
                SourceIngestChannel::DEFAULT_CAPACITY,
                Arc::clone(&handle),
                member.source_node,
            );
            let consumer_id =
                memory.register_consumer(Arc::new(SourceConsumer::new(Arc::clone(&handle))));
            activated.receivers.push((source_name.clone(), receiver));
            activated
                .consumers
                .push((source_name.clone(), (consumer_id, handle)));
            let worker_shutdown = shutdown.clone();
            let lifecycle_shutdown = worker_shutdown.clone();
            let lifecycle_telemetry = telemetry.cloned();
            let source_runtime = self.source_runtime.clone();
            let spawn = std::thread::Builder::new()
                .name(format!("clinker-body-source-{source_name}"))
                .spawn(move || {
                    observe_source(
                        lifecycle_telemetry.as_ref(),
                        lifecycle_shutdown.as_ref(),
                        || {
                            let mut outcome = ingest_source_body(
                                body,
                                input,
                                stream,
                                worker_shutdown,
                                None,
                                source_runtime,
                            )?;
                            outcome.source_name = logical_source_name;
                            Ok(outcome)
                        },
                    )
                });
            match spawn {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    drop(activated.receivers);
                    for (_, (id, handle)) in activated.consumers {
                        handle.resume();
                        handle.set_bytes(0);
                        memory.unregister_consumer(id);
                    }
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(PipelineError::Internal {
                        op: "body-source-spawn",
                        node: String::new(),
                        detail: format!("failed to spawn body Source worker: {error}"),
                    });
                }
            }
        }

        self.active.insert(
            group_id,
            ActiveGroupRuntime {
                scope,
                group: active,
                workers,
            },
        );
        Ok(Some(activated))
    }

    /// Join and close every active group in `scope` after its receivers drop.
    pub(super) fn finish_scope(
        &mut self,
        scope: BodyScopeId,
    ) -> Result<Vec<IngestTaskOutcome>, PipelineError> {
        let group_ids: Vec<_> = self
            .active
            .iter()
            .filter_map(|(id, runtime)| (runtime.scope == scope).then_some(*id))
            .collect();
        let mut outcomes = Vec::new();
        let mut first_error = None;
        for id in group_ids.into_iter().rev() {
            let mut runtime = self
                .active
                .remove(&id)
                .expect("active group id was collected from the same map");
            while let Some(worker) = runtime.workers.pop() {
                match worker.join() {
                    Ok(Ok(outcome)) => outcomes.push(outcome),
                    Ok(Err(error)) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    Err(_) => {
                        if first_error.is_none() {
                            first_error = Some(PipelineError::Internal {
                                op: "body-source-thread",
                                node: String::new(),
                                detail: "body Source worker panicked".to_string(),
                            });
                        }
                    }
                }
            }
            drop(runtime.group);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(outcomes),
        }
    }
}

impl Drop for SourceActivationController {
    fn drop(&mut self) {
        while let Some((_, mut runtime)) = self.active.pop_last() {
            while let Some(worker) = runtime.workers.pop() {
                let _ = worker.join();
            }
            drop(runtime.group);
        }
    }
}

fn source_name_for(
    dag: &ExecutionPlanDag,
    instance: CompiledSourceInstanceId,
) -> Result<&str, PipelineError> {
    dag.graph
        .node_weights()
        .find(|node| node.id() == instance.source_node)
        .map(PlanNode::name)
        .ok_or_else(|| PipelineError::Internal {
            op: "source-activation",
            node: String::new(),
            detail: "sealed Source identity is absent from the active body DAG".to_string(),
        })
}

fn source_body_for(
    dag: &ExecutionPlanDag,
    instance: CompiledSourceInstanceId,
) -> Result<&clinker_plan::config::pipeline_node::SourceBody, PipelineError> {
    dag.graph
        .node_weights()
        .find_map(|node| match node {
            PlanNode::Source {
                id,
                resolved: Some(payload),
                ..
            } if *id == instance.source_node => Some(&payload.body),
            _ => None,
        })
        .ok_or_else(|| PipelineError::Internal {
            op: "source-activation",
            node: String::new(),
            detail: "sealed body Source has no retained reader contract".to_string(),
        })
}

fn observe_open<T>(
    producer: Option<&TelemetryProducer>,
    logical_node: &str,
    operation: impl FnOnce() -> Result<T, RunCapabilityError>,
) -> Result<T, PipelineError> {
    let Some(producer) = producer else {
        return operation().map_err(capability_error);
    };
    let started_at_unix_nanos = unix_nanos_now();
    producer.record_metric(MetricKey::ResourceOpenStarted, 1);
    let result = operation();
    let ended_at_unix_nanos = unix_nanos_now().max(started_at_unix_nanos);
    let (metric, status) = if result.is_ok() {
        (MetricKey::ResourceOpenCompleted, SpanStatus::Ok)
    } else {
        (MetricKey::ResourceOpenFailed, SpanStatus::Error)
    };
    producer.record_metric(metric, 1);
    producer.emit_span(SpanFact {
        name: SpanName::ResourceOpen,
        status,
        logical_node,
        started_at_unix_nanos,
        ended_at_unix_nanos,
    });
    result.map_err(capability_error)
}

fn observe_source<T>(
    producer: Option<&TelemetryProducer>,
    shutdown: Option<&crate::pipeline::shutdown::ShutdownToken>,
    operation: impl FnOnce() -> Result<T, PipelineError>,
) -> Result<T, PipelineError> {
    let Some(producer) = producer else {
        return operation();
    };
    let started_at_unix_nanos = unix_nanos_now();
    producer.record_metric(MetricKey::SourceStarted, 1);
    let result = operation();
    let (metric, status) = match &result {
        Err(PipelineError::Interrupted) => (MetricKey::SourceInterrupted, SpanStatus::Unset),
        Err(_) => (MetricKey::SourceFailed, SpanStatus::Error),
        Ok(_) if shutdown.is_some_and(crate::pipeline::shutdown::ShutdownToken::is_requested) => {
            (MetricKey::SourceInterrupted, SpanStatus::Unset)
        }
        Ok(_) => (MetricKey::SourceCompleted, SpanStatus::Ok),
    };
    producer.record_metric(metric, 1);
    let ended_at_unix_nanos = unix_nanos_now().max(started_at_unix_nanos);
    let _ = producer.emit_span(SpanFact {
        name: SpanName::Source,
        status,
        logical_node: SOURCE_LIFECYCLE_SCOPE,
        started_at_unix_nanos,
        ended_at_unix_nanos,
    });
    result
}

fn capability_error(error: RunCapabilityError) -> PipelineError {
    PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
        "body Source activation failed: {error}"
    )))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::Arc;

    use clinker_bench_support::io::SharedBuffer;
    use clinker_plan::config::{CompileContext, parse_config};
    use clinker_plan::error::PipelineError;

    use super::super::capabilities::{
        AdmittedActivationGroup, AdmittedRunCapabilities, AdmittedSourceOpener,
        CapabilityOpenError, CapabilityOpener, CapabilitySession,
    };
    use super::super::{PipelineExecutor, PipelineRunParams, SourceInput};
    use crate::pipeline::memory::MemoryArbitrator;
    use crate::pipeline::shutdown::ShutdownToken;
    use crate::source::multi_file::FileSlot;

    const BODY: &str = r#"_compose:
  name: memory_reader
  inputs: {}
  outputs: { out: read }
  config_schema: {}
  resources_schema:
    input: { kind: file, required: true }
nodes:
  - type: source
    name: read
    config:
      name: read
      type: csv
      resource: input
      on_unmapped: { mode: drop }
      schema: [{ name: id, type: int }]
"#;

    const PIPELINE: &str = r#"pipeline: { name: body_source_memory }
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      path: driver.csv
      schema: [{ name: seed, type: string }]
  - type: composition
    name: call
    input: driver
    use: ../compositions/memory_reader.comp.yaml
    inputs: {}
    resources: { input: shared_input }
  - type: output
    name: out
    input: call
    config: { name: out, type: csv, path: out.csv }
"#;

    struct InputOpener {
        body: Vec<u8>,
        shutdown: Option<ShutdownToken>,
    }

    impl CapabilityOpener for InputOpener {
        fn open(self: Box<Self>) -> Result<Box<dyn CapabilitySession>, CapabilityOpenError> {
            if let Some(token) = &self.shutdown {
                token.request();
            }
            Ok(Box::new(InputSession {
                input: Some(SourceInput::Files(vec![FileSlot::new(
                    "logical.csv",
                    Box::new(Cursor::new(self.body)),
                )])),
            }))
        }
    }

    struct InputSession {
        input: Option<SourceInput>,
    }

    impl CapabilitySession for InputSession {
        fn take_source_input(&mut self) -> Result<SourceInput, CapabilityOpenError> {
            self.input.take().ok_or(CapabilityOpenError::Unavailable)
        }
    }

    fn fixture() -> (tempfile::TempDir, clinker_plan::plan::CompiledPlan) {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("compositions"))
            .expect("composition directory");
        std::fs::create_dir_all(workspace.path().join("pipelines")).expect("pipeline directory");
        std::fs::create_dir_all(workspace.path().join("data")).expect("data directory");
        std::fs::write(workspace.path().join("data/input.csv"), "id\n1\n")
            .expect("catalog resource");
        std::fs::write(
            workspace
                .path()
                .join("compositions/memory_reader.comp.yaml"),
            BODY,
        )
        .expect("composition body");
        std::fs::write(
            workspace.path().join("clinker.toml"),
            r#"[catalog.resources.shared_input]
kind = "file"
path = "data/input.csv"
access = "read"
"#,
        )
        .expect("catalog");
        let config = parse_config(PIPELINE).expect("pipeline parses");
        let plan = config
            .compile(&CompileContext::with_pipeline_dir(
                workspace.path(),
                PathBuf::from("pipelines"),
            ))
            .unwrap_or_else(|diagnostics| panic!("pipeline compiles: {diagnostics:?}"));
        (workspace, plan)
    }

    fn capabilities(
        plan: &clinker_plan::plan::CompiledPlan,
        body: &[u8],
        shutdown: Option<ShutdownToken>,
    ) -> AdmittedRunCapabilities {
        let activation = plan.dag().source_activation();
        let groups = activation
            .groups()
            .iter()
            .map(|group| {
                let sources = group
                    .members()
                    .iter()
                    .copied()
                    .map(|member| {
                        let opener: Box<dyn CapabilityOpener> = match member.scope {
                            clinker_plan::plan::execution::CompiledSourceScope::TopLevel => {
                                return AdmittedSourceOpener::caller_supplied(member);
                            }
                            clinker_plan::plan::execution::CompiledSourceScope::CompositionBody(
                                _,
                            ) => Box::new(InputOpener {
                                body: body.to_vec(),
                                shutdown: shutdown.clone(),
                            }),
                        };
                        AdmittedSourceOpener::new(member, opener)
                    })
                    .collect();
                AdmittedActivationGroup::uncredentialed(group.id(), group.capacity(), sources)
            })
            .collect();
        AdmittedRunCapabilities::admit(activation, groups).expect("capabilities admit")
    }

    fn run(
        workspace: &std::path::Path,
        plan: &clinker_plan::plan::CompiledPlan,
        capabilities: AdmittedRunCapabilities,
        params: &PipelineRunParams,
        memory: Arc<MemoryArbitrator>,
    ) -> Result<super::super::ExecutionReport, PipelineError> {
        let readers = HashMap::from([(
            "driver".to_string(),
            SourceInput::Files(vec![FileSlot::new(
                "driver.csv",
                Box::new(Cursor::new(b"seed\ngo\n".to_vec())),
            )]),
        )]);
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
            HashMap::from([("out".to_string(), Box::new(SharedBuffer::new()) as _)]);
        PipelineExecutor::run_admitted_plan_with_readers_writers_and_arbitrator(
            plan,
            capabilities,
            readers,
            writers.into(),
            params,
            CompileContext::with_pipeline_dir(workspace, PathBuf::from("pipelines")),
            memory,
        )
    }

    fn memory() -> Arc<MemoryArbitrator> {
        Arc::new(MemoryArbitrator::with_policy(
            100 * 1024 * 1024 * 1024,
            0.80,
            0.70,
            MemoryArbitrator::default_policy(),
        ))
    }

    #[test]
    fn body_source_consumers_leave_the_memory_registry_on_every_exit() {
        let (workspace, plan) = fixture();

        let success_memory = memory();
        let success = run(
            workspace.path(),
            &plan,
            capabilities(&plan, b"id\n1\n2\n", None),
            &PipelineRunParams::default(),
            Arc::clone(&success_memory),
        )
        .expect("successful body Source run");
        assert_eq!(success.counters.ok_count, 2);
        assert_eq!(success_memory.consumer_count(), 0);
        assert_eq!(success_memory.sum_consumer_usage(), 0);

        let error_memory = memory();
        run(
            workspace.path(),
            &plan,
            capabilities(&plan, b"id\nnot-an-int\n", None),
            &PipelineRunParams::default(),
            Arc::clone(&error_memory),
        )
        .expect_err("reader failure propagates");
        assert_eq!(error_memory.consumer_count(), 0);
        assert_eq!(error_memory.sum_consumer_usage(), 0);

        let shutdown = ShutdownToken::detached();
        let cancel_memory = memory();
        let cancelled = run(
            workspace.path(),
            &plan,
            capabilities(&plan, b"id\n1\n2\n", Some(shutdown.clone())),
            &PipelineRunParams {
                shutdown_token: Some(shutdown),
                ..Default::default()
            },
            Arc::clone(&cancel_memory),
        )
        .expect("cancellation unwinds cleanly");
        assert!(cancelled.interrupted);
        assert_eq!(cancel_memory.consumer_count(), 0);
        assert_eq!(cancel_memory.sum_consumer_usage(), 0);
    }
}
