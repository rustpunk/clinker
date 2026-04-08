//! Phase 1 source ingestion with optional source-level parallelism.
//!
//! Reads sources according to the `SourceTier` DAG from the execution plan.
//! Sources within a tier are independent and ingested concurrently via
//! `std::thread::scope`. Sources in later tiers wait for earlier tiers to
//! complete (e.g., reference data before primary data).
//!
//! Single-source pipelines skip `thread::scope` entirely (fast path).

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{PipelineConfig, PipelineNode, SourceConfig};
use crate::error::PipelineError;
use crate::pipeline::arena::Arena;
use crate::pipeline::index::SecondaryIndex;
use crate::plan::execution::ExecutionPlanDag;
use crate::plan::index;
use clinker_format::traits::FormatReader;

/// Data products of Phase 1 ingestion, per source.
///
/// Consumed immutably by Phase 1.5 (partition sorting) and Phase 2 (evaluation).
/// Each source produces its own Arena and set of SecondaryIndices.
#[derive(Debug)]
pub struct IngestionOutput {
    pub arenas: HashMap<String, Arena>,
    pub indices: HashMap<String, Vec<SecondaryIndex>>,
}

/// Collect all `Source` nodes from the unified `config.nodes` list.
///
/// Phase 16b Wave 4ab Checkpoint B: readers source from `nodes:` instead
/// of the legacy top-level `inputs:` field.
fn collect_source_nodes(config: &PipelineConfig) -> Vec<&SourceConfig> {
    config
        .nodes
        .iter()
        .filter_map(|n| match &n.value {
            PipelineNode::Source { config: body, .. } => Some(&body.source),
            _ => None,
        })
        .collect()
}

/// Run Phase 1 ingestion across all sources, respecting the SourceTier DAG.
///
/// - Single source: fast path, no `thread::scope` overhead.
/// - Multiple sources: tiers execute sequentially; sources within a tier
///   run concurrently via `std::thread::scope`.
///
/// Each source thread builds its own Arena + SecondaryIndices locally.
/// After `thread::scope` joins, results are moved into `IngestionOutput`.
pub fn ingest_sources<F>(
    plan: &ExecutionPlanDag,
    config: &PipelineConfig,
    memory_limit: usize,
    open_reader: F,
) -> Result<IngestionOutput, PipelineError>
where
    F: Fn(&str) -> Result<Box<dyn FormatReader + Send>, PipelineError> + Sync,
{
    let source_nodes = collect_source_nodes(config);
    if source_nodes.len() <= 1 {
        return ingest_single_source(plan, config, memory_limit, &open_reader);
    }

    let mut output = IngestionOutput {
        arenas: HashMap::new(),
        indices: HashMap::new(),
    };

    for tier in &plan.source_dag {
        // Collect results from all sources in this tier concurrently
        let tier_results: Vec<_> = std::thread::scope(|s| {
            let handles: Vec<_> = tier
                .sources
                .iter()
                .map(|source_name| {
                    s.spawn(|| {
                        ingest_one_source(source_name, plan, config, memory_limit, &open_reader)
                    })
                })
                .collect();

            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                let result = handle.join().map_err(|_| {
                    PipelineError::ThreadPool("source ingestion thread panicked".into())
                })?;
                results.push(result);
            }
            Ok::<_, PipelineError>(results)
        })?;

        for result in tier_results {
            let (name, arena, idxs) = result?;
            output.arenas.insert(name.clone(), arena);
            output.indices.insert(name, idxs);
        }
    }

    Ok(output)
}

/// Ingest a single source: build Arena + SecondaryIndices.
fn ingest_one_source<F>(
    source_name: &str,
    plan: &ExecutionPlanDag,
    config: &PipelineConfig,
    memory_limit: usize,
    open_reader: &F,
) -> Result<(String, Arena, Vec<SecondaryIndex>), PipelineError>
where
    F: Fn(&str) -> Result<Box<dyn FormatReader + Send>, PipelineError>,
{
    let input = collect_source_nodes(config)
        .into_iter()
        .find(|i| i.name == source_name)
        .ok_or_else(|| PipelineError::Compilation {
            transform_name: String::new(),
            messages: vec![format!("source '{}' not found in config", source_name)],
        })?;

    // Determine which fields this source needs in its Arena
    let arena_fields = index::collect_arena_fields(&plan.indices_to_build, source_name);

    if arena_fields.is_empty() {
        // This source has no indices — return empty Arena
        let schema = Arc::new(clinker_record::Schema::new(vec![]));
        let arena = Arena::empty(schema);
        return Ok((source_name.to_string(), arena, Vec::new()));
    }

    // Open reader for this source
    let mut reader = open_reader(&input.path)?;

    // Build Arena
    let arena = Arena::build(reader.as_mut(), &arena_fields, memory_limit, None).map_err(|e| {
        PipelineError::Compilation {
            transform_name: String::new(),
            messages: vec![e.to_string()],
        }
    })?;

    // Build SecondaryIndices for this source
    let schema_pins: HashMap<String, clinker_record::schema_def::FieldDef> = input
        .schema_overrides
        .as_ref()
        .map(|overrides| {
            overrides
                .iter()
                .map(|o| (o.name.clone(), o.clone()))
                .collect()
        })
        .unwrap_or_default();

    let mut source_indices = Vec::new();
    for spec in &plan.indices_to_build {
        if spec.source == source_name {
            let idx = SecondaryIndex::build(&arena, &spec.group_by, &schema_pins).map_err(|e| {
                PipelineError::Compilation {
                    transform_name: String::new(),
                    messages: vec![e.to_string()],
                }
            })?;
            source_indices.push(idx);
        }
    }

    Ok((source_name.to_string(), arena, source_indices))
}

/// Fast path for single-source pipelines — no `thread::scope` overhead.
fn ingest_single_source<F>(
    plan: &ExecutionPlanDag,
    config: &PipelineConfig,
    memory_limit: usize,
    open_reader: &F,
) -> Result<IngestionOutput, PipelineError>
where
    F: Fn(&str) -> Result<Box<dyn FormatReader + Send>, PipelineError>,
{
    let source_nodes = collect_source_nodes(config);
    let source_name = source_nodes
        .first()
        .map(|i| i.name.as_str())
        .unwrap_or("primary")
        .to_string();

    let (name, arena, idxs) =
        ingest_one_source(&source_name, plan, config, memory_limit, open_reader)?;

    let mut arenas = HashMap::new();
    let mut indices = HashMap::new();
    arenas.insert(name.clone(), arena);
    indices.insert(name, idxs);

    Ok(IngestionOutput { arenas, indices })
}

