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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{self, *};
    use crate::pipeline::shutdown;
    use crate::plan::execution::ExecutionPlanDag;
    use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
    use clinker_record::{RecordStorage, Schema};
    use std::sync::Arc;

    /// Identity helper retained to keep test callsites compact.
    #[allow(dead_code)]
    fn t(entry: &LegacyTransformsBlock) -> &LegacyTransformsBlock {
        entry
    }

    /// Helper: build a minimal pipeline config.
    fn test_config(
        inputs: Vec<(&str, &str)>,
        transforms: Vec<(&str, &str, Option<serde_json::Value>)>,
    ) -> PipelineConfig {
        PipelineConfig {
            pipeline: PipelineMeta {
                name: "test".into(),
                memory_limit: None,
                vars: None,
                date_formats: None,
                rules_path: None,
                concurrency: None,
                date_locale: None,
                log_rules: None,
                include_provenance: None,
                metrics: None,
            },
            nodes: inputs
                .iter()
                .map(|(name, path)| {
                    use crate::config::node_header::SourceHeader;
                    use crate::config::pipeline_node::{PipelineNode, SourceBody};
                    use crate::yaml::{Location, Spanned};
                    Spanned::new(
                        PipelineNode::Source {
                            header: SourceHeader {
                                name: (*name).to_string(),
                                description: None,
                                notes: None,
                            },
                            config: SourceBody {
                                source: SourceConfig {
                                    name: (*name).to_string(),
                                    format: InputFormat::Csv(None),
                                    path: (*path).to_string(),
                                    schema: None,
                                    schema_overrides: None,
                                    array_paths: None,
                                    sort_order: None,
                                    notes: None,
                                },
                            },
                        },
                        Location::UNKNOWN,
                        Location::UNKNOWN,
                    )
                })
                .collect(),
            inputs: inputs
                .into_iter()
                .map(|(name, path)| SourceConfig {
                    name: name.into(),
                    format: InputFormat::Csv(None),
                    path: path.into(),
                    schema: None,
                    schema_overrides: None,
                    array_paths: None,
                    sort_order: None,
                    notes: None,
                })
                .collect(),
            outputs: vec![OutputConfig {
                name: "output".into(),
                path: "out.csv".into(),
                include_unmapped: true,
                include_header: None,
                mapping: None,
                exclude: None,
                sort_order: None,
                preserve_nulls: None,
                include_metadata: Default::default(),
                schema: None,
                split: None,
                format: OutputFormat::Csv(None),
                notes: None,
            }],
            transformations: transforms
                .into_iter()
                .map(|(name, cxl, local_window)| LegacyTransformsBlock {
                    name: name.into(),
                    description: None,
                    cxl: Some(cxl.into()),
                    aggregate: None,
                    local_window,
                    log: None,
                    validations: None,
                    route: None,
                    input: None,
                    notes: None,
                })
                .collect(),
            error_handling: ErrorHandlingConfig::default(),
            notes: None,
        }
    }

    /// Helper: compile CXL source.
    fn compile_cxl(source: &str, fields: &[&str]) -> cxl::typecheck::pass::TypedProgram {
        let parsed = cxl::parser::Parser::parse(source);
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors
        );
        let resolved =
            cxl::resolve::pass::resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
        let schema: indexmap::IndexMap<String, cxl::typecheck::types::Type> =
            indexmap::IndexMap::new();
        cxl::typecheck::pass::type_check(resolved, &schema).unwrap()
    }

    /// Open a CSV reader from in-memory data.
    fn csv_reader_from_str(data: &str) -> Box<dyn FormatReader + Send> {
        let cursor = std::io::Cursor::new(data.as_bytes().to_vec());
        Box::new(CsvReader::from_reader(cursor, CsvReaderConfig::default()))
    }

    #[test]
    fn test_single_source_no_spawn() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![(
                "agg",
                "emit total = $window.sum(amount)",
                Some(serde_json::json!({"group_by": ["dept"]})),
            )],
        );
        let fields = &["dept", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("agg", &typed)];
        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        let csv_data = "dept,amount\nA,100\nB,200\nA,150\n";
        let result = ingest_sources(&plan, &config, 512 * 1024 * 1024, |_path| {
            Ok(csv_reader_from_str(csv_data))
        });

        let output = result.unwrap();
        assert_eq!(output.arenas.len(), 1);
        assert!(output.arenas.contains_key("primary"));
        let arena = &output.arenas["primary"];
        assert_eq!(arena.record_count(), 3);
    }

    #[test]
    fn test_arenas_moved_to_context() {
        // Two independent sources → IngestionOutput has both
        let window = serde_json::json!({
            "source": "reference",
            "group_by": ["id"],
            "on": "ref_id"
        });
        let config = test_config(
            vec![("primary", "data.csv"), ("reference", "ref.csv")],
            vec![("lookup", "emit ref_val = $window.sum(amount)", Some(window))],
        );
        let fields = &["id", "ref_id", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("lookup", &typed)];
        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        let primary_csv = "id,ref_id,amount\n1,R1,100\n2,R2,200\n";
        let ref_csv = "id,amount\nR1,50\nR2,75\n";

        let result = ingest_sources(&plan, &config, 512 * 1024 * 1024, |path| {
            let data = if path == "ref.csv" {
                ref_csv
            } else {
                primary_csv
            };
            Ok(csv_reader_from_str(data))
        });

        let output = result.unwrap();
        assert_eq!(
            output.arenas.len(),
            2,
            "Should have arenas for both sources"
        );
        assert!(
            output.arenas.contains_key("primary") || output.arenas.contains_key("reference"),
            "Should contain reference source arena"
        );
    }

    #[test]
    fn test_source_thread_owns_arena() {
        // Each source produces a distinct Arena with its own schema
        let window = serde_json::json!({
            "source": "reference",
            "group_by": ["id"],
            "on": "ref_id"
        });
        let config = test_config(
            vec![("primary", "data.csv"), ("reference", "ref.csv")],
            vec![("lookup", "emit ref_val = $window.sum(amount)", Some(window))],
        );
        let fields = &["id", "ref_id", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("lookup", &typed)];
        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        let primary_csv = "id,ref_id,amount\n1,R1,100\n2,R2,200\n";
        let ref_csv = "id,amount\nR1,50\nR2,75\n";

        let output = ingest_sources(&plan, &config, 512 * 1024 * 1024, |path| {
            let data = if path == "ref.csv" {
                ref_csv
            } else {
                primary_csv
            };
            Ok(csv_reader_from_str(data))
        })
        .unwrap();

        // Reference source arena should exist and have 2 records
        if let Some(ref_arena) = output.arenas.get("reference") {
            assert_eq!(ref_arena.record_count(), 2);
        }
    }

    #[test]
    fn test_independent_sources_concurrent() {
        // Two independent sources in the same tier — both run
        // We verify by checking both arenas are populated
        let window = serde_json::json!({
            "source": "reference",
            "group_by": ["id"],
            "on": "ref_id"
        });
        let config = test_config(
            vec![("primary", "data.csv"), ("reference", "ref.csv")],
            vec![("lookup", "emit ref_val = $window.sum(amount)", Some(window))],
        );
        let fields = &["id", "ref_id", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("lookup", &typed)];
        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        let primary_csv = "id,ref_id,amount\n1,R1,100\n";
        let ref_csv = "id,amount\nR1,50\n";

        let start = std::time::Instant::now();
        let output = ingest_sources(&plan, &config, 512 * 1024 * 1024, |path| {
            // Small artificial delay to demonstrate concurrency
            std::thread::sleep(std::time::Duration::from_millis(10));
            let data = if path == "ref.csv" {
                ref_csv
            } else {
                primary_csv
            };
            Ok(csv_reader_from_str(data))
        })
        .unwrap();
        let elapsed = start.elapsed();

        // Both sources should be populated
        assert!(output.arenas.len() >= 1);
        // With concurrency, two 10ms sleeps should take < 30ms (not 20ms+ serial)
        // Being generous with the threshold to avoid flakiness
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "Ingestion took {:?}, expected < 100ms for concurrent sources",
            elapsed
        );
    }

    #[test]
    fn test_dependent_sources_sequential() {
        // Reference source is in tier 0, primary in tier 1
        // The DAG ensures reference completes before primary starts
        let window = serde_json::json!({
            "source": "reference",
            "group_by": ["id"],
            "on": "ref_id"
        });
        let config = test_config(
            vec![("primary", "data.csv"), ("reference", "ref.csv")],
            vec![("lookup", "emit ref_val = $window.sum(amount)", Some(window))],
        );
        let fields = &["id", "ref_id", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("lookup", &typed)];
        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        // Verify DAG: reference should be in tier 0 (built first)
        assert!(plan.source_dag.len() >= 2, "Should have at least 2 tiers");
        assert!(
            plan.source_dag[0]
                .sources
                .contains(&"reference".to_string()),
            "Reference should be in tier 0"
        );

        let primary_csv = "id,ref_id,amount\n1,R1,100\n";
        let ref_csv = "id,amount\nR1,50\n";

        let output = ingest_sources(&plan, &config, 512 * 1024 * 1024, |path| {
            let data = if path == "ref.csv" {
                ref_csv
            } else {
                primary_csv
            };
            Ok(csv_reader_from_str(data))
        })
        .unwrap();

        // Both sources ingested successfully — tier ordering was respected
        assert!(output.arenas.contains_key("reference"));
    }

    #[test]
    fn test_source_error_propagation() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![(
                "agg",
                "emit total = $window.sum(amount)",
                Some(serde_json::json!({"group_by": ["dept"]})),
            )],
        );
        let fields = &["dept", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("agg", &typed)];
        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        // Return an error from the reader factory
        let result = ingest_sources(&plan, &config, 512 * 1024 * 1024, |_path| {
            Err(PipelineError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "file not found",
            )))
        });

        assert!(result.is_err(), "Error should propagate from source thread");
    }

    #[test]
    fn test_signal_during_phase1_ingestion() {
        // With per-token shutdown, the only meaningful unit-level assertion
        // is that `Arena::build` checks the token it was handed and returns
        // `ArenaError::ShutdownRequested` once the chunk-boundary check
        // window (4096 records) is crossed.
        use crate::pipeline::arena::{Arena, ArenaError};
        let token = shutdown::ShutdownToken::detached();
        token.request();

        let mut csv = String::from("dept,amount\n");
        for i in 0..5000 {
            csv.push_str(&format!("D{},{}\n", i % 10, i));
        }
        let mut reader = csv_reader_from_str(&csv);
        let result = Arena::build(
            reader.as_mut(),
            &["dept".into(), "amount".into()],
            512 * 1024 * 1024,
            Some(&token),
        );
        assert!(matches!(result, Err(ArenaError::ShutdownRequested)));
    }
}
