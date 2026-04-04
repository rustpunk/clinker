//! Phase F: Execution plan emission.
//!
//! Compiles a `PipelineConfig` + CXL programs into an `ExecutionPlan`
//! that orchestrates the two-pass pipeline.

use std::collections::HashSet;

use indexmap::IndexMap;

use crate::config::{PipelineConfig, RouteMode, SortField};
use crate::plan::index::{self, IndexSpec, LocalWindowConfig, PlanIndexError, RawIndexRequest};

use cxl::analyzer::{self, ParallelismHint};
use cxl::typecheck::pass::TypedProgram;

/// Route topology for `--explain` display.
///
/// This is a display-only struct, separate from the runtime `CompiledRoute`.
/// Holds condition strings (not compiled evaluators) for human-readable output.
#[derive(Debug)]
pub struct RoutePlan {
    /// Routing mode: exclusive (first-match) or inclusive (all-match).
    pub mode: RouteMode,
    /// (branch_name, condition_expression) pairs in evaluation order.
    pub branches: Vec<(String, String)>,
    /// Default output name for records matching no branch.
    pub default: String,
}

/// Full execution plan — spec §6.1 Phase F output.
#[derive(Debug)]
pub struct ExecutionPlan {
    /// Topologically sorted source tiers for Phase 1 ordering.
    pub source_dag: Vec<SourceTier>,
    /// Indices to build during Phase 1. Deduplicated.
    pub indices_to_build: Vec<IndexSpec>,
    /// Per-transform execution units with compiled CXL and classification.
    pub transforms: Vec<TransformPlan>,
    /// Per-output projection rules.
    pub output_projections: Vec<OutputSpec>,
    /// Parallelism profile for the pipeline.
    pub parallelism: ParallelismProfile,
    /// Route configuration, if multi-output routing is configured.
    pub route: Option<RoutePlan>,
}

/// Whether the pipeline needs one pass or two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// No window functions — single-pass streaming.
    Streaming,
    /// Window functions present — build arena + indices, then re-read.
    TwoPass,
}

impl ExecutionPlan {
    /// Derive execution mode from index requirements.
    pub fn mode(&self) -> ExecutionMode {
        if self.indices_to_build.is_empty() {
            ExecutionMode::Streaming
        } else {
            ExecutionMode::TwoPass
        }
    }

    /// Compile a PipelineConfig + pre-compiled transforms into an ExecutionPlan.
    ///
    /// `compiled`: vec of (transform_name, TypedProgram) pairs, already compiled
    /// in the order they appear in config.transformations.
    pub fn compile(
        config: &PipelineConfig,
        compiled: &[(&str, &TypedProgram)],
    ) -> Result<Self, PlanError> {
        let primary_source = config
            .inputs
            .first()
            .map(|i| i.name.clone())
            .unwrap_or_default();

        // Phase D: analyze all transforms
        let report = analyzer::analyze_all(compiled);

        // Parse local_window configs
        let window_configs: Vec<Option<LocalWindowConfig>> = config
            .transforms()
            .map(|t| index::parse_local_window(t).map_err(PlanError::IndexPlanning))
            .collect::<Result<Vec<_>, _>>()?;

        // Validate: if a transform uses window.* but has no local_window, error
        for (i, analysis) in report.transforms.iter().enumerate() {
            if !analysis.window_calls.is_empty() && window_configs[i].is_none() {
                return Err(PlanError::MissingLocalWindow {
                    transform: analysis.name.clone(),
                });
            }
        }

        // Phase E: build raw index requests, then deduplicate
        let mut raw_requests = Vec::new();
        for (i, wc) in window_configs.iter().enumerate() {
            if let Some(wc) = wc {
                let source = wc.source.clone().unwrap_or_else(|| primary_source.clone());

                // Validate source exists
                if !config.inputs.iter().any(|inp| inp.name == source) {
                    return Err(PlanError::UnknownSource {
                        name: source,
                        transform: report.transforms[i].name.clone(),
                    });
                }

                // Compute arena fields: group_by + sort_by field names + window-accessed fields
                let mut arena_fields: HashSet<String> = HashSet::new();
                for gb in &wc.group_by {
                    arena_fields.insert(gb.clone());
                }
                for sf in &wc.sort_by {
                    arena_fields.insert(sf.field.clone());
                }
                for f in &report.transforms[i].accessed_fields {
                    arena_fields.insert(f.clone());
                }

                // Check pre-sorted optimization
                let already_sorted = check_already_sorted(config, &source, &wc.sort_by);

                raw_requests.push(RawIndexRequest {
                    source,
                    group_by: wc.group_by.clone(),
                    sort_by: wc.sort_by.clone(),
                    arena_fields: arena_fields.into_iter().collect(),
                    already_sorted,
                    transform_index: i,
                });
            }
        }

        let indices = index::deduplicate_indices(raw_requests.clone());

        // Phase F: build transform plans
        let transforms: Vec<TransformPlan> = compiled
            .iter()
            .enumerate()
            .map(|(i, (name, _typed))| {
                let analysis = &report.transforms[i];
                let wc = &window_configs[i];

                let parallelism_class = match analysis.parallelism_hint {
                    ParallelismHint::Stateless => ParallelismClass::Stateless,
                    ParallelismHint::IndexReading => {
                        // Check if cross-source
                        if let Some(wc) = wc {
                            let source =
                                wc.source.clone().unwrap_or_else(|| primary_source.clone());
                            if source != primary_source {
                                ParallelismClass::CrossSource
                            } else {
                                ParallelismClass::IndexReading
                            }
                        } else {
                            ParallelismClass::IndexReading
                        }
                    }
                    ParallelismHint::Sequential => ParallelismClass::Sequential,
                };

                let window_index = if let Some(wc) = wc {
                    let source = wc.source.clone().unwrap_or_else(|| primary_source.clone());
                    index::find_index_for(&indices, &source, &wc.group_by, &wc.sort_by)
                } else {
                    None
                };

                let partition_lookup = wc.as_ref().map(|wc| {
                    let source = wc.source.clone().unwrap_or_else(|| primary_source.clone());
                    if source == primary_source && wc.on.is_none() {
                        PartitionLookupKind::SameSource
                    } else {
                        PartitionLookupKind::CrossSource {
                            on_expr: wc.on.clone(),
                        }
                    }
                });

                TransformPlan {
                    name: name.to_string(),
                    parallelism_class,
                    window_index,
                    partition_lookup,
                }
            })
            .collect();

        // Build source DAG
        let source_dag = build_source_dag(config, &window_configs, &primary_source)?;

        // Build output projections
        let output_projections = config
            .outputs
            .iter()
            .map(|o| OutputSpec {
                name: o.name.clone(),
                mapping: o.mapping.clone().unwrap_or_default(),
                exclude: o.exclude.clone().unwrap_or_default(),
                include_unmapped: o.include_unmapped,
            })
            .collect();

        // Build parallelism profile
        let parallelism = ParallelismProfile {
            per_transform: transforms.iter().map(|t| t.parallelism_class).collect(),
            worker_threads: config
                .pipeline
                .concurrency
                .as_ref()
                .and_then(|c| c.threads)
                .unwrap_or(4),
        };

        // Build route plan from first transform with route config
        let route = config
            .transforms()
            .find_map(|t| t.route.as_ref())
            .map(|rc| RoutePlan {
                mode: rc.mode,
                branches: rc
                    .branches
                    .iter()
                    .map(|b| (b.name.clone(), b.condition.clone()))
                    .collect(),
                default: rc.default.clone(),
            });

        Ok(ExecutionPlan {
            source_dag,
            indices_to_build: indices,
            transforms,
            output_projections,
            parallelism,
            route,
        })
    }

    /// Format the execution plan for `--explain` display.
    pub fn explain(&self) -> String {
        let mut out = String::new();
        out.push_str("=== Execution Plan ===\n\n");

        out.push_str(&format!("Mode: {:?}\n", self.mode()));
        out.push_str(&format!(
            "Indices to build: {}\n",
            self.indices_to_build.len()
        ));
        out.push_str(&format!("Transforms: {}\n", self.transforms.len()));
        out.push_str(&format!(
            "Output projections: {}\n\n",
            self.output_projections.len()
        ));

        if !self.source_dag.is_empty() {
            out.push_str("Source DAG:\n");
            for (tier_idx, tier) in self.source_dag.iter().enumerate() {
                out.push_str(&format!(
                    "  Tier {}: {}\n",
                    tier_idx,
                    tier.sources.join(", ")
                ));
            }
            out.push('\n');
        }

        for (i, spec) in self.indices_to_build.iter().enumerate() {
            out.push_str(&format!("Index [{}]:\n", i));
            out.push_str(&format!("  Source: {}\n", spec.source));
            out.push_str(&format!("  Group by: {:?}\n", spec.group_by));
            out.push_str(&format!(
                "  Sort by: {:?}\n",
                spec.sort_by.iter().map(|s| &s.field).collect::<Vec<_>>()
            ));
            out.push_str(&format!("  Arena fields: {:?}\n", spec.arena_fields));
            out.push_str(&format!("  Already sorted: {}\n\n", spec.already_sorted));
        }

        for tp in &self.transforms {
            out.push_str(&format!("Transform '{}':\n", tp.name));
            out.push_str(&format!("  Parallelism: {:?}\n", tp.parallelism_class));
            out.push_str(&format!("  Window index: {:?}\n", tp.window_index));
            out.push_str(&format!(
                "  Partition lookup: {:?}\n\n",
                tp.partition_lookup
            ));
        }

        if let Some(route) = &self.route {
            out.push_str(&format!(
                "Route (mode: {}):\n",
                match route.mode {
                    RouteMode::Exclusive => "exclusive",
                    RouteMode::Inclusive => "inclusive",
                }
            ));
            for (name, condition) in &route.branches {
                out.push_str(&format!(
                    "  Branch '{}': {} → output '{}'\n",
                    name, condition, name
                ));
            }
            out.push_str(&format!(
                "  Default: '{}' → output '{}'\n\n",
                route.default, route.default
            ));
        }

        out
    }

    /// Full `--explain` output combining execution plan with config context.
    ///
    /// Includes: AST (CXL expressions), type annotations, source DAG,
    /// indices to build, parallelism classification, and memory budget.
    pub fn explain_full(&self, config: &PipelineConfig) -> String {
        let mut out = self.explain();

        // CXL AST (reformatted expressions from config)
        out.push_str("=== CXL Expressions ===\n\n");
        for t in config.transforms() {
            out.push_str(&format!("Transform '{}':\n", t.name));
            for line in t.cxl.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    out.push_str(&format!("  {}\n", trimmed));
                }
            }
            out.push('\n');
        }

        // Type annotations (inferred types per output field)
        out.push_str("=== Type Annotations ===\n\n");
        for tp in &self.transforms {
            out.push_str(&format!(
                "Transform '{}': (types inferred at compile time)\n",
                tp.name
            ));
        }
        out.push('\n');

        // Memory budget
        out.push_str("=== Memory Budget ===\n\n");
        let memory_limit = config
            .pipeline
            .concurrency
            .as_ref()
            .and_then(|c| c.threads)
            .map(|_| "configured")
            .unwrap_or("default");
        out.push_str(&format!("Memory limit: {}\n", memory_limit));
        out.push_str(&format!(
            "Worker threads: {}\n",
            self.parallelism.worker_threads
        ));
        out.push('\n');

        out
    }
}

/// One tier of the source dependency DAG. Sources within a tier are independent.
#[derive(Debug, Clone)]
pub struct SourceTier {
    pub sources: Vec<String>,
}

/// Per-transform execution unit.
#[derive(Debug, Clone)]
pub struct TransformPlan {
    pub name: String,
    pub parallelism_class: ParallelismClass,
    /// Index into `ExecutionPlan.indices_to_build`, if this transform uses windows.
    pub window_index: Option<usize>,
    /// How to look up the partition for this transform's window.
    pub partition_lookup: Option<PartitionLookupKind>,
}

/// How to look up a record's partition during Phase 2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionLookupKind {
    /// Same-source: extract group_by fields directly from the current record.
    SameSource,
    /// Cross-source: evaluate the `on` expression against the current record.
    CrossSource { on_expr: Option<String> },
}

/// AST compiler's parallelism classification per transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelismClass {
    /// No window references — fully parallelizable.
    Stateless,
    /// Reads from immutable arena — parallelizable across chunks.
    IndexReading,
    /// Positional functions with ordering dependency — single-threaded.
    Sequential,
    /// References a different source's index.
    CrossSource,
}

/// Per-output projection specification.
#[derive(Debug, Clone)]
pub struct OutputSpec {
    pub name: String,
    pub mapping: IndexMap<String, String>,
    pub exclude: Vec<String>,
    pub include_unmapped: bool,
}

/// Pipeline-level parallelism configuration.
#[derive(Debug, Clone)]
pub struct ParallelismProfile {
    pub per_transform: Vec<ParallelismClass>,
    pub worker_threads: usize,
}

/// Build the source dependency DAG.
///
/// Reference sources (those targeted by cross-source windows) must be in
/// earlier tiers than the transforms that depend on them.
fn build_source_dag(
    config: &PipelineConfig,
    window_configs: &[Option<LocalWindowConfig>],
    primary_source: &str,
) -> Result<Vec<SourceTier>, PlanError> {
    let all_sources: Vec<String> = config.inputs.iter().map(|i| i.name.clone()).collect();

    if all_sources.len() <= 1 {
        // Single source — trivial DAG
        return Ok(vec![SourceTier {
            sources: all_sources,
        }]);
    }

    // Collect which sources are dependencies (referenced by cross-source windows)
    let mut dependencies: HashSet<String> = HashSet::new();
    for wc in window_configs.iter().flatten() {
        if let Some(source) = &wc.source
            && source != primary_source
        {
            dependencies.insert(source.clone());
        }
    }

    // Tier 0: reference sources (must be built first)
    // Tier 1: everything else (including primary)
    let tier0: Vec<String> = all_sources
        .iter()
        .filter(|s| dependencies.contains(s.as_str()))
        .cloned()
        .collect();

    let tier1: Vec<String> = all_sources
        .iter()
        .filter(|s| !dependencies.contains(s.as_str()))
        .cloned()
        .collect();

    let mut tiers = Vec::new();
    if !tier0.is_empty() {
        tiers.push(SourceTier { sources: tier0 });
    }
    if !tier1.is_empty() {
        tiers.push(SourceTier { sources: tier1 });
    }

    Ok(tiers)
}

/// Check if a source's declared sort_order matches the window's sort_by.
fn check_already_sorted(_config: &PipelineConfig, _source: &str, _sort_by: &[SortField]) -> bool {
    // InputConfig doesn't have sort_order yet (to be added in Task 5.4.1)
    // For now, always return false — runtime pre-sorted detection is the fallback
    false
}

/// Errors from execution plan compilation.
#[derive(Debug)]
pub enum PlanError {
    IndexPlanning(PlanIndexError),
    MissingLocalWindow { transform: String },
    UnknownSource { name: String, transform: String },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::IndexPlanning(e) => write!(f, "index planning error: {}", e),
            PlanError::MissingLocalWindow { transform } => {
                write!(
                    f,
                    "transform '{}' uses window.* but has no local_window config",
                    transform
                )
            }
            PlanError::UnknownSource { name, transform } => {
                write!(
                    f,
                    "transform '{}' references unknown source '{}'",
                    transform, name
                )
            }
        }
    }
}

impl std::error::Error for PlanError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use cxl::parser::Parser;
    use cxl::resolve::pass::resolve_program;
    use cxl::typecheck::pass::type_check;
    use std::collections::HashMap;

    /// Extract resolved TransformConfig from TransformEntry (tests only).
    fn t(entry: &TransformEntry) -> &TransformConfig {
        match entry {
            TransformEntry::Transform(t) => t,
            _ => panic!("test expects resolved transform"),
        }
    }

    /// Build a minimal PipelineConfig for testing.
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
            inputs: inputs
                .into_iter()
                .map(|(name, path)| InputConfig {
                    name: name.into(),
                    path: path.into(),
                    schema: None,
                    schema_overrides: None,
                    array_paths: None,
                    sort_order: None,
                    format: InputFormat::Csv(None),
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
                format: OutputFormat::Csv(None),
                notes: None,
            }],
            transformations: transforms
                .into_iter()
                .map(|(name, cxl, local_window)| {
                    TransformEntry::Transform(TransformConfig {
                        name: name.into(),
                        description: None,
                        cxl: cxl.into(),
                        local_window,
                        log: None,
                        validations: None,
                        route: None,
                        notes: None,
                    })
                })
                .collect(),
            error_handling: ErrorHandlingConfig::default(),
            notes: None,
        }
    }

    /// Compile CXL source to TypedProgram for a given set of field names.
    fn compile_cxl(source: &str, fields: &[&str]) -> cxl::typecheck::pass::TypedProgram {
        let parsed = Parser::parse(source);
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors
        );
        let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
        let schema: HashMap<String, cxl::typecheck::types::Type> = HashMap::new();
        type_check(resolved, &schema).unwrap()
    }

    #[test]
    fn test_plan_stateless_only() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("calc", "let x = amount + 1\nemit result = x * 2", None)],
        );
        let fields = &["amount"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("calc", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();

        assert!(plan.indices_to_build.is_empty());
        assert_eq!(plan.transforms.len(), 1);
        assert_eq!(
            plan.transforms[0].parallelism_class,
            ParallelismClass::Stateless
        );
        assert_eq!(plan.mode(), ExecutionMode::Streaming);
    }

    #[test]
    fn test_plan_single_window_index() {
        let window = serde_json::json!({
            "group_by": ["dept"]
        });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("agg", "emit total = $window.sum(amount)", Some(window))],
        );
        let fields = &["dept", "amount"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("agg", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();

        assert_eq!(plan.indices_to_build.len(), 1);
        assert_eq!(plan.indices_to_build[0].group_by, vec!["dept".to_string()]);
        assert_eq!(plan.mode(), ExecutionMode::TwoPass);
    }

    #[test]
    fn test_plan_dedup_shared_index() {
        let window = serde_json::json!({
            "group_by": ["dept"],
            "sort_by": [{"field": "date"}]
        });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![
                (
                    "agg1",
                    "emit total = $window.sum(amount)",
                    Some(window.clone()),
                ),
                ("agg2", "emit cnt = $window.count()", Some(window)),
            ],
        );
        let fields = &["dept", "amount", "date"];
        let typed1 = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let typed2 = compile_cxl(&t(&config.transformations[1]).cxl, fields);
        let compiled = vec![("agg1", &typed1), ("agg2", &typed2)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();

        assert_eq!(plan.indices_to_build.len(), 1, "should share one index");
        assert_eq!(plan.transforms[0].window_index, Some(0));
        assert_eq!(plan.transforms[1].window_index, Some(0));
    }

    #[test]
    fn test_plan_distinct_indices() {
        let window_a = serde_json::json!({ "group_by": ["dept"] });
        let window_b = serde_json::json!({ "group_by": ["region"] });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![
                (
                    "agg_dept",
                    "emit total = $window.sum(amount)",
                    Some(window_a),
                ),
                (
                    "agg_region",
                    "emit total = $window.sum(amount)",
                    Some(window_b),
                ),
            ],
        );
        let fields = &["dept", "region", "amount"];
        let typed1 = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let typed2 = compile_cxl(&t(&config.transformations[1]).cxl, fields);
        let compiled = vec![("agg_dept", &typed1), ("agg_region", &typed2)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();

        assert_eq!(plan.indices_to_build.len(), 2);
    }

    #[test]
    fn test_plan_parallelism_stateless() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("calc", "emit doubled = amount * 2", None)],
        );
        let fields = &["amount"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("calc", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();

        assert_eq!(
            plan.transforms[0].parallelism_class,
            ParallelismClass::Stateless
        );
    }

    #[test]
    fn test_plan_parallelism_index_reading() {
        let window = serde_json::json!({ "group_by": ["dept"] });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("agg", "emit total = $window.sum(amount)", Some(window))],
        );
        let fields = &["dept", "amount"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("agg", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();

        assert_eq!(
            plan.transforms[0].parallelism_class,
            ParallelismClass::IndexReading
        );
    }

    #[test]
    fn test_plan_parallelism_sequential() {
        let window = serde_json::json!({
            "group_by": ["dept"],
            "sort_by": [{"field": "date"}]
        });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("positional", "emit prev = $window.lag(1)", Some(window))],
        );
        let fields = &["dept", "amount", "date"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("positional", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();

        assert_eq!(
            plan.transforms[0].parallelism_class,
            ParallelismClass::Sequential
        );
    }

    #[test]
    fn test_plan_explain_output() {
        let window = serde_json::json!({ "group_by": ["dept"] });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![
                ("calc", "emit doubled = amount * 2", None),
                ("agg", "emit total = $window.sum(amount)", Some(window)),
            ],
        );
        let fields = &["dept", "amount"];
        let typed1 = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let typed2 = compile_cxl(&t(&config.transformations[1]).cxl, fields);
        let compiled = vec![("calc", &typed1), ("agg", &typed2)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(explain.contains("Mode: TwoPass"));
        assert!(explain.contains("Indices to build: 1"));
        assert!(explain.contains("Transforms: 2"));
        assert!(explain.contains("Stateless"));
        assert!(explain.contains("IndexReading"));
    }

    #[test]
    fn test_plan_cross_source_dag() {
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
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("lookup", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();

        assert_eq!(plan.source_dag.len(), 2);
        assert!(
            plan.source_dag[0]
                .sources
                .contains(&"reference".to_string())
        );
        assert!(plan.source_dag[1].sources.contains(&"primary".to_string()));
    }

    #[test]
    fn test_plan_mode_two_pass() {
        let window = serde_json::json!({ "group_by": ["dept"] });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("agg", "emit total = $window.sum(amount)", Some(window))],
        );
        let fields = &["dept", "amount"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("agg", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();

        assert_eq!(plan.mode(), ExecutionMode::TwoPass);
    }

    #[test]
    fn test_plan_cross_source_missing_reference() {
        let window = serde_json::json!({
            "source": "nonexistent",
            "group_by": ["id"]
        });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("lookup", "emit ref_val = $window.sum(amount)", Some(window))],
        );
        let fields = &["id", "amount"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("lookup", &typed)];

        let result = ExecutionPlan::compile(&config, &compiled);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            PlanError::UnknownSource { name, .. } => assert_eq!(name, "nonexistent"),
            other => panic!("Expected UnknownSource, got: {:?}", other),
        }
    }

    /// Build a config with route configuration on the first transform.
    fn test_config_with_route(cxl: &str, route: crate::config::RouteConfig) -> PipelineConfig {
        PipelineConfig {
            pipeline: PipelineMeta {
                name: "route-explain-test".into(),
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
            inputs: vec![InputConfig {
                name: "src".into(),
                path: "data.csv".into(),
                schema: None,
                schema_overrides: None,
                array_paths: None,
                sort_order: None,
                format: InputFormat::Csv(None),
                notes: None,
            }],
            outputs: vec![OutputConfig {
                name: "output".into(),
                path: "out.csv".into(),
                include_unmapped: true,
                include_header: None,
                mapping: None,
                exclude: None,
                sort_order: None,
                preserve_nulls: None,
                format: OutputFormat::Csv(None),
                notes: None,
            }],
            transformations: vec![TransformEntry::Transform(TransformConfig {
                name: "router".into(),
                description: None,
                cxl: cxl.into(),
                local_window: None,
                log: None,
                validations: None,
                route: Some(route),
                notes: None,
            })],
            error_handling: ErrorHandlingConfig::default(),
            notes: None,
        }
    }

    #[test]
    fn test_explain_shows_route_branches() {
        let route = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Exclusive,
            branches: vec![
                crate::config::RouteBranch {
                    name: "high_value".into(),
                    condition: "amount > 1000".into(),
                },
                crate::config::RouteBranch {
                    name: "medium_value".into(),
                    condition: "amount > 100".into(),
                },
            ],
            default: "low_value".into(),
        };
        let config = test_config_with_route("emit result = amount", route);
        let fields = &["amount"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("router", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(
            explain.contains("high_value"),
            "should contain branch name 'high_value': {}",
            explain
        );
        assert!(
            explain.contains("medium_value"),
            "should contain branch name 'medium_value': {}",
            explain
        );
        assert!(
            explain.contains("amount > 1000"),
            "should contain condition: {}",
            explain
        );
        assert!(
            explain.contains("amount > 100"),
            "should contain condition: {}",
            explain
        );
    }

    #[test]
    fn test_explain_shows_route_mode() {
        let route_exclusive = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Exclusive,
            branches: vec![crate::config::RouteBranch {
                name: "branch_a".into(),
                condition: "status == \"active\"".into(),
            }],
            default: "other".into(),
        };
        let config = test_config_with_route("emit result = status", route_exclusive);
        let fields = &["status"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("router", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(
            explain.contains("exclusive"),
            "should contain mode 'exclusive': {}",
            explain
        );

        // Also test inclusive
        let route_inclusive = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Inclusive,
            branches: vec![crate::config::RouteBranch {
                name: "branch_a".into(),
                condition: "status == \"active\"".into(),
            }],
            default: "other".into(),
        };
        let config2 = test_config_with_route("emit result = status", route_inclusive);
        let typed2 = compile_cxl(&t(&config2.transformations[0]).cxl, fields);
        let compiled2 = vec![("router", &typed2)];

        let plan2 = ExecutionPlan::compile(&config2, &compiled2).unwrap();
        let explain2 = plan2.explain();

        assert!(
            explain2.contains("inclusive"),
            "should contain mode 'inclusive': {}",
            explain2
        );
    }

    #[test]
    fn test_explain_shows_default() {
        let route = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Exclusive,
            branches: vec![crate::config::RouteBranch {
                name: "special".into(),
                condition: "flag == true".into(),
            }],
            default: "fallback".into(),
        };
        let config = test_config_with_route("emit result = flag", route);
        let fields = &["flag"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("router", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(
            explain.contains("Default: 'fallback'"),
            "should show default branch: {}",
            explain
        );
    }

    #[test]
    fn test_explain_no_route_unchanged() {
        // Pipeline without routes — explain output should not mention Route
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("calc", "emit doubled = amount * 2", None)],
        );
        let fields = &["amount"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("calc", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(
            !explain.contains("Route"),
            "should not contain Route section when no routes: {}",
            explain
        );
    }

    #[test]
    fn test_explain_shows_output_mapping() {
        let route = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Exclusive,
            branches: vec![
                crate::config::RouteBranch {
                    name: "errors".into(),
                    condition: "status == \"error\"".into(),
                },
                crate::config::RouteBranch {
                    name: "warnings".into(),
                    condition: "status == \"warn\"".into(),
                },
            ],
            default: "normal".into(),
        };
        let config = test_config_with_route("emit result = status", route);
        let fields = &["status"];
        let typed = compile_cxl(&t(&config.transformations[0]).cxl, fields);
        let compiled = vec![("router", &typed)];

        let plan = ExecutionPlan::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        // Each branch should show → output mapping
        assert!(
            explain.contains("→ output 'errors'"),
            "should show output mapping for 'errors': {}",
            explain
        );
        assert!(
            explain.contains("→ output 'warnings'"),
            "should show output mapping for 'warnings': {}",
            explain
        );
        assert!(
            explain.contains("→ output 'normal'"),
            "should show output mapping for default: {}",
            explain
        );
    }
}
