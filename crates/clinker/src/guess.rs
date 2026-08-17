//! Read-only schema concretization preview for inference-only `numeric` leaves.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use clinker_format::numeric_observation::{
    NumericAcceptance, NumericBoundary, NumericIssue, NumericObservation, NumericParserOutcome,
    NumericVote,
};
use clinker_format::{ByteTally, Column, NumericObserver, ReopenableSource, SourceSchema};
use clinker_plan::config::composition::ScopedSchemaLeafAddress;
use clinker_plan::config::{PipelineNode, SourceTransport};
use clinker_plan::error::PipelineError;
use cxl::typecheck::Type;
use indexmap::{IndexMap, IndexSet};
use serde::Serialize;

use crate::GuessArgs;

const MAX_FILES_PER_SOURCE: usize = 4;
const MAX_RECORDS_PER_FILE: u64 = 1_024;
const MAX_INPUT_BYTES_PER_FILE: u64 = 8 * 1_024 * 1_024;
const MAX_EVIDENCE_PER_OWNER: usize = 8;
const MAX_SCHEMA_LEAVES: usize = clinker_plan::yaml::MAX_NODES;

/// A classified `guess` failure. Selection/configuration errors are command
/// misuse (exit 1); source I/O and reader failures are infrastructure (exit 4).
#[derive(Debug)]
pub(crate) struct GuessError {
    kind: GuessErrorKind,
    message: String,
}

#[derive(Debug, Clone, Copy)]
enum GuessErrorKind {
    Configuration,
    Infrastructure,
}

impl GuessError {
    fn configuration(message: impl Into<String>) -> Self {
        Self {
            kind: GuessErrorKind::Configuration,
            message: message.into(),
        }
    }

    fn infrastructure(message: impl Into<String>) -> Self {
        Self {
            kind: GuessErrorKind::Infrastructure,
            message: message.into(),
        }
    }

    pub(crate) fn exit_code(&self) -> u8 {
        match self.kind {
            GuessErrorKind::Configuration => 1,
            GuessErrorKind::Infrastructure => 4,
        }
    }
}

impl fmt::Display for GuessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for GuessError {}

#[derive(Debug, Clone)]
struct Candidate {
    source: String,
    column: String,
    owners: Vec<NumericOwner>,
}

#[derive(Debug, Clone)]
struct NumericOwner {
    address: ScopedSchemaLeafAddress,
    declared_type: Type,
    record: Option<String>,
    observed_fields: Vec<String>,
    accumulator_index: usize,
}

impl Candidate {
    fn selector(&self) -> String {
        format!("{}.{}", self.source, self.column)
    }
}

#[derive(Debug, Clone, Default)]
struct FieldAccumulator {
    observed: u64,
    int_votes: u64,
    float_votes: u64,
    no_value_votes: u64,
    unresolved_votes: u64,
    all_ints_float_safe: bool,
    unresolved: BTreeSet<&'static str>,
    evidence: Vec<EvidenceReport>,
}

impl FieldAccumulator {
    fn new() -> Self {
        Self {
            all_ints_float_safe: true,
            ..Self::default()
        }
    }

    fn observe(&mut self, observation: &NumericObservation) {
        self.observed = self.observed.saturating_add(1);
        match observation.vote() {
            NumericVote::NoValue => self.no_value_votes = self.no_value_votes.saturating_add(1),
            NumericVote::Int => {
                self.int_votes = self.int_votes.saturating_add(1);
                if !matches!(
                    observation.float_acceptance(),
                    NumericAcceptance::Accepted(_)
                ) {
                    self.all_ints_float_safe = false;
                }
            }
            NumericVote::Float => {
                self.float_votes = self.float_votes.saturating_add(1);
            }
            NumericVote::Unresolved(issue) => {
                self.unresolved_votes = self.unresolved_votes.saturating_add(1);
                self.unresolved.insert(issue_label(issue));
            }
        }
        if self.evidence.len() < MAX_EVIDENCE_PER_OWNER {
            self.evidence.push(EvidenceReport::from(observation));
        }
    }

    fn resolution(&self) -> (Option<&'static str>, Vec<&'static str>) {
        let mut reasons = self.unresolved.iter().copied().collect::<Vec<_>>();
        if !reasons.is_empty() {
            return (None, reasons);
        }
        if self.float_votes > 0 && self.int_votes > 0 && !self.all_ints_float_safe {
            reasons.push("unsafe_integer_widening");
            return (None, reasons);
        }
        if self.float_votes > 0 {
            return (Some("float"), reasons);
        }
        if self.int_votes > 0 {
            return (Some("int"), reasons);
        }
        reasons.push("no_value_evidence");
        (None, reasons)
    }
}

#[derive(Debug, Serialize)]
struct GuessReport {
    schema: &'static str,
    version: u8,
    target: String,
    selection: SelectionReport,
    limits: LimitsReport,
    coverage: Vec<SourceCoverage>,
    fields: Vec<FieldReport>,
    patch: String,
}

#[derive(Debug, Serialize)]
struct SelectionReport {
    kind: &'static str,
    name: Option<String>,
    applied_groups: Vec<String>,
}

#[derive(Debug, Serialize)]
struct LimitsReport {
    max_yaml_input_bytes: usize,
    max_yaml_nodes_per_document: usize,
    max_schema_leaves: usize,
    max_files_per_source: usize,
    max_records_per_file: u64,
    max_input_bytes_per_file: u64,
    max_numeric_lexeme_evidence_bytes: usize,
    max_evidence_per_owner: usize,
}

#[derive(Debug, Serialize)]
struct SourceCoverage {
    source: String,
    format: &'static str,
    discovered_files: usize,
    unreported_file_count: usize,
    files: Vec<FileCoverage>,
}

#[derive(Debug, Serialize)]
struct FileCoverage {
    path: String,
    status: &'static str,
    input_bytes: Option<u64>,
    bytes_read: u64,
    records_sampled: u64,
    truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
struct EvidenceReport {
    boundary: &'static str,
    lexeme: String,
    original_bytes: usize,
    truncated: bool,
    parser_outcome: &'static str,
    vote: &'static str,
    reason: Option<&'static str>,
}

impl From<&NumericObservation> for EvidenceReport {
    fn from(observation: &NumericObservation) -> Self {
        let lexeme = observation.lexeme();
        let rendered = match lexeme.complete() {
            Some(complete) => complete.to_owned(),
            None => format!("{}…{}", lexeme.head(), lexeme.tail()),
        };
        let (vote, reason) = match observation.vote() {
            NumericVote::NoValue => ("no_value", None),
            NumericVote::Int => ("int", None),
            NumericVote::Float => ("float", None),
            NumericVote::Unresolved(issue) => ("unresolved", Some(issue_label(issue))),
        };
        Self {
            boundary: boundary_label(observation.boundary()),
            lexeme: rendered,
            original_bytes: lexeme.original_bytes(),
            truncated: lexeme.is_truncated(),
            parser_outcome: parser_outcome_label(observation.parser_outcome()),
            vote,
            reason,
        }
    }
}

#[derive(Debug, Serialize)]
struct FieldReport {
    field: String,
    owners: Vec<OwnerReport>,
}

#[derive(Debug, Serialize)]
struct OwnerReport {
    address: String,
    observations: u64,
    votes: VoteReport,
    evidence: Vec<EvidenceReport>,
    proposed_type: Option<&'static str>,
    unresolved_reasons: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct VoteReport {
    int: u64,
    float: u64,
    no_value: u64,
    unresolved: u64,
}

#[derive(Debug)]
struct EffectiveConfig {
    config: clinker_plan::config::PipelineConfig,
    selection: SelectionReport,
}

/// Execute the read-only preview and print one stable JSON document.
pub(crate) fn run(args: &GuessArgs) -> Result<u8, GuessError> {
    let effective = resolve_effective_config(args)?;
    let mut candidates = select_candidates(&effective.config, &args.fields)?;
    let accumulator_count = index_numeric_owners(&mut candidates);
    let accumulators = Arc::new(Mutex::new(
        (0..accumulator_count)
            .map(|_| FieldAccumulator::new())
            .collect::<Vec<_>>(),
    ));
    let config_dir = args
        .config
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(|error| {
            GuessError::infrastructure(format!(
                "cannot resolve pipeline directory for {}: {error}",
                args.config.display()
            ))
        })?;

    let coverage = sample_sources(
        &effective.config,
        &candidates,
        Arc::clone(&accumulators),
        &config_dir,
    )?;
    let accumulators = accumulators
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut edits = Vec::new();
    let fields = candidates
        .iter()
        .map(|candidate| {
            let owners = candidate
                .owners
                .iter()
                .map(|owner| {
                    let accumulated = &accumulators[owner.accumulator_index];
                    let (proposed_type, unresolved_reasons) = accumulated.resolution();
                    if let Some(concrete) = proposed_type {
                        edits.push(PatchEdit {
                            address: owner.address.render(),
                            from_type: render_numeric_type(&owner.declared_type, "numeric"),
                            to_type: render_numeric_type(&owner.declared_type, concrete),
                        });
                    }
                    OwnerReport {
                        address: owner.address.render(),
                        observations: accumulated.observed,
                        votes: VoteReport {
                            int: accumulated.int_votes,
                            float: accumulated.float_votes,
                            no_value: accumulated.no_value_votes,
                            unresolved: accumulated.unresolved_votes,
                        },
                        evidence: accumulated.evidence.clone(),
                        proposed_type,
                        unresolved_reasons,
                    }
                })
                .collect();
            FieldReport {
                field: candidate.selector(),
                owners,
            }
        })
        .collect();
    let patch = render_patch(&edits);
    let report = GuessReport {
        schema: "clinker.guess.preview",
        version: 1,
        target: effective.config.pipeline.name.clone(),
        selection: effective.selection,
        limits: LimitsReport {
            max_yaml_input_bytes: clinker_plan::yaml::MAX_INPUT_BYTES,
            max_yaml_nodes_per_document: clinker_plan::yaml::MAX_NODES,
            max_schema_leaves: MAX_SCHEMA_LEAVES,
            max_files_per_source: MAX_FILES_PER_SOURCE,
            max_records_per_file: MAX_RECORDS_PER_FILE,
            max_input_bytes_per_file: MAX_INPUT_BYTES_PER_FILE,
            max_numeric_lexeme_evidence_bytes:
                clinker_format::numeric_observation::MAX_NUMERIC_LEXEME_EVIDENCE_BYTES,
            max_evidence_per_owner: MAX_EVIDENCE_PER_OWNER,
        },
        coverage,
        fields,
        patch,
    };
    let output = serde_json::to_string_pretty(&report)
        .map_err(|error| GuessError::infrastructure(format!("cannot render preview: {error}")))?;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(output.as_bytes()).map_err(|error| {
        GuessError::infrastructure(format!("cannot write preview to stdout: {error}"))
    })?;
    stdout.write_all(b"\n").map_err(|error| {
        GuessError::infrastructure(format!("cannot finish preview on stdout: {error}"))
    })?;
    Ok(0)
}

fn resolve_effective_config(args: &GuessArgs) -> Result<EffectiveConfig, GuessError> {
    if args.channel.is_some() && args.group.is_some() {
        return Err(GuessError::configuration(
            "choose exactly one effective configuration: remove either `--channel ID` or `--group NAME`",
        ));
    }
    let (workspace_root, _) = crate::resolve_compile_anchor(&args.config, args.base_dir.as_deref());
    let clinker_toml = clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(|error| GuessError::configuration(format!("clinker.toml: {error}")))?;

    let overlay = if args.channel.is_none() && args.group.is_none() {
        None
    } else {
        if let Some(group_name) = args.group.as_deref() {
            let groups = clinker_channel::scan_groups(&clinker_toml.group, &workspace_root)
                .map_err(crate::diag_message("group scan failed"))
                .map_err(GuessError::configuration)?;
            let matches = groups
                .iter()
                .filter(|group| group.name == group_name)
                .count();
            if matches != 1 {
                return Err(GuessError::configuration(if matches == 0 {
                    format!(
                        "no group named {group_name:?}; correction: pass one name reported by `clinker channels group members NAME`"
                    )
                } else {
                    format!(
                        "group selector {group_name:?} is ambiguous across {matches} files; correction: keep one group declaration with that name"
                    )
                }));
            }
        }
        let catalog =
            clinker_plan::resources::WorkspaceCatalog::load(&workspace_root, &clinker_toml.catalog)
                .map_err(|error| GuessError::configuration(error.to_string()))?;
        let pipeline_id =
            crate::catalog_pipeline_id(&workspace_root, &clinker_toml.catalog, &args.config)
                .map_err(GuessError::configuration)?;
        let explicit_groups = args.group.iter().cloned().collect::<Vec<_>>();
        Some(
            clinker_channel::resolve_target_channel(
                &workspace_root,
                &catalog,
                &clinker_toml.group,
                &pipeline_id,
                args.channel.as_deref(),
                &explicit_groups,
                args.channel.is_some(),
            )
            .map_err(|error| {
                GuessError::configuration(format!(
                    "effective configuration selection failed: {error}; correction: choose one cataloged channel or one target-admitted group"
                ))
            })?,
        )
    };

    let empty_patches = indexmap::IndexMap::new();
    let source_patches = overlay
        .as_ref()
        .and_then(clinker_channel::OverlayResolution::source_patches)
        .unwrap_or(&empty_patches);
    let mut config =
        clinker_plan::config::load_config_with_vars_and_patches(&args.config, &[], source_patches)
            .map_err(|error| GuessError::configuration(error.to_string()))?;
    if let Some(resolution) = &overlay
        && !resolution.op_stream().is_empty()
    {
        config.nodes = clinker_plan::apply_overlay_ops(
            std::mem::take(&mut config.nodes),
            resolution.op_stream().to_vec(),
        )
        .map_err(|error| {
            GuessError::configuration(format!("cannot apply selected structural overlay: {error}"))
        })?;
    }

    let selection = match (args.channel.as_ref(), args.group.as_ref()) {
        (Some(channel), None) => SelectionReport {
            kind: "channel",
            name: Some(channel.clone()),
            applied_groups: overlay
                .as_ref()
                .map(|resolution| {
                    resolution
                        .applied_groups()
                        .iter()
                        .map(|group| group.name.clone())
                        .collect()
                })
                .unwrap_or_default(),
        },
        (None, Some(group)) => SelectionReport {
            kind: "group",
            name: Some(group.clone()),
            applied_groups: vec![group.clone()],
        },
        (None, None) => SelectionReport {
            kind: "base",
            name: None,
            applied_groups: Vec::new(),
        },
        (Some(_), Some(_)) => unreachable!("selector conflict returned above"),
    };
    Ok(EffectiveConfig { config, selection })
}

fn select_candidates(
    config: &clinker_plan::config::PipelineConfig,
    requested: &[String],
) -> Result<Vec<Candidate>, GuessError> {
    let mut candidates = IndexMap::new();
    let mut all_fields: HashMap<String, Vec<Type>> = HashMap::new();
    let mut schema_leaves = 0usize;
    for node in &config.nodes {
        let PipelineNode::Source {
            header,
            config: body,
        } = &node.value
        else {
            continue;
        };
        match &body.schema {
            SourceSchema::Columns(columns) => {
                for column in columns {
                    register_candidate_column(
                        &mut candidates,
                        &mut all_fields,
                        &mut schema_leaves,
                        &header.name,
                        None,
                        column,
                        true,
                    )?;
                }
            }
            SourceSchema::MultiRecord { record_types, .. } => {
                for record_type in record_types {
                    for column in &record_type.columns {
                        register_candidate_column(
                            &mut candidates,
                            &mut all_fields,
                            &mut schema_leaves,
                            &header.name,
                            Some(&record_type.id),
                            column,
                            false,
                        )?;
                    }
                }
            }
            SourceSchema::Generated(_) | SourceSchema::File(_) => {}
        }
    }
    if candidates.is_empty() {
        return Err(GuessError::configuration(
            "the selected effective configuration has no literal `numeric` source-schema leaves; correction: declare `type: numeric` on an inference-only source column",
        ));
    }
    if requested.is_empty() {
        return Ok(candidates.into_values().collect());
    }

    let mut requested_once = IndexSet::new();
    for field in requested {
        if field.split('.').count() != 2 || field.starts_with('.') || field.ends_with('.') {
            return Err(GuessError::configuration(format!(
                "invalid --field {field:?}; correction: use `--field node.column`"
            )));
        }
        requested_once.insert(field.clone());
    }
    let by_selector = candidates;
    requested_once
        .into_iter()
        .map(|field| {
            if let Some(candidate) = by_selector.get(&field) {
                return Ok(candidate.clone());
            }
            if let Some(concrete) = all_fields.get(&field) {
                let concrete = concrete
                    .iter()
                    .map(ToString::to_string)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(GuessError::configuration(format!(
                    "--field {field:?} has only concrete declaration(s) ({concrete}), not `numeric`; correction: remove that selector or point it at a literal `numeric` source column"
                )))
            } else {
                Err(GuessError::configuration(format!(
                    "unknown --field {field:?}; correction: use one of {}",
                    {
                        let mut selectors = by_selector.keys().cloned().collect::<Vec<_>>();
                        selectors.sort();
                        selectors.join(", ")
                    }
                )))
            }
        })
        .collect()
}

fn register_candidate_column(
    candidates: &mut IndexMap<String, Candidate>,
    all_fields: &mut HashMap<String, Vec<Type>>,
    schema_leaves: &mut usize,
    source: &str,
    record: Option<&str>,
    column: &Column,
    observe_physical_name: bool,
) -> Result<(), GuessError> {
    if *schema_leaves >= MAX_SCHEMA_LEAVES {
        return Err(GuessError::configuration(format!(
            "the selected effective configuration exceeds the guess limit of {MAX_SCHEMA_LEAVES} source-schema leaves (the canonical YAML node cap); correction: narrow the selected configuration"
        )));
    }
    *schema_leaves += 1;
    let selector = format!("{source}.{}", column.name);
    all_fields
        .entry(selector.clone())
        .or_default()
        .push(column.ty.clone());
    if !contains_numeric_leaf(&column.ty) {
        return Ok(());
    }
    let address = match record {
        Some(record) => {
            ScopedSchemaLeafAddress::record_column(source, record, &column.name, "type")
        }
        None => ScopedSchemaLeafAddress::column(source, &column.name, "type"),
    };
    let candidate = candidates.entry(selector).or_insert_with(|| Candidate {
        source: source.to_owned(),
        column: column.name.clone(),
        owners: Vec::new(),
    });
    let mut observed_fields = vec![column.name.clone()];
    if observe_physical_name {
        let physical = column.physical_name();
        if !observed_fields.iter().any(|name| name == physical) {
            observed_fields.push(physical.to_owned());
        }
    }
    candidate.owners.push(NumericOwner {
        address,
        declared_type: column.ty.clone(),
        record: record.map(str::to_owned),
        observed_fields,
        accumulator_index: usize::MAX,
    });
    Ok(())
}

fn index_numeric_owners(candidates: &mut [Candidate]) -> usize {
    let mut next = 0;
    for candidate in candidates {
        for owner in &mut candidate.owners {
            owner.accumulator_index = next;
            next += 1;
        }
    }
    next
}

fn sample_sources(
    config: &clinker_plan::config::PipelineConfig,
    candidates: &[Candidate],
    accumulators: Arc<Mutex<Vec<FieldAccumulator>>>,
    config_dir: &Path,
) -> Result<Vec<SourceCoverage>, GuessError> {
    let selected_sources = candidates
        .iter()
        .map(|candidate| candidate.source.as_str())
        .collect::<IndexSet<_>>();
    let mut coverage = Vec::new();
    for source_name in selected_sources {
        let body = config
            .nodes
            .iter()
            .find_map(|node| match &node.value {
                PipelineNode::Source { header, config } if header.name == source_name => {
                    Some(config)
                }
                _ => None,
            })
            .ok_or_else(|| {
                GuessError::configuration(format!(
                    "selected source {source_name:?} disappeared from the effective configuration"
                ))
            })?;
        if !matches!(&body.source.transport, SourceTransport::File) {
            coverage.push(SourceCoverage {
                source: source_name.to_owned(),
                format: body.source.format.format_name(),
                discovered_files: 0,
                unreported_file_count: 0,
                files: Vec::new(),
            });
            continue;
        }
        let discovered = clinker_plan::config::discovery::discover_bounded(
            &body.source,
            config_dir,
            MAX_FILES_PER_SOURCE,
        )
        .map_err(|error| match error {
            clinker_plan::config::discovery::DiscoveryError::Io(_) => GuessError::infrastructure(
                format!("source {source_name:?} discovery failed: {error}"),
            ),
            _ => GuessError::configuration(format!(
                "source {source_name:?} discovery failed: {error}"
            )),
        })?;
        let discovered_files = discovered.discovered_file_count();
        let unreported_file_count = discovered_files.saturating_sub(discovered.files().len());
        let mut files = Vec::with_capacity(discovered.files().len());
        for discovered_file in discovered.files() {
            let path = &discovered_file.path;
            let display_path = stable_input_path(path, config_dir);
            if discovered_file.size > MAX_INPUT_BYTES_PER_FILE {
                files.push(FileCoverage {
                    path: display_path,
                    status: "uncovered_file_byte_limit",
                    input_bytes: Some(discovered_file.size),
                    bytes_read: 0,
                    records_sampled: 0,
                    truncated: true,
                });
                continue;
            }

            let field_map = observer_field_map(candidates, source_name);
            let target = Arc::clone(&accumulators);
            let observer = NumericObserver::new_scoped(move |scope, observation| {
                let Some(indices) = field_map.indices(scope.record(), scope.field()) else {
                    return;
                };
                let mut accumulated = target
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for index in indices {
                    accumulated[*index].observe(&observation);
                }
            });
            let tally = ByteTally::new();
            let source = ReopenableSource::path(path).with_tally(tally.clone());
            let mut reader = clinker_exec::executor::build_source_format_reader(
                &body.source,
                &body.schema,
                body.on_unmapped.clone(),
                source,
                Some(observer),
            )
            .map_err(reader_error)?;
            let mut records = 0u64;
            while records < MAX_RECORDS_PER_FILE {
                match reader.next_record().map_err(|error| {
                    GuessError::infrastructure(format!(
                        "cannot sample source {source_name:?} file {display_path}: {error}"
                    ))
                })? {
                    Some(_) => records = records.saturating_add(1),
                    None => break,
                }
            }
            let truncated = records == MAX_RECORDS_PER_FILE;
            files.push(FileCoverage {
                path: display_path,
                status: if truncated {
                    "truncated_record_limit"
                } else {
                    "sampled"
                },
                input_bytes: Some(discovered_file.size),
                bytes_read: tally.read(),
                records_sampled: records,
                truncated,
            });
        }
        coverage.push(SourceCoverage {
            source: source_name.to_owned(),
            format: body.source.format.format_name(),
            discovered_files,
            unreported_file_count,
            files,
        });
    }
    Ok(coverage)
}

#[derive(Debug, Default)]
struct ObserverFieldMap {
    columns: HashMap<String, Vec<usize>>,
    records: HashMap<String, HashMap<String, Vec<usize>>>,
}

impl ObserverFieldMap {
    fn indices(&self, record: Option<&str>, field: &str) -> Option<&[usize]> {
        match record {
            Some(record) => self
                .records
                .get(record)
                .and_then(|fields| fields.get(field))
                .map(Vec::as_slice),
            None => self.columns.get(field).map(Vec::as_slice),
        }
    }
}

fn observer_field_map(candidates: &[Candidate], source: &str) -> ObserverFieldMap {
    let mut fields = ObserverFieldMap::default();
    for candidate in candidates {
        if candidate.source != source {
            continue;
        }
        for owner in &candidate.owners {
            let target = match owner.record.as_deref() {
                Some(record) => fields.records.entry(record.to_owned()).or_default(),
                None => &mut fields.columns,
            };
            for name in &owner.observed_fields {
                let indices = target.entry(name.clone()).or_default();
                if !indices.contains(&owner.accumulator_index) {
                    indices.push(owner.accumulator_index);
                }
            }
        }
    }
    fields
}

fn reader_error(error: PipelineError) -> GuessError {
    match error {
        PipelineError::Config(_) | PipelineError::Compilation { .. } => {
            GuessError::configuration(error.to_string())
        }
        _ => GuessError::infrastructure(error.to_string()),
    }
}

fn stable_input_path(path: &Path, config_dir: &Path) -> String {
    path.strip_prefix(config_dir)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

struct PatchEdit {
    address: String,
    from_type: String,
    to_type: String,
}

fn render_patch(edits: &[PatchEdit]) -> String {
    let mut patch = String::from("edits:\n");
    for edit in edits {
        patch.push_str(&format!(
            "  - address: {}\n    from: {}\n    to: {}\n",
            edit.address, edit.from_type, edit.to_type
        ));
    }
    patch
}

fn render_numeric_type(declared: &Type, numeric_leaf: &str) -> String {
    match declared {
        Type::Numeric => numeric_leaf.to_owned(),
        Type::Nullable(inner) => {
            format!("nullable({})", render_numeric_type(inner, numeric_leaf))
        }
        _ => unreachable!("guess candidates contain a literal numeric leaf"),
    }
}

fn contains_numeric_leaf(ty: &Type) -> bool {
    match ty {
        Type::Numeric => true,
        Type::Nullable(inner) => contains_numeric_leaf(inner),
        Type::Null
        | Type::Bool
        | Type::Int
        | Type::Float
        | Type::Decimal
        | Type::String
        | Type::Date
        | Type::DateTime
        | Type::Array
        | Type::Map
        | Type::Any => false,
    }
}

fn boundary_label(boundary: NumericBoundary) -> &'static str {
    match boundary {
        NumericBoundary::Json => "json",
        NumericBoundary::Xml => "xml",
        NumericBoundary::Positional => "positional",
        NumericBoundary::SchemaCoerce => "schema_coerce",
    }
}

fn issue_label(issue: NumericIssue) -> &'static str {
    match issue {
        NumericIssue::InvalidNumeric => "invalid_numeric",
        NumericIssue::IntegerOverflow => "integer_overflow",
        NumericIssue::NonFinite => "non_finite",
        NumericIssue::UnsafeIntegerWidening => "unsafe_integer_widening",
        NumericIssue::UnderflowToZero => "underflow_to_zero",
        NumericIssue::PrecisionLoss => "precision_loss",
        NumericIssue::RepresentationChanged => "representation_changed",
    }
}

fn parser_outcome_label(outcome: &NumericParserOutcome) -> &'static str {
    match outcome {
        NumericParserOutcome::NoValue => "no_value",
        NumericParserOutcome::Integer(_) => "integer",
        NumericParserOutcome::Float(_) => "float",
        NumericParserOutcome::NonNumeric => "non_numeric",
        NumericParserOutcome::Rejected(_) => "rejected",
    }
}
