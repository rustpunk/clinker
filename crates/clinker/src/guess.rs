//! Schema concretization preview, check, and guarded write for `numeric` leaves.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clinker_format::numeric_observation::{
    NumericAcceptance, NumericBoundary, NumericIssue, NumericObservation, NumericParserOutcome,
    NumericVote, observe_schema_numeric,
};
use clinker_format::{
    ByteTally, Charset, Column, FormatReader, NumericObserver, RECORD_TYPE_COLUMN,
    ReopenableSource, SourceSchema, SplitValues,
};
use clinker_plan::config::composition::ScopedSchemaLeafAddress;
use clinker_plan::config::patch::{MultiplicityConfigEdit, ScopedColumnAddress};
use clinker_plan::config::{InputFormat, PipelineNode, SourceBody, SourceTransport};
use clinker_plan::error::PipelineError;
use clinker_record::{Record, Value};
use cxl::typecheck::Type;
use fs4::FileExt;
use indexmap::{IndexMap, IndexSet};
use serde::Serialize;

use crate::GuessArgs;

const MAX_MANIFEST_FILES: usize = 4_096;
const MAX_FILE_OPENS_TOTAL: usize = 4;
const MAX_RECORDS_TOTAL: u64 = 1_024;
const MAX_INPUT_BYTES_TOTAL: u64 = 8 * 1_024 * 1_024;
const MAX_REPORTED_FILES_PER_SOURCE: usize = 4;
const MAX_EVIDENCE_PER_OWNER: usize = 8;
const MAX_SCHEMA_LEAVES: usize = clinker_plan::yaml::MAX_NODES;
const MAX_CSV_DELIMITER_CANDIDATES: usize = 16;

/// A classified `guess` failure. Selection/configuration errors are command
/// misuse (exit 1), source I/O and reader failures are infrastructure (exit
/// 4), and cooperative interruption exits 130 without a partial report.
#[derive(Debug)]
pub(crate) struct GuessError {
    kind: GuessErrorKind,
    message: String,
}

#[derive(Debug, Clone, Copy)]
enum GuessErrorKind {
    Configuration,
    Infrastructure,
    Interrupted,
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

    fn interrupted() -> Self {
        Self {
            kind: GuessErrorKind::Interrupted,
            message: "interrupted before the guess report was complete".to_owned(),
        }
    }

    pub(crate) fn exit_code(&self) -> u8 {
        match self.kind {
            GuessErrorKind::Configuration => 1,
            GuessErrorKind::Infrastructure => 4,
            GuessErrorKind::Interrupted => 130,
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
    absence_policy: AbsencePolicy,
    default_observation: Option<NumericObservation>,
    accumulator_index: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct AbsencePolicy {
    nullable: bool,
    required: bool,
    has_default: bool,
}

impl Candidate {
    fn selector(&self) -> String {
        format!("{}.{}", self.source, self.column)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultiplicityFormat {
    Xml,
    Json,
    Csv(Charset),
}

impl MultiplicityFormat {
    fn label(self) -> &'static str {
        match self {
            Self::Xml => "xml",
            Self::Json => "json",
            Self::Csv(_) => "csv",
        }
    }
}

#[derive(Debug, Clone)]
struct MultiplicityCandidate {
    source: String,
    column: String,
    physical_field: String,
    address: ScopedColumnAddress,
    format: MultiplicityFormat,
    accumulator_index: usize,
}

impl MultiplicityCandidate {
    fn selector(&self) -> String {
        format!("{}.{}", self.source, self.column)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CsvInterpretation {
    delimiter: char,
    escape: Option<char>,
}

#[derive(Debug, Clone)]
struct CsvInterpretationState {
    interpretation: CsvInterpretation,
    viable: bool,
    delimiter_seen: bool,
    escape_activated: bool,
    multi_records: u64,
}

#[derive(Debug, Clone)]
struct MultiplicityAccumulator {
    format: MultiplicityFormat,
    observations: u64,
    empty_records: u64,
    singleton_records: u64,
    multi_records: u64,
    delimiter_candidates: BTreeSet<char>,
    candidate_overflow: bool,
    csv: Vec<CsvInterpretationState>,
}

impl MultiplicityAccumulator {
    fn new(format: MultiplicityFormat) -> Self {
        let csv = if matches!(format, MultiplicityFormat::Csv(_)) {
            csv_interpretations()
        } else {
            Vec::new()
        };
        Self {
            format,
            observations: 0,
            empty_records: 0,
            singleton_records: 0,
            multi_records: 0,
            delimiter_candidates: BTreeSet::new(),
            candidate_overflow: false,
            csv,
        }
    }

    fn observe(&mut self, value: Option<&Value>) -> Result<(), GuessError> {
        self.observations = self.observations.saturating_add(1);
        match self.format {
            MultiplicityFormat::Xml | MultiplicityFormat::Json => {
                let count = match value {
                    None | Some(Value::Null) => 0,
                    Some(Value::Array(values)) => values.len(),
                    Some(_) => 1,
                };
                self.observe_count(count);
                Ok(())
            }
            MultiplicityFormat::Csv(charset) => match value {
                None | Some(Value::Null) => {
                    self.empty_records = self.empty_records.saturating_add(1);
                    Ok(())
                }
                Some(Value::String(text)) => self.observe_csv(text.as_str(), charset),
                Some(_) => {
                    self.singleton_records = self.singleton_records.saturating_add(1);
                    Ok(())
                }
            },
        }
    }

    fn observe_count(&mut self, count: usize) {
        match count {
            0 => self.empty_records = self.empty_records.saturating_add(1),
            1 => self.singleton_records = self.singleton_records.saturating_add(1),
            _ => self.multi_records = self.multi_records.saturating_add(1),
        }
    }

    fn observe_csv(&mut self, text: &str, charset: Charset) -> Result<(), GuessError> {
        for character in text
            .chars()
            .filter(|character| is_csv_candidate(*character))
        {
            self.delimiter_candidates.insert(character);
        }
        if self.delimiter_candidates.len() > MAX_CSV_DELIMITER_CANDIDATES {
            self.candidate_overflow = true;
        }
        let original = charset
            .encode(text)
            .map_err(|error| GuessError::infrastructure(error.to_string()))?;
        for state in &mut self.csv {
            if !state.viable {
                continue;
            }
            let interpretation = state.interpretation;
            let activated = interpretation.escape.is_some_and(|escape| {
                text.contains(&format!("{escape}{}", interpretation.delimiter))
                    || text.contains(&format!("{escape}{escape}"))
            });
            state.escape_activated |= activated;
            state.delimiter_seen |= text.contains(interpretation.delimiter);
            let fields = split_csv_cell(text, interpretation);
            let rendered = join_csv_cell(&fields, interpretation);
            let encoded = charset
                .encode(&rendered)
                .map_err(|error| GuessError::infrastructure(error.to_string()))?;
            state.viable = encoded == original;
            if state.viable && fields.len() > 1 {
                state.multi_records = state.multi_records.saturating_add(1);
            }
        }
        Ok(())
    }

    fn resolution(&self) -> MultiplicityResolution {
        match self.format {
            MultiplicityFormat::Xml | MultiplicityFormat::Json if self.multi_records > 0 => {
                MultiplicityResolution::Conclusive(MultiplicityConfigEdit::native())
            }
            MultiplicityFormat::Xml | MultiplicityFormat::Json => {
                MultiplicityResolution::Unconfirmed("same_record_multiplicity_unconfirmed")
            }
            MultiplicityFormat::Csv(_) if self.candidate_overflow => {
                MultiplicityResolution::ReviewOnly("candidate_bound_exceeded")
            }
            MultiplicityFormat::Csv(_) => {
                let candidates = self.csv_candidates();
                match candidates.as_slice() {
                    [interpretation] => MultiplicityResolution::Conclusive(
                        MultiplicityConfigEdit::delimited(SplitValues {
                            field: String::new(),
                            delimiter: interpretation.delimiter.to_string(),
                            escape: interpretation
                                .escape
                                .map_or_else(String::new, |c| c.to_string()),
                            json: false,
                        }),
                    ),
                    [] => MultiplicityResolution::Unconfirmed("no_lossless_interpretation"),
                    _ => MultiplicityResolution::ReviewOnly("ambiguous_interpretation"),
                }
            }
        }
    }

    fn csv_candidates(&self) -> Vec<CsvInterpretation> {
        let activated_escapes = self
            .csv
            .iter()
            .filter(|state| state.viable && state.escape_activated)
            .map(|state| state.interpretation.delimiter)
            .collect::<HashSet<_>>();
        self.csv
            .iter()
            .filter(|state| {
                state.viable
                    && state.delimiter_seen
                    && state.multi_records > 0
                    && self
                        .delimiter_candidates
                        .contains(&state.interpretation.delimiter)
                    && match state.interpretation.escape {
                        Some(_) => state.escape_activated,
                        None => !activated_escapes.contains(&state.interpretation.delimiter),
                    }
            })
            .map(|state| state.interpretation)
            .collect()
    }
}

enum MultiplicityResolution {
    Conclusive(MultiplicityConfigEdit),
    Unconfirmed(&'static str),
    ReviewOnly(&'static str),
}

#[derive(Debug, Clone, Default)]
struct FieldAccumulator {
    absence_policy: AbsencePolicy,
    observed: u64,
    int_votes: u64,
    float_votes: u64,
    no_value_votes: u64,
    unresolved_votes: u64,
    accepted_absences: u64,
    forbidden_absences: u64,
    default_votes: u64,
    all_ints_float_safe: bool,
    unresolved: BTreeSet<&'static str>,
    evidence: Vec<EvidenceReport>,
}

impl FieldAccumulator {
    fn new(owner: &NumericOwner) -> Self {
        let mut accumulator = Self {
            absence_policy: owner.absence_policy,
            all_ints_float_safe: true,
            ..Self::default()
        };
        if let Some(observation) = owner.default_observation.as_ref() {
            accumulator.default_votes = 1;
            accumulator.observe_with_policy(observation, "default", false);
        }
        accumulator
    }

    fn observe(&mut self, observation: &NumericObservation) {
        self.observe_with_policy(observation, "input", false);
    }

    fn observe_missing(&mut self, observation: &NumericObservation) {
        self.observe_with_policy(observation, "missing", true);
    }

    fn observe_with_policy(
        &mut self,
        observation: &NumericObservation,
        origin: &'static str,
        missing: bool,
    ) {
        self.observed = self.observed.saturating_add(1);
        match observation.vote() {
            NumericVote::NoValue => {
                self.no_value_votes = self.no_value_votes.saturating_add(1);
                let default_applies = missing && self.absence_policy.has_default;
                if default_applies
                    || (self.absence_policy.nullable && !self.absence_policy.required)
                {
                    self.accepted_absences = self.accepted_absences.saturating_add(1);
                } else {
                    self.forbidden_absences = self.forbidden_absences.saturating_add(1);
                    self.unresolved.insert("forbidden_absence");
                }
            }
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
            self.evidence
                .push(EvidenceReport::from_observation(observation, origin));
        }
    }

    fn missing_parser_observation(&mut self) {
        self.unresolved.insert("missing_parser_observation");
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
    mode: &'static str,
    exhaustive: bool,
    outcome: &'static str,
    target: String,
    selection: SelectionReport,
    manifest: ManifestReport,
    limits: LimitsReport,
    coverage: Vec<SourceCoverage>,
    fields: Vec<FieldReport>,
    multiplicity: Vec<MultiplicityReport>,
    patch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    write: Option<WriteReport>,
}

#[derive(Debug, Serialize)]
struct WriteReport {
    status: &'static str,
    reason: Option<&'static str>,
    owner: Option<String>,
}

impl WriteReport {
    fn written(owner: String) -> Self {
        Self {
            status: "written",
            reason: None,
            owner: Some(owner),
        }
    }

    fn not_written(reason: &'static str, owner: Option<String>) -> Self {
        Self {
            status: "not_written",
            reason: Some(reason),
            owner,
        }
    }
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
    max_manifest_files: usize,
    preview_max_file_opens_total: usize,
    preview_max_records_total: u64,
    preview_max_input_bytes_total: u64,
    max_reported_files_per_source: usize,
    max_numeric_lexeme_evidence_bytes: usize,
    max_evidence_per_owner: usize,
    max_csv_delimiter_candidates: usize,
}

#[derive(Debug, Serialize)]
struct MultiplicityReport {
    field: String,
    address: String,
    format: &'static str,
    observations: u64,
    empty_records: u64,
    singleton_records: u64,
    multi_records: u64,
    outcome: &'static str,
    reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct SourceCoverage {
    source: String,
    format: &'static str,
    discovered_files: usize,
    sampled_files: usize,
    truncated_files: usize,
    uncovered_files: usize,
    unreported_file_count: usize,
    sampled_input_bytes: u64,
    bytes_read: u64,
    records_sampled: u64,
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
    origin: &'static str,
    boundary: &'static str,
    lexeme: String,
    original_bytes: usize,
    truncated: bool,
    parser_outcome: &'static str,
    vote: &'static str,
    reason: Option<&'static str>,
}

impl EvidenceReport {
    fn from_observation(observation: &NumericObservation, origin: &'static str) -> Self {
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
            origin,
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
    absence: AbsenceReport,
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

#[derive(Debug, Serialize)]
struct AbsenceReport {
    accepted: u64,
    forbidden: u64,
    default_votes: u64,
}

#[derive(Debug, Serialize)]
struct ManifestReport {
    schema: &'static str,
    version: u8,
    identity_basis: &'static str,
    preview_strata: [&'static str; 2],
    total_files: usize,
    sources: Vec<ManifestSourceReport>,
}

#[derive(Debug, Serialize)]
struct ManifestSourceReport {
    source: String,
    discovered_files: usize,
    identity: String,
}

#[derive(Debug)]
struct EffectiveConfig {
    config: clinker_plan::config::PipelineConfig,
    selection: SelectionReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GuessMode {
    Preview,
    Check,
    Write,
}

impl GuessMode {
    fn from_args(args: &GuessArgs) -> Result<Self, GuessError> {
        if args.check && args.write {
            return Err(GuessError::configuration(
                "--check and --write are distinct exhaustive modes; correction: remove one of the two flags",
            ));
        }
        Ok(if args.write {
            Self::Write
        } else if args.check {
            Self::Check
        } else {
            Self::Preview
        })
    }

    fn label(self) -> &'static str {
        match self {
            Self::Preview => "preview",
            Self::Check => "check",
            Self::Write => "write",
        }
    }

    fn exhaustive(self) -> bool {
        matches!(self, Self::Check | Self::Write)
    }
}

#[derive(Clone, Copy)]
enum GuessTerminalOutcome {
    Completed,
    Unresolved,
    Failed,
    Interrupted,
}

impl GuessTerminalOutcome {
    #[cfg(feature = "otlp")]
    const fn metric_key(self) -> clinker_exec::telemetry::MetricKey {
        match self {
            Self::Completed => clinker_exec::telemetry::MetricKey::GuessCompleted,
            Self::Unresolved => clinker_exec::telemetry::MetricKey::GuessUnresolved,
            Self::Failed => clinker_exec::telemetry::MetricKey::GuessFailed,
            Self::Interrupted => clinker_exec::telemetry::MetricKey::GuessInterrupted,
        }
    }

    #[cfg(feature = "otlp")]
    const fn span_status(self) -> clinker_exec::telemetry::SpanStatus {
        match self {
            Self::Completed => clinker_exec::telemetry::SpanStatus::Ok,
            Self::Unresolved | Self::Failed | Self::Interrupted => {
                clinker_exec::telemetry::SpanStatus::Error
            }
        }
    }
}

#[derive(Clone, Copy)]
struct GuessSuccess {
    exit_code: u8,
    outcome: GuessTerminalOutcome,
}

/// Optional delivery state for the mandatory, fixed Guess signal vocabulary.
///
/// A deployment with no OTLP policy retains nothing. When configured, the
/// existing arena owns all retained bytes and the worker remains best effort:
/// failure to resolve, reserve, start, admit, or deliver telemetry never
/// changes the authoring command result.
struct GuessTelemetry {
    #[cfg(feature = "otlp")]
    live: Option<GuessTelemetryLive>,
}

#[cfg(feature = "otlp")]
struct GuessTelemetryLive {
    producer: clinker_exec::telemetry::TelemetryProducer,
    worker: crate::observability::OtlpWorker,
}

impl GuessTelemetry {
    fn start(args: &GuessArgs, shutdown: clinker_exec::pipeline::shutdown::ShutdownToken) -> Self {
        #[cfg(feature = "otlp")]
        {
            let live = Self::try_start(args, shutdown);
            Self { live }
        }
        #[cfg(not(feature = "otlp"))]
        {
            let _ = (args, shutdown);
            Self {}
        }
    }

    #[cfg(feature = "otlp")]
    fn try_start(
        args: &GuessArgs,
        shutdown: clinker_exec::pipeline::shutdown::ShutdownToken,
    ) -> Option<GuessTelemetryLive> {
        let (workspace_root, _) =
            crate::resolve_compile_anchor(&args.config, args.base_dir.as_deref());
        let clinker_toml =
            clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root).ok()?;
        let policy = clinker_toml.resolve_observability(None).ok()?;
        let runtime = crate::observability::OtlpRuntimeBundle::admit(&policy)
            .ok()
            .flatten()?;
        let (producer, receiver) = runtime.reserve_arena(&policy).ok()?;
        let correlation = clinker_exec::telemetry::RunCorrelation::bounded(
            "clinker.guess",
            "clinker.guess",
            "clinker.guess",
        );
        let worker =
            crate::observability::OtlpWorker::start(runtime, receiver, shutdown, correlation)
                .ok()?;
        producer.record_metric(clinker_exec::telemetry::MetricKey::GuessStarted, 1);
        Some(GuessTelemetryLive { producer, worker })
    }

    fn finish(self, outcome: GuessTerminalOutcome, started_at_unix_nanos: u64) {
        #[cfg(feature = "otlp")]
        if let Some(live) = self.live {
            live.producer.record_metric(outcome.metric_key(), 1);
            let ended_at_unix_nanos =
                clinker_exec::telemetry::unix_nanos_now().max(started_at_unix_nanos);
            let _ = live.producer.emit_span(clinker_exec::telemetry::SpanFact {
                name: clinker_exec::telemetry::SpanName::Guess,
                status: outcome.span_status(),
                logical_node: "guess",
                started_at_unix_nanos,
                ended_at_unix_nanos,
            });
            drop(live.producer);
            drop(live.worker);
        }
        #[cfg(not(feature = "otlp"))]
        {
            let _ = (self, outcome, started_at_unix_nanos);
        }
    }
}

/// Execute a preview, exhaustive check, or guarded single-owner write and print
/// one stable JSON document.
pub(crate) fn run(args: &GuessArgs) -> Result<u8, GuessError> {
    let started_at_unix_nanos = clinker_exec::telemetry::unix_nanos_now();
    let shutdown = clinker_exec::pipeline::shutdown::ShutdownToken::new();
    let telemetry = GuessTelemetry::start(args, shutdown.clone());
    let result = run_inner(args, &shutdown);
    let outcome = match &result {
        Ok(success) => success.outcome,
        Err(error) if matches!(error.kind, GuessErrorKind::Interrupted) => {
            GuessTerminalOutcome::Interrupted
        }
        Err(_) => GuessTerminalOutcome::Failed,
    };
    telemetry.finish(outcome, started_at_unix_nanos);
    result.map(|success| success.exit_code)
}

fn run_inner(
    args: &GuessArgs,
    shutdown: &clinker_exec::pipeline::shutdown::ShutdownToken,
) -> Result<GuessSuccess, GuessError> {
    let mode = GuessMode::from_args(args)?;
    let config_snapshot = if matches!(mode, GuessMode::Write) {
        Some(snapshot_config(&args.config)?)
    } else {
        None
    };
    let effective = resolve_effective_config(args)?;
    let effective_digest =
        clinker_plan::config::canonical::semantic_config_digest(&effective.config)
            .map_err(|error| GuessError::infrastructure(error.to_string()))?;
    let config_unchanged_for_analysis = if let Some(snapshot) = &config_snapshot {
        snapshot_config(&args.config)?.raw == snapshot.raw
    } else {
        true
    };
    let mut selected = select_candidates(&effective.config, &args.fields)?;
    index_numeric_owners(&mut selected.numeric);
    let accumulators = Arc::new(Mutex::new(build_accumulators(&selected.numeric)));
    let multiplicity_accumulators = Arc::new(Mutex::new(
        selected
            .multiplicity
            .iter()
            .map(|candidate| MultiplicityAccumulator::new(candidate.format))
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
    let manifest = freeze_manifest(&effective.config, &selected, &config_dir)?;
    let input_snapshot = if matches!(mode, GuessMode::Write) {
        Some(InputSnapshot::capture(&manifest)?)
    } else {
        None
    };
    let manifest_report = ManifestReport {
        schema: "clinker.guess.manifest",
        version: 1,
        identity_basis: "blake3-path-size-v1",
        preview_strata: ["source", "file"],
        total_files: manifest.iter().fold(0usize, |total, source| {
            total.saturating_add(source.files.len())
        }),
        sources: manifest
            .iter()
            .map(|source| ManifestSourceReport {
                source: source.source.clone(),
                discovered_files: source.files.len(),
                identity: source.identity.clone(),
            })
            .collect(),
    };
    let coverage = match mode {
        GuessMode::Preview => sample_sources_fairly(
            &manifest,
            &selected,
            Arc::clone(&accumulators),
            Arc::clone(&multiplicity_accumulators),
            &config_dir,
            shutdown,
        )?,
        GuessMode::Check => check_sources_exhaustively(
            &manifest,
            &selected,
            Arc::clone(&accumulators),
            Arc::clone(&multiplicity_accumulators),
            &config_dir,
            shutdown,
        )?,
        GuessMode::Write => check_sources_exhaustively(
            &manifest,
            &selected,
            Arc::clone(&accumulators),
            Arc::clone(&multiplicity_accumulators),
            &config_dir,
            shutdown,
        )?,
    };
    if shutdown.is_requested() {
        return Err(GuessError::interrupted());
    }
    let accumulators = accumulators
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut edits = Vec::new();
    let mut resolved = true;
    let fields = selected
        .numeric
        .iter()
        .map(|candidate| {
            let owners = candidate
                .owners
                .iter()
                .map(|owner| {
                    let accumulated = &accumulators[owner.accumulator_index];
                    let (proposed_type, unresolved_reasons) = accumulated.resolution();
                    if let Some(concrete) = proposed_type {
                        let replacement = match concrete {
                            "int" => clinker_plan::config::canonical::ConcreteNumericType::Int,
                            "float" => clinker_plan::config::canonical::ConcreteNumericType::Float,
                            _ => unreachable!("numeric resolution returns int or float"),
                        };
                        edits.push(PatchEdit::Numeric {
                            owner: owner.address.clone(),
                            replacement,
                            address: owner.address.render(),
                            from_type: render_numeric_type(&owner.declared_type, "numeric"),
                            to_type: render_numeric_type(&owner.declared_type, concrete),
                        });
                    } else {
                        resolved = false;
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
                        absence: AbsenceReport {
                            accepted: accumulated.accepted_absences,
                            forbidden: accumulated.forbidden_absences,
                            default_votes: accumulated.default_votes,
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
    let multiplicity_accumulators = multiplicity_accumulators
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let multiplicity = selected
        .multiplicity
        .iter()
        .map(|candidate| {
            let accumulated = &multiplicity_accumulators[candidate.accumulator_index];
            let (outcome, reason) = match accumulated.resolution() {
                MultiplicityResolution::Conclusive(mut edit) => {
                    if let Some(split) = edit.split_values().cloned() {
                        edit = MultiplicityConfigEdit::delimited(SplitValues {
                            field: candidate.column.clone(),
                            ..split
                        });
                    }
                    edits.push(PatchEdit::Multiplicity {
                        owner: candidate.address.clone(),
                        edit,
                    });
                    ("conclusive", None)
                }
                MultiplicityResolution::Unconfirmed(reason) => {
                    resolved = false;
                    ("unconfirmed", Some(reason))
                }
                MultiplicityResolution::ReviewOnly(reason) => {
                    resolved = false;
                    ("review_only", Some(reason))
                }
            };
            MultiplicityReport {
                field: candidate.selector(),
                address: candidate.address.render(),
                format: candidate.format.label(),
                observations: accumulated.observations,
                empty_records: accumulated.empty_records,
                singleton_records: accumulated.singleton_records,
                multi_records: accumulated.multi_records.max(
                    accumulated
                        .csv_candidates()
                        .iter()
                        .filter_map(|interpretation| {
                            accumulated
                                .csv
                                .iter()
                                .find(|state| state.interpretation == *interpretation)
                                .map(|state| state.multi_records)
                        })
                        .max()
                        .unwrap_or(0),
                ),
                outcome,
                reason,
            }
        })
        .collect();
    let patch = render_patch(&edits);
    drop(accumulators);
    drop(multiplicity_accumulators);
    let mut write_succeeded = false;
    let write = if matches!(mode, GuessMode::Write) {
        let Some(initial_config) = config_snapshot.as_ref() else {
            return Err(GuessError::infrastructure(
                "internal: write mode did not capture config bytes".to_owned(),
            ));
        };
        let Some(initial_inputs) = input_snapshot.as_ref() else {
            return Err(GuessError::infrastructure(
                "internal: write mode did not capture input bytes".to_owned(),
            ));
        };
        let report = if !resolved {
            WriteReport::not_written("unresolved_evidence", None)
        } else if effective.selection.kind != "base" {
            WriteReport::not_written("effective_config_has_overlay", None)
        } else if let Some(reason) = initial_config.ineligible_reason {
            WriteReport::not_written(reason, None)
        } else if let Some(reason) = initial_inputs.ineligible_reason {
            WriteReport::not_written(reason, None)
        } else if !config_unchanged_for_analysis {
            WriteReport::not_written("config_changed_during_analysis", None)
        } else if InputSnapshot::capture(&manifest)
            .map(|snapshot| snapshot != *initial_inputs)
            .unwrap_or(true)
        {
            WriteReport::not_written("input_changed_during_analysis", None)
        } else if edits.len() != 1 {
            WriteReport::not_written("write_requires_one_owner", None)
        } else {
            perform_write(
                args,
                initial_config,
                initial_inputs,
                effective_digest,
                &edits[0],
                &config_dir,
                shutdown,
            )?
        };
        write_succeeded = report.status == "written";
        Some(report)
    } else {
        None
    };
    let report = GuessReport {
        schema: "clinker.guess.report",
        version: if matches!(mode, GuessMode::Write) {
            3
        } else {
            2
        },
        mode: mode.label(),
        exhaustive: mode.exhaustive(),
        outcome: if resolved { "resolved" } else { "unresolved" },
        target: effective.config.pipeline.name.clone(),
        selection: effective.selection,
        manifest: manifest_report,
        limits: LimitsReport {
            max_yaml_input_bytes: clinker_plan::yaml::MAX_INPUT_BYTES,
            max_yaml_nodes_per_document: clinker_plan::yaml::MAX_NODES,
            max_schema_leaves: MAX_SCHEMA_LEAVES,
            max_manifest_files: MAX_MANIFEST_FILES,
            preview_max_file_opens_total: MAX_FILE_OPENS_TOTAL,
            preview_max_records_total: MAX_RECORDS_TOTAL,
            preview_max_input_bytes_total: MAX_INPUT_BYTES_TOTAL,
            max_reported_files_per_source: MAX_REPORTED_FILES_PER_SOURCE,
            max_numeric_lexeme_evidence_bytes:
                clinker_format::numeric_observation::MAX_NUMERIC_LEXEME_EVIDENCE_BYTES,
            max_evidence_per_owner: MAX_EVIDENCE_PER_OWNER,
            max_csv_delimiter_candidates: MAX_CSV_DELIMITER_CANDIDATES,
        },
        coverage,
        fields,
        multiplicity,
        patch,
        write,
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
    let exit_code = match mode {
        GuessMode::Preview => 0,
        GuessMode::Check if resolved => 0,
        GuessMode::Check => 3,
        GuessMode::Write if write_succeeded => 0,
        GuessMode::Write => 3,
    };
    Ok(GuessSuccess {
        exit_code,
        outcome: if exit_code == 3 || !resolved {
            GuessTerminalOutcome::Unresolved
        } else {
            GuessTerminalOutcome::Completed
        },
    })
}

struct ConfigFileLock {
    file: File,
}

enum ConfigLockAttempt {
    Acquired(ConfigFileLock),
    Contended,
    Ineligible(&'static str),
}

impl ConfigFileLock {
    fn try_acquire(config_path: &Path) -> Result<ConfigLockAttempt, GuessError> {
        let lock_path = config_lock_path(config_path)?;
        match std::fs::symlink_metadata(&lock_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Ok(ConfigLockAttempt::Ineligible("config_lock_symlink"));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Ok(ConfigLockAttempt::Ineligible(
                    "config_lock_not_regular_file",
                ));
            }
            Ok(metadata) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;

                    if metadata.permissions().mode() & 0o077 != 0 {
                        return Ok(ConfigLockAttempt::Ineligible("config_lock_permissions"));
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(GuessError::infrastructure(format!(
                    "cannot inspect pipeline config lock {}: {error}",
                    lock_path.display()
                )));
            }
        }
        let file = match clinker_channel::staging_copy::open_owner_only_lock_file(&lock_path) {
            Ok(file) => file,
            Err(error) => {
                let reason = std::fs::symlink_metadata(&lock_path)
                    .ok()
                    .and_then(|metadata| {
                        if metadata.file_type().is_symlink() {
                            Some("config_lock_symlink")
                        } else if !metadata.is_file() {
                            Some("config_lock_not_regular_file")
                        } else {
                            None
                        }
                    });
                if let Some(reason) = reason {
                    return Ok(ConfigLockAttempt::Ineligible(reason));
                }
                return Err(GuessError::infrastructure(format!(
                    "cannot safely open pipeline config lock {}: {error}",
                    lock_path.display()
                )));
            }
        };
        match FileExt::try_lock(&file) {
            Ok(()) => Ok(ConfigLockAttempt::Acquired(Self { file })),
            Err(fs4::TryLockError::WouldBlock) => Ok(ConfigLockAttempt::Contended),
            Err(fs4::TryLockError::Error(error)) => Err(GuessError::infrastructure(format!(
                "cannot lock pipeline config lock {}: {error}",
                lock_path.display()
            ))),
        }
    }
}

fn config_lock_path(config_path: &Path) -> Result<PathBuf, GuessError> {
    let Some(file_name) = config_path.file_name() else {
        return Err(GuessError::configuration(format!(
            "pipeline config {} has no filename to lock",
            config_path.display()
        )));
    };
    let mut lock_name = file_name.to_os_string();
    lock_name.push(".clinker-guess.lock");
    Ok(config_path.with_file_name(lock_name))
}

impl Drop for ConfigFileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PlannedEdit {
    Numeric(clinker_plan::config::canonical::NumericTypeEdit),
    Multiplicity(clinker_plan::config::canonical::MultiplicityEdit),
}

fn plan_proposed_edit(
    raw: &str,
    proposed: &PatchEdit,
) -> Result<Result<PlannedEdit, &'static str>, GuessError> {
    use clinker_plan::config::canonical::{
        MultiplicityEditDecision, NumericTypeEditDecision, plan_multiplicity_edit,
        plan_numeric_type_edit,
    };
    let decision = match proposed {
        PatchEdit::Numeric {
            owner, replacement, ..
        } => match plan_numeric_type_edit(raw, owner, *replacement)
            .map_err(|error| GuessError::infrastructure(error.to_string()))?
        {
            NumericTypeEditDecision::Editable(edit) => Ok(PlannedEdit::Numeric(edit)),
            NumericTypeEditDecision::Ineligible(reason) => Err(reason.as_str()),
        },
        PatchEdit::Multiplicity { owner, edit } => match plan_multiplicity_edit(raw, owner, edit)
            .map_err(|error| GuessError::infrastructure(error.to_string()))?
        {
            MultiplicityEditDecision::Editable(edit) => Ok(PlannedEdit::Multiplicity(edit)),
            MultiplicityEditDecision::Ineligible(reason) => Err(reason.as_str()),
        },
    };
    Ok(decision)
}

fn apply_planned_edit(raw: &str, edit: &PlannedEdit) -> Result<String, GuessError> {
    match edit {
        PlannedEdit::Numeric(edit) => edit.apply(raw),
        PlannedEdit::Multiplicity(edit) => edit.apply(raw),
    }
    .map_err(|error| GuessError::infrastructure(error.to_string()))
}

fn prove_proposed_raw(
    original: &str,
    edited: &str,
    proposed: &PatchEdit,
) -> Result<(), GuessError> {
    use clinker_plan::config::canonical::{
        prove_multiplicity_only_change, prove_numeric_type_only_change,
    };
    match proposed {
        PatchEdit::Numeric {
            owner, replacement, ..
        } => prove_numeric_type_only_change(original, edited, owner, *replacement),
        PatchEdit::Multiplicity { owner, edit } => {
            prove_multiplicity_only_change(original, edited, owner, edit)
        }
    }
    .map_err(|error| GuessError::infrastructure(error.to_string()))
}

fn prove_proposed_resolved(
    original: &clinker_plan::config::PipelineConfig,
    edited: &clinker_plan::config::PipelineConfig,
    proposed: &PatchEdit,
) -> Result<(), GuessError> {
    use clinker_plan::config::canonical::{
        prove_resolved_multiplicity_only_change, prove_resolved_numeric_type_only_change,
    };
    match proposed {
        PatchEdit::Numeric {
            owner, replacement, ..
        } => prove_resolved_numeric_type_only_change(original, edited, owner, *replacement),
        PatchEdit::Multiplicity { owner, edit } => {
            prove_resolved_multiplicity_only_change(original, edited, owner, edit)
        }
    }
    .map_err(|error| GuessError::infrastructure(error.to_string()))
}

fn perform_write(
    args: &GuessArgs,
    initial_config: &ConfigSnapshot,
    initial_inputs: &InputSnapshot,
    effective_digest: [u8; 32],
    proposed: &PatchEdit,
    config_dir: &Path,
    shutdown: &clinker_exec::pipeline::shutdown::ShutdownToken,
) -> Result<WriteReport, GuessError> {
    let owner = proposed.address();
    let edit = match plan_proposed_edit(&initial_config.raw, proposed)? {
        Ok(edit) => edit,
        Err(reason) => return Ok(WriteReport::not_written(reason, Some(owner))),
    };
    let edited = apply_planned_edit(&initial_config.raw, &edit)?;
    prove_proposed_raw(&initial_config.raw, &edited, proposed)?;

    let _config_lock = match ConfigFileLock::try_acquire(&args.config)? {
        ConfigLockAttempt::Acquired(lock) => lock,
        ConfigLockAttempt::Contended => {
            return Ok(WriteReport::not_written(
                "config_lock_contended",
                Some(owner),
            ));
        }
        ConfigLockAttempt::Ineligible(reason) => {
            return Ok(WriteReport::not_written(reason, Some(owner)));
        }
    };
    let locked_metadata = std::fs::symlink_metadata(&args.config).map_err(|error| {
        GuessError::infrastructure(format!(
            "cannot inspect pipeline config {} after locking: {error}",
            args.config.display()
        ))
    })?;
    if !locked_metadata.is_file() {
        return Ok(WriteReport::not_written(
            "config_not_regular_file",
            Some(owner),
        ));
    }
    let permissions = locked_metadata.permissions();
    let parent = args
        .config
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut staged = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
        GuessError::infrastructure(format!(
            "cannot create sibling temporary config beside {}: {error}",
            args.config.display()
        ))
    })?;
    staged
        .as_file()
        .set_permissions(permissions)
        .map_err(|error| {
            GuessError::infrastructure(format!(
                "cannot preserve pipeline config permissions: {error}"
            ))
        })?;
    staged.write_all(edited.as_bytes()).map_err(|error| {
        GuessError::infrastructure(format!("cannot stage edited pipeline config: {error}"))
    })?;
    staged.flush().map_err(|error| {
        GuessError::infrastructure(format!("cannot flush staged pipeline config: {error}"))
    })?;
    staged.as_file().sync_all().map_err(|error| {
        GuessError::infrastructure(format!("cannot fsync staged pipeline config: {error}"))
    })?;

    wait_for_test_write_barrier(shutdown)?;
    if shutdown.is_requested() {
        return Err(GuessError::interrupted());
    }

    // This is the final compare immediately before the publication CAS. The
    // lock coordinates cooperating writers; exact path bytes also catch a
    // non-cooperating replace that happened before this check.
    let path_snapshot = match snapshot_config(&args.config) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            return Ok(WriteReport::not_written(
                "config_changed_before_publication",
                Some(owner),
            ));
        }
    };
    if path_snapshot.ineligible_reason.is_some() || path_snapshot.raw != initial_config.raw {
        return Ok(WriteReport::not_written(
            "config_changed_before_publication",
            Some(owner),
        ));
    }
    let fresh_effective = match resolve_effective_config(args) {
        Ok(effective) => effective,
        Err(_) => {
            return Ok(WriteReport::not_written(
                "effective_config_changed_before_publication",
                Some(owner),
            ));
        }
    };
    let fresh_digest =
        clinker_plan::config::canonical::semantic_config_digest(&fresh_effective.config)
            .map_err(|error| GuessError::infrastructure(error.to_string()))?;
    if fresh_digest != effective_digest {
        return Ok(WriteReport::not_written(
            "effective_config_changed_before_publication",
            Some(owner),
        ));
    }
    let fresh_candidates = match select_candidates(&fresh_effective.config, &args.fields) {
        Ok(candidates) => candidates,
        Err(_) => {
            return Ok(WriteReport::not_written(
                "effective_config_changed_before_publication",
                Some(owner),
            ));
        }
    };
    let fresh_manifest =
        match freeze_manifest(&fresh_effective.config, &fresh_candidates, config_dir) {
            Ok(manifest) => manifest,
            Err(_) => {
                return Ok(WriteReport::not_written(
                    "input_changed_before_publication",
                    Some(owner),
                ));
            }
        };
    let fresh_inputs = match InputSnapshot::capture(&fresh_manifest) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            return Ok(WriteReport::not_written(
                "input_changed_before_publication",
                Some(owner),
            ));
        }
    };
    if fresh_inputs != *initial_inputs {
        return Ok(WriteReport::not_written(
            "input_changed_before_publication",
            Some(owner),
        ));
    }
    // Input hashing can take longer than the config checks above. Repeat the
    // exact config and typed-effective checks after it so the final config
    // comparison, owner proof, and rename are adjacent.
    let final_path_snapshot = match snapshot_config(&args.config) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            return Ok(WriteReport::not_written(
                "config_changed_before_publication",
                Some(owner),
            ));
        }
    };
    if final_path_snapshot.ineligible_reason.is_some()
        || final_path_snapshot.raw != initial_config.raw
    {
        return Ok(WriteReport::not_written(
            "config_changed_before_publication",
            Some(owner),
        ));
    }
    let final_effective = match resolve_effective_config(args) {
        Ok(effective) => effective,
        Err(_) => {
            return Ok(WriteReport::not_written(
                "effective_config_changed_before_publication",
                Some(owner),
            ));
        }
    };
    let final_digest =
        clinker_plan::config::canonical::semantic_config_digest(&final_effective.config)
            .map_err(|error| GuessError::infrastructure(error.to_string()))?;
    if final_digest != effective_digest {
        return Ok(WriteReport::not_written(
            "effective_config_changed_before_publication",
            Some(owner),
        ));
    }
    let empty_patches = IndexMap::new();
    let staged_effective =
        clinker_plan::config::load_config_with_vars_and_patches(staged.path(), &[], &empty_patches)
            .map_err(|error| {
                GuessError::infrastructure(format!(
                    "cannot resolve staged pipeline config before publication: {error}"
                ))
            })?;
    prove_proposed_resolved(&final_effective.config, &staged_effective, proposed)?;
    match plan_proposed_edit(&final_path_snapshot.raw, proposed)? {
        Ok(current) if current == edit => {}
        Ok(_) | Err(_) => {
            return Ok(WriteReport::not_written(
                "owner_changed_before_publication",
                Some(owner),
            ));
        }
    }
    prove_proposed_raw(&final_path_snapshot.raw, &edited, proposed)?;

    // Re-hash the complete bounded input snapshot after staged semantic
    // resolution so input equality is the last potentially long operation
    // before publication. The stable config lock protects cooperating config
    // writers while this pass runs; a final exact-byte check follows it.
    let publication_candidates = match select_candidates(&final_effective.config, &args.fields) {
        Ok(candidates) => candidates,
        Err(_) => {
            return Ok(WriteReport::not_written(
                "effective_config_changed_before_publication",
                Some(owner),
            ));
        }
    };
    let publication_manifest =
        match freeze_manifest(&final_effective.config, &publication_candidates, config_dir) {
            Ok(manifest) => manifest,
            Err(_) => {
                return Ok(WriteReport::not_written(
                    "input_changed_before_publication",
                    Some(owner),
                ));
            }
        };
    let publication_inputs = match InputSnapshot::capture(&publication_manifest) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            return Ok(WriteReport::not_written(
                "input_changed_before_publication",
                Some(owner),
            ));
        }
    };
    if publication_inputs != *initial_inputs {
        return Ok(WriteReport::not_written(
            "input_changed_before_publication",
            Some(owner),
        ));
    }
    let publication_config = match snapshot_config(&args.config) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            return Ok(WriteReport::not_written(
                "config_changed_before_publication",
                Some(owner),
            ));
        }
    };
    if publication_config.ineligible_reason.is_some()
        || publication_config.raw != initial_config.raw
    {
        return Ok(WriteReport::not_written(
            "config_changed_before_publication",
            Some(owner),
        ));
    }

    maybe_inject_write_failure()?;
    maybe_inject_write_interruption(shutdown);
    if !shutdown.try_begin_publication() {
        return Err(GuessError::interrupted());
    }
    staged.persist(&args.config).map_err(|error| {
        GuessError::infrastructure(format!(
            "cannot atomically replace pipeline config {}: {}",
            args.config.display(),
            error.error
        ))
    })?;
    clinker_channel::staging_copy::sync_parent_directory(&args.config)
        .map_err(|error| GuessError::infrastructure(error.to_string()))?;
    Ok(WriteReport::written(owner))
}

#[cfg(debug_assertions)]
fn wait_for_test_write_barrier(
    shutdown: &clinker_exec::pipeline::shutdown::ShutdownToken,
) -> Result<(), GuessError> {
    let Some(directory) = std::env::var_os("CLINKER_TEST_GUESS_WRITE_BARRIER") else {
        return Ok(());
    };
    let directory = PathBuf::from(directory);
    let ready = directory.join("ready");
    let proceed = directory.join("continue");
    std::fs::write(&ready, b"ready").map_err(|error| {
        GuessError::infrastructure(format!("cannot announce test write barrier: {error}"))
    })?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !proceed.exists() {
        if shutdown.is_requested() {
            return Err(GuessError::interrupted());
        }
        if Instant::now() >= deadline {
            return Err(GuessError::infrastructure(
                "test write barrier timed out".to_owned(),
            ));
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn wait_for_test_write_barrier(
    _shutdown: &clinker_exec::pipeline::shutdown::ShutdownToken,
) -> Result<(), GuessError> {
    Ok(())
}

#[cfg(debug_assertions)]
fn maybe_inject_write_failure() -> Result<(), GuessError> {
    if std::env::var_os("CLINKER_TEST_GUESS_WRITE_FAIL_BEFORE_RENAME").is_some() {
        return Err(GuessError::infrastructure(
            "injected failure before config rename".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn maybe_inject_write_failure() -> Result<(), GuessError> {
    Ok(())
}

fn maybe_inject_write_interruption(shutdown: &clinker_exec::pipeline::shutdown::ShutdownToken) {
    #[cfg(debug_assertions)]
    if std::env::var_os("CLINKER_TEST_GUESS_WRITE_INTERRUPT_BEFORE_RENAME").is_some() {
        shutdown.request();
    }
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

struct SelectedCandidates {
    numeric: Vec<Candidate>,
    multiplicity: Vec<MultiplicityCandidate>,
}

fn select_candidates(
    config: &clinker_plan::config::PipelineConfig,
    requested: &[String],
) -> Result<SelectedCandidates, GuessError> {
    let mut candidates = IndexMap::new();
    let mut multiplicity = IndexMap::new();
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
                    if !column.is_multiple()
                        && !header.name.starts_with('$')
                        && !column.name.starts_with('$')
                    {
                        let format = match &body.source.format {
                            InputFormat::Xml(_) => Some(MultiplicityFormat::Xml),
                            InputFormat::Json(_) => Some(MultiplicityFormat::Json),
                            InputFormat::Csv(options) => {
                                let charset = options
                                    .as_ref()
                                    .and_then(|options| options.encoding.as_deref())
                                    .map(Charset::from_name)
                                    .transpose()
                                    .map_err(|error| GuessError::configuration(error.to_string()))?
                                    .unwrap_or_default();
                                Some(MultiplicityFormat::Csv(charset))
                            }
                            _ => None,
                        };
                        if let Some(format) = format {
                            let selector = format!("{}.{}", header.name, column.name);
                            multiplicity.insert(
                                selector,
                                MultiplicityCandidate {
                                    source: header.name.clone(),
                                    column: column.name.clone(),
                                    physical_field: column.physical_name().to_owned(),
                                    address: ScopedColumnAddress::new(&header.name, &column.name),
                                    format,
                                    accumulator_index: usize::MAX,
                                },
                            );
                        }
                    }
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
    if requested.is_empty() {
        if candidates.is_empty() {
            return Err(GuessError::configuration(
                "the selected effective configuration has no literal `numeric` source-schema leaves; correction: declare `type: numeric` on an inference-only source column or select one concrete field for multiplicity review",
            ));
        }
        return Ok(SelectedCandidates {
            numeric: candidates.into_values().collect(),
            multiplicity: Vec::new(),
        });
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
    let mut selected_numeric = Vec::new();
    let mut selected_multiplicity = Vec::new();
    for field in requested_once {
        if let Some(candidate) = candidates.get(&field) {
            selected_numeric.push(candidate.clone());
            continue;
        }
        if let Some(candidate) = multiplicity.get(&field) {
            let mut candidate = candidate.clone();
            candidate.accumulator_index = selected_multiplicity.len();
            selected_multiplicity.push(candidate);
            continue;
        }
        if let Some(concrete) = all_fields.get(&field) {
            let concrete = concrete
                .iter()
                .map(ToString::to_string)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
                .join(", ");
            return Err(GuessError::configuration(format!(
                "--field {field:?} has concrete declaration(s) ({concrete}) but its source format cannot supply multiplicity evidence; correction: select a single-record CSV, JSON, or XML source column"
            )));
        }
        let mut selectors = candidates
            .keys()
            .chain(multiplicity.keys())
            .cloned()
            .collect::<Vec<_>>();
        selectors.sort();
        selectors.dedup();
        return Err(GuessError::configuration(format!(
            "unknown --field {field:?}; correction: use one of {}",
            selectors.join(", ")
        )));
    }
    Ok(SelectedCandidates {
        numeric: selected_numeric,
        multiplicity: selected_multiplicity,
    })
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
        absence_policy: AbsencePolicy {
            nullable: column.ty.is_nullable(),
            required: column.required.unwrap_or(false),
            has_default: column.default.is_some(),
        },
        default_observation: column
            .default
            .as_ref()
            .map(|value| numeric_default_observation(source, &column.name, value))
            .transpose()?,
        accumulator_index: usize::MAX,
    });
    Ok(())
}

fn numeric_default_observation(
    source: &str,
    field: &str,
    value: &serde_json::Value,
) -> Result<NumericObservation, GuessError> {
    match value {
        serde_json::Value::Null => Ok(observe_schema_numeric(&Value::Null)),
        serde_json::Value::Number(number) => Ok(observe_schema_numeric(&Value::String(
            number.to_string().into(),
        ))),
        serde_json::Value::String(text) => {
            Ok(observe_schema_numeric(&Value::String(text.as_str().into())))
        }
        serde_json::Value::Bool(value) => Ok(observe_schema_numeric(&Value::Bool(*value))),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(GuessError::configuration(format!(
                "numeric source field {source}.{field} has a non-scalar default; correction: use an exact numeric scalar default or remove `default`"
            )))
        }
    }
}

fn index_numeric_owners(candidates: &mut [Candidate]) {
    let mut next = 0;
    for candidate in candidates {
        for owner in &mut candidate.owners {
            owner.accumulator_index = next;
            next += 1;
        }
    }
}

fn build_accumulators(candidates: &[Candidate]) -> Vec<FieldAccumulator> {
    let owner_count = candidates.iter().fold(0usize, |count, candidate| {
        count.saturating_add(candidate.owners.len())
    });
    let mut accumulators = Vec::with_capacity(owner_count);
    for candidate in candidates {
        for owner in &candidate.owners {
            debug_assert_eq!(owner.accumulator_index, accumulators.len());
            accumulators.push(FieldAccumulator::new(owner));
        }
    }
    accumulators
}

struct FrozenSource<'a> {
    source: String,
    format: &'static str,
    body: &'a SourceBody,
    files: Vec<clinker_plan::config::discovery::DiscoveredFile>,
    identity: String,
    local_file: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InputSnapshot {
    sources: Vec<InputSourceSnapshot>,
    ineligible_reason: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InputSourceSnapshot {
    source: String,
    manifest_identity: String,
    files: Vec<InputFileSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InputFileSnapshot {
    path: PathBuf,
    size: u64,
    digest: String,
}

impl InputSnapshot {
    fn capture(manifest: &[FrozenSource<'_>]) -> Result<Self, GuessError> {
        let mut ineligible_reason = None;
        let mut sources = Vec::with_capacity(manifest.len());
        for source in manifest {
            if !source.local_file {
                ineligible_reason.get_or_insert("input_not_local_file");
            }
            let mut files = Vec::with_capacity(source.files.len());
            for file in &source.files {
                let metadata = std::fs::symlink_metadata(&file.path).map_err(|error| {
                    GuessError::infrastructure(format!(
                        "cannot snapshot source file {}: {error}",
                        file.path.display()
                    ))
                })?;
                if metadata.file_type().is_symlink() {
                    ineligible_reason.get_or_insert("input_symlink");
                } else if !metadata.file_type().is_file() {
                    ineligible_reason.get_or_insert("input_not_regular_file");
                }
                let digest = clinker_channel::staging_copy::content_digest(&file.path)
                    .map_err(|error| GuessError::infrastructure(error.to_string()))?;
                files.push(InputFileSnapshot {
                    path: file.path.clone(),
                    size: file.size,
                    digest,
                });
            }
            sources.push(InputSourceSnapshot {
                source: source.source.clone(),
                manifest_identity: source.identity.clone(),
                files,
            });
        }
        Ok(Self {
            sources,
            ineligible_reason,
        })
    }
}

#[derive(Debug)]
struct ConfigSnapshot {
    raw: String,
    ineligible_reason: Option<&'static str>,
}

fn snapshot_config(path: &Path) -> Result<ConfigSnapshot, GuessError> {
    let contains_symlink = path_contains_symlink(path)?;
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        GuessError::infrastructure(format!(
            "cannot inspect pipeline config {}: {error}",
            path.display()
        ))
    })?;
    let ineligible_reason = if contains_symlink || metadata.file_type().is_symlink() {
        Some("config_symlink")
    } else if !metadata.file_type().is_file() {
        Some("config_not_regular_file")
    } else {
        None
    };
    let file = File::open(path).map_err(|error| {
        GuessError::infrastructure(format!(
            "cannot read pipeline config {}: {error}",
            path.display()
        ))
    })?;
    let raw = read_capped_config(file).map_err(|error| {
        GuessError::infrastructure(format!(
            "cannot read pipeline config {}: {error}",
            path.display()
        ))
    })?;
    if raw.len() > clinker_plan::yaml::MAX_INPUT_BYTES {
        return Err(GuessError::configuration(format!(
            "pipeline config {} exceeds the {}-byte canonical YAML limit",
            path.display(),
            clinker_plan::yaml::MAX_INPUT_BYTES
        )));
    }
    Ok(ConfigSnapshot {
        raw,
        ineligible_reason,
    })
}

fn path_contains_symlink(path: &Path) -> Result<bool, GuessError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| {
                GuessError::infrastructure(format!(
                    "cannot resolve the current directory for {}: {error}",
                    path.display()
                ))
            })?
            .join(path)
    };
    let mut ancestors = absolute.ancestors().collect::<Vec<_>>();
    while let Some(current) = ancestors.pop() {
        let metadata = std::fs::symlink_metadata(current).map_err(|error| {
            GuessError::infrastructure(format!(
                "cannot inspect pipeline config path component {}: {error}",
                current.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn read_capped_config(reader: impl Read) -> std::io::Result<String> {
    let mut raw = String::new();
    reader
        .take((clinker_plan::yaml::MAX_INPUT_BYTES as u64).saturating_add(1))
        .read_to_string(&mut raw)?;
    Ok(raw)
}

fn freeze_manifest<'a>(
    config: &'a clinker_plan::config::PipelineConfig,
    candidates: &SelectedCandidates,
    config_dir: &Path,
) -> Result<Vec<FrozenSource<'a>>, GuessError> {
    let selected_sources = candidates
        .numeric
        .iter()
        .map(|candidate| candidate.source.as_str())
        .chain(
            candidates
                .multiplicity
                .iter()
                .map(|candidate| candidate.source.as_str()),
        )
        .collect::<IndexSet<_>>();
    let mut manifest = Vec::with_capacity(selected_sources.len());
    let mut retained_files = 0usize;
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
            manifest.push(FrozenSource {
                source: source_name.to_owned(),
                format: body.source.format.format_name(),
                body,
                files: Vec::new(),
                identity: "non-file-source".to_owned(),
                local_file: false,
            });
            continue;
        }
        let remaining = MAX_MANIFEST_FILES.saturating_sub(retained_files);
        let discovered = clinker_plan::config::discovery::discover_bounded(
            &body.source,
            config_dir,
            remaining.saturating_add(1),
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
        if discovered_files > remaining {
            return Err(GuessError::configuration(format!(
                "the selected input manifest contains more than {MAX_MANIFEST_FILES} files; correction: narrow the source matcher or use `files.take_first` / `files.take_last` to admit a finite exhaustive manifest"
            )));
        }
        let identity = discovered.complete_manifest_id(config_dir).ok_or_else(|| {
            GuessError::configuration(
                "the selected input manifest could not be retained completely within its fixed bound; correction: narrow the source matcher",
            )
        })?;
        retained_files = retained_files.saturating_add(discovered_files);
        manifest.push(FrozenSource {
            source: source_name.to_owned(),
            format: body.source.format.format_name(),
            body,
            files: discovered.files().to_vec(),
            identity,
            local_file: true,
        });
    }
    Ok(manifest)
}

fn empty_coverage(manifest: &[FrozenSource<'_>]) -> Vec<SourceCoverage> {
    manifest
        .iter()
        .map(|source| SourceCoverage {
            source: source.source.clone(),
            format: source.format,
            discovered_files: source.files.len(),
            sampled_files: 0,
            truncated_files: 0,
            uncovered_files: source.files.len(),
            unreported_file_count: source.files.len(),
            sampled_input_bytes: 0,
            bytes_read: 0,
            records_sampled: 0,
            files: Vec::new(),
        })
        .collect()
}

struct ActiveReader {
    source_index: usize,
    path: String,
    input_bytes: u64,
    reader: Box<dyn FormatReader>,
    tally: ByteTally,
    observation_state: Arc<Mutex<ReaderObservationState>>,
    field_map: Arc<ObserverFieldMap>,
    multiplicity_fields: Vec<(String, usize)>,
    records: u64,
    numeric_errors: u64,
}

#[derive(Default)]
struct ReaderObservationState {
    seen: HashSet<usize>,
    explainable: HashSet<usize>,
    record: Option<String>,
}

impl ReaderObservationState {
    fn begin_record(&mut self) {
        self.seen.clear();
        self.explainable.clear();
        self.record = None;
    }
}

enum ReaderStep {
    Record,
    NumericError,
    End,
}

fn open_reader(
    source_index: usize,
    source: &FrozenSource<'_>,
    file: &clinker_plan::config::discovery::DiscoveredFile,
    candidates: &SelectedCandidates,
    accumulators: Arc<Mutex<Vec<FieldAccumulator>>>,
    config_dir: &Path,
) -> Result<ActiveReader, GuessError> {
    let field_map = Arc::new(observer_field_map(&candidates.numeric, &source.source));
    let multiplicity_fields = candidates
        .multiplicity
        .iter()
        .filter(|candidate| candidate.source == source.source)
        .map(|candidate| {
            (
                candidate.physical_field.clone(),
                candidate.accumulator_index,
            )
        })
        .collect::<Vec<_>>();
    let observation_state = Arc::new(Mutex::new(ReaderObservationState::default()));
    let observer_map = Arc::clone(&field_map);
    let observer_state = Arc::clone(&observation_state);
    let target = accumulators;
    let observer = NumericObserver::new_scoped(move |scope, observation| {
        let Some(indices) = observer_map.indices(scope.record(), scope.field()) else {
            return;
        };
        {
            let mut accumulated = target
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for index in indices {
                accumulated[*index].observe(&observation);
            }
        }
        let explains_reader_error = matches!(
            observation.vote(),
            NumericVote::NoValue | NumericVote::Unresolved(_)
        );
        let mut state = observer_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(record) = scope.record() {
            state.record = Some(record.to_owned());
        }
        state.seen.extend(indices.iter().copied());
        if explains_reader_error {
            state.explainable.extend(indices.iter().copied());
        }
    });
    let tally = ByteTally::new();
    #[cfg(debug_assertions)]
    let open_path = if std::env::var_os("CLINKER_TEST_GUESS_FAIL_OPEN_AFTER_DISCOVERY").is_some() {
        file.path
            .with_file_name(".__clinker_guess_missing_after_discovery__")
    } else {
        file.path.clone()
    };
    #[cfg(not(debug_assertions))]
    let open_path = file.path.clone();
    #[cfg(debug_assertions)]
    if std::env::var_os("CLINKER_TEST_GUESS_GROW_AFTER_DISCOVERY").is_some() {
        std::fs::OpenOptions::new()
            .append(true)
            .open(&file.path)
            .and_then(|mut input| input.write_all(b"\n"))
            .map_err(|error| {
                GuessError::infrastructure(format!(
                    "cannot inject post-discovery input growth: {error}"
                ))
            })?;
    }
    let byte_source = ReopenableSource::path(open_path)
        .with_exact_len(file.size)
        .with_tally(tally.clone());
    let mut probe_schema = source.body.schema.clone();
    if let SourceSchema::Columns(columns) = &mut probe_schema {
        for candidate in candidates.multiplicity.iter().filter(|candidate| {
            candidate.source == source.source
                && !matches!(candidate.format, MultiplicityFormat::Csv(_))
        }) {
            if let Some(column) = columns
                .iter_mut()
                .find(|column| column.name == candidate.column)
            {
                column.multiple = Some(true);
            }
        }
    }
    let reader = clinker_exec::executor::build_source_format_reader(
        &source.body.source,
        &probe_schema,
        source.body.on_unmapped.clone(),
        byte_source,
        Some(observer),
    )
    .map_err(reader_error)?;
    Ok(ActiveReader {
        source_index,
        path: stable_input_path(&file.path, config_dir),
        input_bytes: file.size,
        reader,
        tally,
        observation_state,
        field_map,
        multiplicity_fields,
        records: 0,
        numeric_errors: 0,
    })
}

fn next_observed_record(
    active: &mut ActiveReader,
    accumulators: &Arc<Mutex<Vec<FieldAccumulator>>>,
    multiplicity_accumulators: &Arc<Mutex<Vec<MultiplicityAccumulator>>>,
) -> Result<ReaderStep, GuessError> {
    active
        .observation_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .begin_record();
    match active.reader.next_record() {
        Ok(Some(record)) => {
            let seen = {
                let state = active
                    .observation_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.seen.clone()
            };
            complete_absence_observations(&active.field_map, &record, &seen, accumulators);
            let mut accumulated = multiplicity_accumulators
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (field, index) in &active.multiplicity_fields {
                accumulated[*index].observe(record.get(field))?;
            }
            active.records = active.records.saturating_add(1);
            Ok(ReaderStep::Record)
        }
        Ok(None) => Ok(ReaderStep::End),
        Err(error) => {
            if matches!(&error, clinker_format::FormatError::Interrupted) {
                return Err(GuessError::interrupted());
            }
            let state = active
                .observation_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut missing = Vec::new();
            let mut unobserved = Vec::new();
            let explains_reader_error = match &error {
                clinker_format::FormatError::DeclaredType(failure) => {
                    for owner in active.field_map.expected(&failure.original_record) {
                        if state.seen.contains(&owner.index) {
                            continue;
                        }
                        if owner
                            .observed_fields
                            .iter()
                            .all(|field| failure.original_record.get(field).is_none())
                        {
                            missing.push(owner.index);
                        } else {
                            unobserved.push(owner.index);
                        }
                    }
                    missing
                        .iter()
                        .any(|index| active.field_map.exposed_field(*index) == failure.field)
                        || state
                            .explainable
                            .iter()
                            .any(|index| active.field_map.exposed_field(*index) == failure.field)
                }
                // Positional readers report field coercion through their
                // format-specific error after emitting the scoped parser
                // observation. Accept that evidence only when exactly one
                // authored numeric owner explains the failed row.
                clinker_format::FormatError::FixedWidth(_) if state.explainable.len() == 1 => {
                    unobserved.extend(
                        active
                            .field_map
                            .expected_for_scope(state.record.as_deref())
                            .iter()
                            .filter(|owner| !state.seen.contains(&owner.index))
                            .map(|owner| owner.index),
                    );
                    true
                }
                _ => false,
            };
            drop(state);
            if explains_reader_error {
                let observation = observe_schema_numeric(&Value::Null);
                let mut accumulated = accumulators
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for index in missing {
                    accumulated[index].observe_missing(&observation);
                }
                for index in unobserved {
                    accumulated[index].missing_parser_observation();
                }
                active.records = active.records.saturating_add(1);
                active.numeric_errors = active.numeric_errors.saturating_add(1);
                Ok(ReaderStep::NumericError)
            } else {
                Err(GuessError::infrastructure(format!(
                    "cannot read source file {}: {error}",
                    active.path
                )))
            }
        }
    }
}

fn complete_absence_observations(
    field_map: &ObserverFieldMap,
    record: &Record,
    seen: &HashSet<usize>,
    accumulators: &Arc<Mutex<Vec<FieldAccumulator>>>,
) {
    let expected = field_map.expected(record);
    if expected.is_empty() {
        return;
    }
    let mut accumulated = accumulators
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for owner in expected {
        if seen.contains(&owner.index) {
            continue;
        }
        match record.get(&owner.field) {
            None | Some(Value::Null) => {
                accumulated[owner.index].observe_missing(&observe_schema_numeric(&Value::Null));
            }
            Some(_) => accumulated[owner.index].missing_parser_observation(),
        }
    }
}

fn finish_reader(active: ActiveReader, coverage: &mut [SourceCoverage], truncated: bool) {
    let source = &mut coverage[active.source_index];
    source.sampled_files = source.sampled_files.saturating_add(1);
    source.sampled_input_bytes = source
        .sampled_input_bytes
        .saturating_add(active.input_bytes);
    source.bytes_read = source.bytes_read.saturating_add(active.tally.read());
    source.records_sampled = source.records_sampled.saturating_add(active.records);
    if truncated {
        source.truncated_files = source.truncated_files.saturating_add(1);
    }
    if source.files.len() < MAX_REPORTED_FILES_PER_SOURCE {
        source.files.push(FileCoverage {
            path: active.path,
            status: if truncated {
                "truncated_global_record_budget"
            } else if active.numeric_errors > 0 {
                "sampled_with_numeric_conflicts"
            } else {
                "sampled"
            },
            input_bytes: Some(active.input_bytes),
            bytes_read: active.tally.read(),
            records_sampled: active.records,
            truncated,
        });
    }
    source.uncovered_files = source.discovered_files.saturating_sub(source.sampled_files);
    source.unreported_file_count = source.discovered_files.saturating_sub(source.files.len());
}

fn sample_sources_fairly(
    manifest: &[FrozenSource<'_>],
    candidates: &SelectedCandidates,
    accumulators: Arc<Mutex<Vec<FieldAccumulator>>>,
    multiplicity_accumulators: Arc<Mutex<Vec<MultiplicityAccumulator>>>,
    config_dir: &Path,
    shutdown: &clinker_exec::pipeline::shutdown::ShutdownToken,
) -> Result<Vec<SourceCoverage>, GuessError> {
    let mut coverage = empty_coverage(manifest);
    let mut pending = manifest
        .iter()
        .enumerate()
        .filter(|(_, source)| !source.files.is_empty())
        .map(|(source, _)| (source, 0usize))
        .collect::<VecDeque<_>>();
    let mut active = VecDeque::new();
    let mut opened_input_bytes = 0u64;
    while active.len() < MAX_FILE_OPENS_TOTAL {
        let Some((source_index, file_index)) = pending.pop_front() else {
            break;
        };
        let source = &manifest[source_index];
        let file = &source.files[file_index];
        if file.size > MAX_INPUT_BYTES_TOTAL.saturating_sub(opened_input_bytes) {
            continue;
        }
        opened_input_bytes = opened_input_bytes.saturating_add(file.size);
        if file_index + 1 < source.files.len() {
            pending.push_back((source_index, file_index + 1));
        }
        active.push_back(open_reader(
            source_index,
            source,
            file,
            candidates,
            Arc::clone(&accumulators),
            config_dir,
        )?);
    }

    let interrupt_after = test_interrupt_after_records();
    let mut records = 0u64;
    while records < MAX_RECORDS_TOTAL {
        if shutdown.is_requested() {
            return Err(GuessError::interrupted());
        }
        let Some(mut reader) = active.pop_front() else {
            break;
        };
        match next_observed_record(&mut reader, &accumulators, &multiplicity_accumulators)? {
            ReaderStep::Record | ReaderStep::NumericError => {
                records = records.saturating_add(1);
                maybe_inject_interruption(shutdown, interrupt_after, records);
                active.push_back(reader);
            }
            ReaderStep::End => finish_reader(reader, &mut coverage, false),
        }
    }
    for reader in active {
        finish_reader(reader, &mut coverage, true);
    }
    if shutdown.is_requested() {
        return Err(GuessError::interrupted());
    }
    Ok(coverage)
}

fn check_sources_exhaustively(
    manifest: &[FrozenSource<'_>],
    candidates: &SelectedCandidates,
    accumulators: Arc<Mutex<Vec<FieldAccumulator>>>,
    multiplicity_accumulators: Arc<Mutex<Vec<MultiplicityAccumulator>>>,
    config_dir: &Path,
    shutdown: &clinker_exec::pipeline::shutdown::ShutdownToken,
) -> Result<Vec<SourceCoverage>, GuessError> {
    let mut coverage = empty_coverage(manifest);
    let interrupt_after = test_interrupt_after_records();
    let mut total_records = 0u64;
    for (source_index, source) in manifest.iter().enumerate() {
        for file in &source.files {
            if shutdown.is_requested() {
                return Err(GuessError::interrupted());
            }
            let mut reader = open_reader(
                source_index,
                source,
                file,
                candidates,
                Arc::clone(&accumulators),
                config_dir,
            )?;
            loop {
                if shutdown.is_requested() {
                    return Err(GuessError::interrupted());
                }
                match next_observed_record(&mut reader, &accumulators, &multiplicity_accumulators)?
                {
                    ReaderStep::Record | ReaderStep::NumericError => {
                        total_records = total_records.saturating_add(1);
                        maybe_inject_interruption(shutdown, interrupt_after, total_records);
                    }
                    ReaderStep::End => break,
                }
            }
            finish_reader(reader, &mut coverage, false);
        }
    }
    if shutdown.is_requested() {
        return Err(GuessError::interrupted());
    }
    Ok(coverage)
}

#[cfg(debug_assertions)]
fn test_interrupt_after_records() -> Option<u64> {
    std::env::var("CLINKER_TEST_GUESS_INTERRUPT_AFTER_RECORDS")
        .ok()
        .and_then(|value| value.parse().ok())
}

#[cfg(not(debug_assertions))]
fn test_interrupt_after_records() -> Option<u64> {
    None
}

fn maybe_inject_interruption(
    shutdown: &clinker_exec::pipeline::shutdown::ShutdownToken,
    interrupt_after: Option<u64>,
    records: u64,
) {
    if interrupt_after.is_some_and(|limit| records >= limit) {
        shutdown.request();
    }
}

#[derive(Debug)]
struct ExpectedOwner {
    index: usize,
    field: String,
    observed_fields: Vec<String>,
}

#[derive(Debug, Default)]
struct ObserverFieldMap {
    columns: HashMap<String, Vec<usize>>,
    records: HashMap<String, HashMap<String, Vec<usize>>>,
    column_owners: Vec<ExpectedOwner>,
    record_owners: HashMap<String, Vec<ExpectedOwner>>,
    exposed_fields: HashMap<usize, String>,
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

    fn expected(&self, record: &Record) -> &[ExpectedOwner] {
        match record.get(RECORD_TYPE_COLUMN) {
            Some(Value::String(record_type)) => self.expected_for_scope(Some(record_type.as_str())),
            _ => self.expected_for_scope(None),
        }
    }

    fn expected_for_scope(&self, record: Option<&str>) -> &[ExpectedOwner] {
        match record {
            Some(record_type) => self
                .record_owners
                .get(record_type)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            None => &self.column_owners,
        }
    }

    fn exposed_field(&self, index: usize) -> &str {
        self.exposed_fields
            .get(&index)
            .map(String::as_str)
            .unwrap_or("")
    }
}

fn observer_field_map(candidates: &[Candidate], source: &str) -> ObserverFieldMap {
    let mut fields = ObserverFieldMap::default();
    for candidate in candidates {
        if candidate.source != source {
            continue;
        }
        for owner in &candidate.owners {
            fields
                .exposed_fields
                .insert(owner.accumulator_index, candidate.column.clone());
            let target = match owner.record.as_deref() {
                Some(record) => {
                    fields
                        .record_owners
                        .entry(record.to_owned())
                        .or_default()
                        .push(ExpectedOwner {
                            index: owner.accumulator_index,
                            field: candidate.column.clone(),
                            observed_fields: owner.observed_fields.clone(),
                        });
                    fields.records.entry(record.to_owned()).or_default()
                }
                None => {
                    fields.column_owners.push(ExpectedOwner {
                        index: owner.accumulator_index,
                        field: candidate.column.clone(),
                        observed_fields: owner.observed_fields.clone(),
                    });
                    &mut fields.columns
                }
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

enum PatchEdit {
    Numeric {
        owner: ScopedSchemaLeafAddress,
        replacement: clinker_plan::config::canonical::ConcreteNumericType,
        address: String,
        from_type: String,
        to_type: String,
    },
    Multiplicity {
        owner: ScopedColumnAddress,
        edit: MultiplicityConfigEdit,
    },
}

impl PatchEdit {
    fn address(&self) -> String {
        match self {
            Self::Numeric { address, .. } => address.clone(),
            Self::Multiplicity { owner, .. } => owner.render(),
        }
    }
}

fn render_patch(edits: &[PatchEdit]) -> String {
    let mut patch = String::from("edits:\n");
    for edit in edits {
        match edit {
            PatchEdit::Numeric {
                address,
                from_type,
                to_type,
                ..
            } => patch.push_str(&format!(
                "  - address: {address}\n    from: {from_type}\n    to: {to_type}\n"
            )),
            PatchEdit::Multiplicity { owner, edit } => {
                patch.push_str(&format!(
                    "  - address: {}\n    from: false\n    to: true\n",
                    owner.render()
                ));
                if let Some(split) = edit.split_values() {
                    patch.push_str(&format!(
                        "    split_values:\n      delimiter: {:?}\n      escape: {:?}\n",
                        split.delimiter, split.escape
                    ));
                }
            }
        }
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

fn is_csv_candidate(character: char) -> bool {
    character.is_ascii_graphic()
        && !character.is_ascii_alphanumeric()
        && !matches!(character, '\'' | '"')
}

fn csv_interpretations() -> Vec<CsvInterpretationState> {
    let punctuation = ('!'..='~')
        .filter(|character| is_csv_candidate(*character))
        .collect::<Vec<_>>();
    let mut interpretations =
        Vec::with_capacity(punctuation.len().saturating_mul(punctuation.len()));
    for delimiter in &punctuation {
        interpretations.push(CsvInterpretationState {
            interpretation: CsvInterpretation {
                delimiter: *delimiter,
                escape: None,
            },
            viable: true,
            delimiter_seen: false,
            escape_activated: false,
            multi_records: 0,
        });
        for escape in punctuation.iter().filter(|escape| *escape != delimiter) {
            interpretations.push(CsvInterpretationState {
                interpretation: CsvInterpretation {
                    delimiter: *delimiter,
                    escape: Some(*escape),
                },
                viable: true,
                delimiter_seen: false,
                escape_activated: false,
                multi_records: 0,
            });
        }
    }
    interpretations
}

fn split_csv_cell(text: &str, interpretation: CsvInterpretation) -> Vec<String> {
    let mut fields = vec![String::new()];
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        if interpretation.escape == Some(character)
            && characters.peek().is_some_and(|next| {
                *next == interpretation.delimiter || Some(*next) == interpretation.escape
            })
        {
            if let Some(escaped) = characters.next() {
                fields
                    .last_mut()
                    .expect("one CSV field exists")
                    .push(escaped);
            }
        } else if character == interpretation.delimiter {
            fields.push(String::new());
        } else {
            fields
                .last_mut()
                .expect("one CSV field exists")
                .push(character);
        }
    }
    fields
}

fn join_csv_cell(fields: &[String], interpretation: CsvInterpretation) -> String {
    let mut rendered = String::new();
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            rendered.push(interpretation.delimiter);
        }
        for character in field.chars() {
            if interpretation.escape.is_some()
                && (character == interpretation.delimiter
                    || Some(character) == interpretation.escape)
            {
                rendered.push(interpretation.escape.expect("escape is present"));
            }
            rendered.push(character);
        }
    }
    rendered
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
