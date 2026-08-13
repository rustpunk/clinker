//! Strict workspace observability policy parsed from `clinker.toml`.
//!
//! This module owns only the secret-free author form and deterministic numeric
//! validation. In particular, the OTLP endpoint remains bounded raw text. A
//! later network boundary is solely responsible for parsing and admitting it.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use clinker_core_types::FailureClassification;
use serde::{Deserialize, Serialize};

use super::utils::ByteSize;

// Every byte default and ceiling here is written in the grammar `ByteSize`
// parses, where `KB`/`MB`/`GB` are powers of a thousand. A default spelled in
// binary would be a quantity no author can write: `arena_bytes = "4MB"` is
// 4_000_000, so a 4 MiB default is a number the documented spelling of that
// same default does not produce.
const DEFAULT_ARENA_BYTES: u64 = 4_000_000;
/// The share of the arena the high-severity lane takes when the lanes are not
/// spelled out. The rest is the ordinary lane, so the two partition the arena
/// exactly whatever `arena_bytes` is.
const DEFAULT_HIGH_SEVERITY_LANE_DIVISOR: u64 = 4;
const DEFAULT_MAX_BATCH_BYTES: u64 = 256_000;
const DEFAULT_MAX_ATTRIBUTES_PER_EVENT: u32 = 32;
const DEFAULT_MAX_ATTRIBUTE_BYTES: u64 = 4_000;
const DEFAULT_SAMPLE_EVERY: u32 = 1;
const DEFAULT_RATE_LIMIT_PER_SECOND: u32 = 1_000;
const DEFAULT_RATE_LIMIT_BURST: u32 = 1_000;
const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_RETRY_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_TOTAL_TIMEOUT_MS: u64 = 10_000;
const RETRY_INITIAL_BACKOFF_MS: u64 = 100;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 64_000;
const DEFAULT_LINEAGE_QUEUE_BYTES: u64 = 1_000_000;
const DEFAULT_LINEAGE_MAX_EVENT_BYTES: u64 = 64_000;
const DEFAULT_LINEAGE_FLUSH_TIMEOUT_MS: u64 = 5_000;

const MAX_ENDPOINT_BYTES: usize = 2_048;
const MAX_AUTH_REFERENCE_BYTES: usize = 256;
const MAX_SELECTOR_BYTES: usize = 128;
const MAX_DATASET_IDENTITY_BYTES: usize = 1_024;
const MAX_REPLACEMENT_BYTES: usize = 1_024;
const MAX_FIELD_POLICIES: usize = 256;
const MAX_DATASET_BINDINGS: usize = 1_024;
const MAX_ARENA_BYTES: u64 = 64_000_000;
const MAX_BATCH_BYTES: u64 = 1_000_000;
const MAX_ATTRIBUTES_PER_EVENT: u32 = 256;
const MAX_ATTRIBUTE_BYTES: u64 = 64_000;
const MAX_SAMPLE_EVERY: u32 = 1_000_000;
const MAX_RATE_LIMIT: u32 = 1_000_000;
const MAX_TIMEOUT_MS: u64 = 60_000;
const MAX_RETRY_ATTEMPTS: u32 = 10;
const MAX_RESPONSE_BYTES: u64 = 1_000_000;
const MAX_LINEAGE_QUEUE_BYTES: u64 = 64_000_000;
const MAX_LINEAGE_EVENT_BYTES: u64 = 1_000_000;

fn default_max_batch_bytes() -> ByteSize {
    ByteSize(DEFAULT_MAX_BATCH_BYTES)
}

fn default_max_attributes_per_event() -> u32 {
    DEFAULT_MAX_ATTRIBUTES_PER_EVENT
}

fn default_max_attribute_bytes() -> ByteSize {
    ByteSize(DEFAULT_MAX_ATTRIBUTE_BYTES)
}

fn default_sample_every() -> u32 {
    DEFAULT_SAMPLE_EVERY
}

fn default_rate_limit_per_second() -> u32 {
    DEFAULT_RATE_LIMIT_PER_SECOND
}

fn default_rate_limit_burst() -> u32 {
    DEFAULT_RATE_LIMIT_BURST
}

fn default_flush_timeout_ms() -> u64 {
    DEFAULT_FLUSH_TIMEOUT_MS
}

fn default_connect_timeout_ms() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_MS
}

fn default_request_timeout_ms() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_MS
}

fn default_retry_max_attempts() -> u32 {
    DEFAULT_RETRY_MAX_ATTEMPTS
}

fn default_retry_total_timeout_ms() -> u64 {
    DEFAULT_RETRY_TOTAL_TIMEOUT_MS
}

fn default_max_response_bytes() -> ByteSize {
    ByteSize(DEFAULT_MAX_RESPONSE_BYTES)
}

fn default_lineage_queue_bytes() -> ByteSize {
    ByteSize(DEFAULT_LINEAGE_QUEUE_BYTES)
}

fn default_lineage_max_event_bytes() -> ByteSize {
    ByteSize(DEFAULT_LINEAGE_MAX_EVENT_BYTES)
}

fn default_lineage_flush_timeout_ms() -> u64 {
    DEFAULT_LINEAGE_FLUSH_TIMEOUT_MS
}

/// Complete workspace author form for optional telemetry and lineage delivery.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    // The arena and its two lanes carry one equality between them -- the lanes
    // partition the arena exactly -- so each is optional and what is left out
    // is derived from what is written. Three independent defaults could not do
    // that: any single override broke an equality the other two still held,
    // and the run was refused for a file that named one quantity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    arena_bytes: Option<ByteSize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ordinary_lane_bytes: Option<ByteSize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    high_severity_lane_bytes: Option<ByteSize>,
    #[serde(default = "default_max_batch_bytes")]
    max_batch_bytes: ByteSize,
    #[serde(default = "default_max_attributes_per_event")]
    max_attributes_per_event: u32,
    #[serde(default = "default_max_attribute_bytes")]
    max_attribute_bytes: ByteSize,
    #[serde(default)]
    drop_policy: ObservabilityDropPolicy,
    #[serde(default = "default_sample_every")]
    sample_every: u32,
    #[serde(default = "default_rate_limit_per_second")]
    rate_limit_per_second: u32,
    #[serde(default = "default_rate_limit_burst")]
    rate_limit_burst: u32,
    #[serde(default = "default_flush_timeout_ms")]
    flush_timeout_ms: u64,
    // Two independent delivery paths, each optional. Requiring both tables
    // would make a lineage export declare a collector endpoint it never
    // contacts, and a collector export declare a lineage sink it never writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    otlp: Option<OtlpConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lineage: Option<LineageConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    field_policy: Vec<FieldPolicyConfig>,
}

impl std::fmt::Debug for ObservabilityConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservabilityConfig")
            .field("arena_bytes", &self.arena_bytes)
            .field("ordinary_lane_bytes", &self.ordinary_lane_bytes)
            .field("high_severity_lane_bytes", &self.high_severity_lane_bytes)
            .field("max_batch_bytes", &self.max_batch_bytes)
            .field("max_attributes_per_event", &self.max_attributes_per_event)
            .field("max_attribute_bytes", &self.max_attribute_bytes)
            .field("drop_policy", &self.drop_policy)
            .field("sample_every", &self.sample_every)
            .field("rate_limit_per_second", &self.rate_limit_per_second)
            .field("rate_limit_burst", &self.rate_limit_burst)
            .field("flush_timeout_ms", &self.flush_timeout_ms)
            .field("otlp", &"<configured>")
            .field("lineage", &"<configured>")
            .field("field_policy_count", &self.field_policy.len())
            .finish()
    }
}

/// Raw OTLP destination and deterministic delivery bounds.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OtlpConfig {
    endpoint: String,
    #[serde(default = "default_connect_timeout_ms")]
    connect_timeout_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_retry_max_attempts")]
    retry_max_attempts: u32,
    #[serde(default = "default_retry_total_timeout_ms")]
    retry_total_timeout_ms: u64,
    #[serde(default = "default_max_response_bytes")]
    max_response_bytes: ByteSize,
    auth: ObservabilityAuthConfig,
}

/// Exact secret-free authentication choice recorded by workspace policy.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservabilityAuthConfig {
    mode: ObservabilityAuthMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reference: Option<String>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ObservabilityAuthMode {
    None,
    Reference,
}

/// Author form for the independently bounded lineage delivery path.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LineageConfig {
    #[serde(default = "default_lineage_queue_bytes")]
    queue_bytes: ByteSize,
    #[serde(default = "default_lineage_max_event_bytes")]
    max_event_bytes: ByteSize,
    #[serde(default)]
    drop_policy: ObservabilityDropPolicy,
    #[serde(default = "default_lineage_flush_timeout_ms")]
    flush_timeout_ms: u64,
    #[serde(default)]
    identity_mode: LineageIdentityMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dataset: Vec<LineageDatasetConfig>,
}

/// One exact node-to-dataset binding. Validation admits exactly one identity
/// shape: `canonical_datasource`, or the catalog namespace/name pair.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LineageDatasetConfig {
    node: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    canonical_datasource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    catalog_namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    catalog_name: Option<String>,
}

/// One explicit event-field privacy decision. Unlisted fields remain denied.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldPolicyConfig {
    event: String,
    field: String,
    action: FieldPolicyAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replacement: Option<String>,
}

/// Non-blocking behavior for a full preallocated observability queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservabilityDropPolicy {
    #[default]
    DropNewest,
}

/// Privacy action for an exact event-field pair.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldPolicyAction {
    Allow,
    Hash,
    Replace,
}

/// Dataset identity mode for OpenLineage production or local diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineageIdentityMode {
    #[default]
    External,
    LocalDiagnosticPaths,
}

/// Complete immutable policy passed to later observability consumers.
#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedObservabilityPolicy {
    enabled: bool,
    arena_bytes: u64,
    ordinary_lane_bytes: u64,
    high_severity_lane_bytes: u64,
    max_batch_bytes: u64,
    max_attributes_per_event: u32,
    max_attribute_bytes: u64,
    drop_policy: ObservabilityDropPolicy,
    sample_every: u32,
    rate_limit_per_second: u32,
    rate_limit_burst: u32,
    flush_timeout: Duration,
    otlp: Option<ResolvedOtlpPolicy>,
    lineage: Option<ResolvedLineageDeliveryPolicy>,
    field_policies: Vec<ResolvedFieldPolicy>,
}

impl std::fmt::Debug for ResolvedObservabilityPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedObservabilityPolicy")
            .field("enabled", &self.enabled)
            .field("arena_bytes", &self.arena_bytes)
            .field("ordinary_lane_bytes", &self.ordinary_lane_bytes)
            .field("high_severity_lane_bytes", &self.high_severity_lane_bytes)
            .field("max_batch_bytes", &self.max_batch_bytes)
            .field("max_attributes_per_event", &self.max_attributes_per_event)
            .field("max_attribute_bytes", &self.max_attribute_bytes)
            .field("drop_policy", &self.drop_policy)
            .field("sample_every", &self.sample_every)
            .field("rate_limit_per_second", &self.rate_limit_per_second)
            .field("rate_limit_burst", &self.rate_limit_burst)
            .field("flush_timeout", &self.flush_timeout)
            .field("otlp", &self.otlp.as_ref().map(|_| "<configured>"))
            .field("lineage", &self.lineage.as_ref().map(|_| "<configured>"))
            .field("field_policy_count", &self.field_policies.len())
            .finish()
    }
}

impl ResolvedObservabilityPolicy {
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            arena_bytes: 0,
            ordinary_lane_bytes: 0,
            high_severity_lane_bytes: 0,
            max_batch_bytes: 0,
            max_attributes_per_event: 0,
            max_attribute_bytes: 0,
            drop_policy: ObservabilityDropPolicy::DropNewest,
            sample_every: 0,
            rate_limit_per_second: 0,
            rate_limit_burst: 0,
            flush_timeout: Duration::ZERO,
            otlp: None,
            lineage: None,
            field_policies: Vec::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn arena_bytes(&self) -> u64 {
        self.arena_bytes
    }

    pub fn ordinary_lane_bytes(&self) -> u64 {
        self.ordinary_lane_bytes
    }

    pub fn high_severity_lane_bytes(&self) -> u64 {
        self.high_severity_lane_bytes
    }

    pub fn max_batch_bytes(&self) -> u64 {
        self.max_batch_bytes
    }

    pub fn max_attributes_per_event(&self) -> u32 {
        self.max_attributes_per_event
    }

    pub fn max_attribute_bytes(&self) -> u64 {
        self.max_attribute_bytes
    }

    pub fn drop_policy(&self) -> ObservabilityDropPolicy {
        self.drop_policy
    }

    pub fn sample_every(&self) -> u32 {
        self.sample_every
    }

    pub fn rate_limit_per_second(&self) -> u32 {
        self.rate_limit_per_second
    }

    pub fn rate_limit_burst(&self) -> u32 {
        self.rate_limit_burst
    }

    pub fn flush_timeout(&self) -> Duration {
        self.flush_timeout
    }

    pub fn otlp(&self) -> Option<&ResolvedOtlpPolicy> {
        self.otlp.as_ref()
    }

    pub fn lineage(&self) -> Option<&ResolvedLineageDeliveryPolicy> {
        self.lineage.as_ref()
    }

    pub fn field_policies(&self) -> &[ResolvedFieldPolicy] {
        &self.field_policies
    }
}

/// Raw endpoint, explicit auth intent, and bounded OTLP delivery policy.
#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedOtlpPolicy {
    raw_endpoint: Box<str>,
    auth: ObservabilityAuth,
    connect_timeout: Duration,
    request_timeout: Duration,
    retry_max_attempts: NonZeroU32,
    retry_total_timeout: Duration,
    retry_initial_backoff: Duration,
    max_response_bytes: NonZeroU64,
}

impl std::fmt::Debug for ResolvedOtlpPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedOtlpPolicy")
            .field("raw_endpoint", &"<configured>")
            .field("auth", &self.auth)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("retry_max_attempts", &self.retry_max_attempts)
            .field("retry_total_timeout", &self.retry_total_timeout)
            .field("retry_initial_backoff", &self.retry_initial_backoff)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

impl ResolvedOtlpPolicy {
    pub fn raw_endpoint(&self) -> &str {
        &self.raw_endpoint
    }

    pub fn auth(&self) -> &ObservabilityAuth {
        &self.auth
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub fn retry_max_attempts(&self) -> NonZeroU32 {
        self.retry_max_attempts
    }

    pub fn retry_total_timeout(&self) -> Duration {
        self.retry_total_timeout
    }

    pub fn retry_initial_backoff(&self) -> Duration {
        self.retry_initial_backoff
    }

    pub fn max_response_bytes(&self) -> NonZeroU64 {
        self.max_response_bytes
    }
}

/// Explicit credential-free intent or one provider-neutral logical reference.
#[derive(Clone, Eq, PartialEq)]
pub enum ObservabilityAuth {
    None,
    Reference { reference: Box<str> },
}

impl std::fmt::Debug for ObservabilityAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Reference { .. } => f.write_str("Reference { reference: <configured> }"),
        }
    }
}

impl ObservabilityAuth {
    pub fn reference(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Reference { reference } => Some(reference),
        }
    }
}

/// Immutable, separately bounded OpenLineage delivery policy.
#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedLineageDeliveryPolicy {
    queue_bytes: NonZeroU64,
    max_event_bytes: NonZeroU64,
    drop_policy: ObservabilityDropPolicy,
    flush_timeout: Duration,
    identity_mode: LineageIdentityMode,
    datasets: Vec<ResolvedLineageDataset>,
}

impl std::fmt::Debug for ResolvedLineageDeliveryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedLineageDeliveryPolicy")
            .field("queue_bytes", &self.queue_bytes)
            .field("max_event_bytes", &self.max_event_bytes)
            .field("drop_policy", &self.drop_policy)
            .field("flush_timeout", &self.flush_timeout)
            .field("identity_mode", &self.identity_mode)
            .field("dataset_count", &self.datasets.len())
            .finish()
    }
}

impl ResolvedLineageDeliveryPolicy {
    pub fn queue_bytes(&self) -> NonZeroU64 {
        self.queue_bytes
    }

    pub fn max_event_bytes(&self) -> NonZeroU64 {
        self.max_event_bytes
    }

    pub fn drop_policy(&self) -> ObservabilityDropPolicy {
        self.drop_policy
    }

    pub fn flush_timeout(&self) -> Duration {
        self.flush_timeout
    }

    pub fn identity_mode(&self) -> LineageIdentityMode {
        self.identity_mode
    }

    pub fn datasets(&self) -> &[ResolvedLineageDataset] {
        &self.datasets
    }
}

/// One resolved node binding, sorted by logical node name.
#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedLineageDataset {
    node: Box<str>,
    identity: LineageDatasetIdentity,
}

impl std::fmt::Debug for ResolvedLineageDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedLineageDataset")
            .field("node", &self.node)
            .field("identity", &"<configured>")
            .finish()
    }
}

impl ResolvedLineageDataset {
    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn identity(&self) -> &LineageDatasetIdentity {
        &self.identity
    }
}

/// Closed external identity form consumed by the lineage crate in a later plan.
#[derive(Clone, Eq, PartialEq)]
pub enum LineageDatasetIdentity {
    CanonicalDatasource { identifier: Box<str> },
    Catalog { namespace: Box<str>, name: Box<str> },
}

impl std::fmt::Debug for LineageDatasetIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CanonicalDatasource { .. } => {
                f.write_str("CanonicalDatasource { identifier: <configured> }")
            }
            Self::Catalog { .. } => {
                f.write_str("Catalog { namespace: <configured>, name: <configured> }")
            }
        }
    }
}

/// One validated exact event-field privacy action.
#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedFieldPolicy {
    event: Box<str>,
    field: Box<str>,
    action: FieldPolicyAction,
    replacement: Option<Box<str>>,
}

impl std::fmt::Debug for ResolvedFieldPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedFieldPolicy")
            .field("event", &self.event)
            .field("field", &self.field)
            .field("action", &self.action)
            .field(
                "replacement",
                &self.replacement.as_ref().map(|_| "<configured>"),
            )
            .finish()
    }
}

impl ResolvedFieldPolicy {
    pub fn event(&self) -> &str {
        &self.event
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn action(&self) -> FieldPolicyAction {
        self.action
    }

    pub fn replacement(&self) -> Option<&str> {
        self.replacement.as_deref()
    }
}

/// Sanitized deterministic workspace observability configuration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservabilityConfigError {
    field: Box<str>,
    detail: &'static str,
    correction: Box<str>,
    classification: FailureClassification,
}

impl ObservabilityConfigError {
    pub(crate) fn invalid(
        field: impl Into<Box<str>>,
        detail: &'static str,
        correction: impl Into<Box<str>>,
    ) -> Self {
        Self {
            field: field.into(),
            detail,
            correction: correction.into(),
            classification: FailureClassification::for_code("observability.configuration.invalid")
                .expect("observability configuration failure code is registered"),
        }
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn correction(&self) -> &str {
        &self.correction
    }

    pub fn classification(&self) -> &FailureClassification {
        &self.classification
    }

    /// Convert a serde/TOML failure inside the observability subtree into a
    /// field-local diagnostic without copying the rejected value or parser
    /// source excerpt into the error.
    pub(crate) fn from_toml_parse(text: &str, error: &toml::de::Error) -> Self {
        let auth_table = table_body(text, "observability.otlp.auth");
        if auth_table.is_some_and(|body| authored_key(body, "mode").is_none()) {
            return Self::invalid(
                "observability.otlp.auth.mode",
                "is required; omission never selects anonymous delivery",
                auth_correction(),
            );
        }

        let offset = error.span().map_or(text.len(), |span| span.start);
        let (table, line) = authored_location(text, offset);
        let key = key_on_line(line);
        // Rendered the way the author spelled it, so a segment whose name
        // contains a dot stays quoted. Joining the segments plainly made
        // `[observability."otlp.auth"]` read as the nested
        // `[observability.otlp.auth]`, and the correction then told the author
        // to add a key to a table that is not in their file while the key they
        // actually mis-wrote went unnamed.
        let field = match (&table, key) {
            (path, Some(key)) if is_observability_table(path) => {
                format!("{}.{key}", render_path(path))
            }
            (path, None) if is_observability_table(path) => render_path(path),
            _ => "observability".to_owned(),
        };

        let correction = parse_correction(&field, key);
        Self::invalid(
            field,
            "has an unknown, missing, or malformed value in the strict observability policy",
            correction,
        )
    }
}

impl std::fmt::Display for ObservabilityConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}. Correction: {}",
            self.field, self.detail, self.correction
        )
    }
}

impl std::error::Error for ObservabilityConfigError {}

pub(crate) fn is_observability_toml_error(text: &str, error: &toml::de::Error) -> bool {
    // Where the error is, not what the document happens to contain. A
    // whole-document test would claim an unrelated `[storage]` error for this
    // subsystem and report the wrong correction for it.
    // Where the error is, and only when the error says where it is. A failure
    // with no span could be anywhere, and answering from the end of the
    // document claimed whichever table happened to be written last -- so an
    // unrelated failure was reported as this subsystem's, with an invented
    // correction, in place of the parser's own message naming the offending
    // key and line.
    let Some(span) = error.span() else {
        return false;
    };
    let (table, _) = authored_location(text, span.start);
    is_observability_table(&table)
}

/// A table's key path as TOML spells it, quoting any segment that is not a
/// bare key so the rendering is reversible.
///
/// Quoted through TOML's own serializer rather than Rust's `Debug`, which
/// escapes several characters differently: a diagnostic is meant to hand the
/// author something they can paste back, and a name rendered in the wrong
/// escape syntax gives them a second parse error instead of a fix.
///
/// Through the serializer's *key* position specifically. Asking it to render
/// the same text as a value spells a newline-bearing name as a multi-line
/// string, which TOML does not accept as a key at all -- so the correction
/// was unparseable in exactly the case the quoting exists to handle.
fn render_path(path: &[String]) -> String {
    path.iter()
        .map(|segment| render_key(segment))
        .collect::<Vec<_>>()
        .join(".")
}

/// One key segment as TOML spells it in key position.
fn render_key(segment: &str) -> String {
    if !segment.is_empty()
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return segment.to_owned();
    }
    let mut table = toml::Table::new();
    table.insert(segment.to_owned(), toml::Value::Integer(0));
    let rendered = table.to_string();
    rendered
        .trim_end()
        .rsplit_once(" = ")
        .map_or_else(|| segment.to_owned(), |(key, _)| key.to_owned())
}

/// Whether a table's key path is inside the observability policy.
///
/// The first segment has to *be* `observability`, not merely start with it: a
/// table whose one quoted name is `observability.otlp` is a different table,
/// and claiming it for this subsystem reported someone else's error here with
/// a correction naming keys that table never had.
fn is_observability_table(path: &[String]) -> bool {
    path.first()
        .is_some_and(|segment| segment == "observability")
}

/// The telemetry arena and the two disjoint lanes that partition it exactly.
struct TelemetryArena {
    arena_bytes: u64,
    ordinary_lane_bytes: u64,
    high_severity_lane_bytes: u64,
}

impl ObservabilityConfig {
    /// Resolve the arena and its two lanes from whichever of the three the
    /// author wrote.
    ///
    /// One equality holds over the three -- the lanes partition the arena, so
    /// no telemetry byte is charged twice and none is unreachable. That leaves
    /// two free quantities, and the author may write any of them:
    ///
    /// - all three: the equality is checked, not assumed;
    /// - the arena alone, or nothing at all: the lanes split it, the
    ///   high-severity lane taking one part in
    ///   [`DEFAULT_HIGH_SEVERITY_LANE_DIVISOR`] and the ordinary lane taking
    ///   the remainder, so the split is exact for every arena size rather than
    ///   only for the default one;
    /// - the arena and one lane: the other lane is what is left of the arena;
    /// - both lanes: the arena is their sum;
    /// - one lane alone: the arena keeps its default and the other lane is
    ///   what is left of it.
    ///
    /// The arena is the budget in every case; a lane is never allowed to grow
    /// it. Deriving rather than defaulting is what makes a partial override
    /// work at all: three independently defaulted quantities cannot satisfy an
    /// equality between them unless the author restates all three.
    fn resolve_arena(&self) -> Result<TelemetryArena, ObservabilityConfigError> {
        let ordinary = self
            .ordinary_lane_bytes
            .map(|value| {
                bounded_nonzero_u64(
                    "observability.ordinary_lane_bytes",
                    value.0,
                    MAX_ARENA_BYTES,
                    "set `ordinary_lane_bytes = \"3MB\"`",
                )
            })
            .transpose()?;
        let high_severity = self
            .high_severity_lane_bytes
            .map(|value| {
                bounded_nonzero_u64(
                    "observability.high_severity_lane_bytes",
                    value.0,
                    MAX_ARENA_BYTES,
                    "set `high_severity_lane_bytes = \"1MB\"`",
                )
            })
            .transpose()?;

        let arena_bytes = match (self.arena_bytes, ordinary, high_severity) {
            (Some(arena), _, _) => bounded_nonzero_u64(
                "observability.arena_bytes",
                arena.0,
                MAX_ARENA_BYTES,
                "set `arena_bytes = \"4MB\"`",
            )?,
            // Two lanes and no arena name the arena between them.
            (None, Some(ordinary), Some(high_severity)) => bounded_nonzero_u64(
                "observability.arena_bytes",
                ordinary.checked_add(high_severity).ok_or_else(|| {
                    ObservabilityConfigError::invalid(
                        "observability.arena_bytes",
                        "cannot represent the sum of the two telemetry lanes",
                        "set the two lane byte caps so their sum is within the documented arena ceiling",
                    )
                })?,
                MAX_ARENA_BYTES,
                "lower the two lane byte caps so their sum is within the documented arena ceiling",
            )?,
            (None, _, _) => DEFAULT_ARENA_BYTES,
        };

        let (ordinary_lane_bytes, high_severity_lane_bytes) = match (ordinary, high_severity) {
            (Some(ordinary), Some(high_severity)) => {
                if ordinary.checked_add(high_severity) != Some(arena_bytes) {
                    return Err(ObservabilityConfigError::invalid(
                        "observability.arena_bytes",
                        "must equal the exact sum of `ordinary_lane_bytes` and `high_severity_lane_bytes`",
                        "remove `arena_bytes` to take it from the two lane caps, or set it to their exact sum",
                    ));
                }
                (ordinary, high_severity)
            }
            (Some(ordinary), None) => (
                ordinary,
                remainder_of_arena(
                    arena_bytes,
                    ordinary,
                    "observability.ordinary_lane_bytes",
                    "raise `arena_bytes` above `ordinary_lane_bytes`, or lower `ordinary_lane_bytes` below it",
                )?,
            ),
            (None, Some(high_severity)) => (
                remainder_of_arena(
                    arena_bytes,
                    high_severity,
                    "observability.high_severity_lane_bytes",
                    "raise `arena_bytes` above `high_severity_lane_bytes`, or lower `high_severity_lane_bytes` below it",
                )?,
                high_severity,
            ),
            (None, None) => {
                let high_severity = (arena_bytes / DEFAULT_HIGH_SEVERITY_LANE_DIVISOR).max(1);
                (
                    remainder_of_arena(
                        arena_bytes,
                        high_severity,
                        "observability.arena_bytes",
                        "set `arena_bytes = \"4MB\"`",
                    )?,
                    high_severity,
                )
            }
        };

        Ok(TelemetryArena {
            arena_bytes,
            ordinary_lane_bytes,
            high_severity_lane_bytes,
        })
    }

    pub(crate) fn resolve(&self) -> Result<ResolvedObservabilityPolicy, ObservabilityConfigError> {
        let TelemetryArena {
            arena_bytes,
            ordinary_lane_bytes,
            high_severity_lane_bytes,
        } = self.resolve_arena()?;

        let max_batch_bytes = bounded_nonzero_u64(
            "observability.max_batch_bytes",
            self.max_batch_bytes.0,
            MAX_BATCH_BYTES,
            "set `max_batch_bytes = \"256KB\"`",
        )?;
        if max_batch_bytes > ordinary_lane_bytes || max_batch_bytes > high_severity_lane_bytes {
            return Err(ObservabilityConfigError::invalid(
                "observability.max_batch_bytes",
                "must fit independently in each disjoint telemetry lane",
                "set `max_batch_bytes` no larger than the smaller lane cap",
            ));
        }

        let max_attributes_per_event = bounded_nonzero_u32(
            "observability.max_attributes_per_event",
            self.max_attributes_per_event,
            MAX_ATTRIBUTES_PER_EVENT,
            "set `max_attributes_per_event = 32`",
        )?;
        let max_attribute_bytes = bounded_nonzero_u64(
            "observability.max_attribute_bytes",
            self.max_attribute_bytes.0,
            MAX_ATTRIBUTE_BYTES,
            "set `max_attribute_bytes = \"4KB\"`",
        )?;
        let sample_every = bounded_nonzero_u32(
            "observability.sample_every",
            self.sample_every,
            MAX_SAMPLE_EVERY,
            "set `sample_every = 1`",
        )?;
        let rate_limit_per_second = bounded_nonzero_u32(
            "observability.rate_limit_per_second",
            self.rate_limit_per_second,
            MAX_RATE_LIMIT,
            "set `rate_limit_per_second = 1000`",
        )?;
        let rate_limit_burst = bounded_nonzero_u32(
            "observability.rate_limit_burst",
            self.rate_limit_burst,
            MAX_RATE_LIMIT,
            "set `rate_limit_burst = 1000`",
        )?;
        let flush_timeout_ms = bounded_nonzero_u64(
            "observability.flush_timeout_ms",
            self.flush_timeout_ms,
            MAX_TIMEOUT_MS,
            "set `flush_timeout_ms = 15000`",
        )?;

        let otlp = self
            .otlp
            .as_ref()
            .map(|otlp| otlp.resolve(flush_timeout_ms))
            .transpose()?;
        let lineage = self
            .lineage
            .as_ref()
            .map(LineageConfig::resolve)
            .transpose()?;
        let field_policies = resolve_field_policies(&self.field_policy)?;

        Ok(ResolvedObservabilityPolicy {
            enabled: true,
            arena_bytes,
            ordinary_lane_bytes,
            high_severity_lane_bytes,
            max_batch_bytes,
            max_attributes_per_event,
            max_attribute_bytes,
            drop_policy: self.drop_policy,
            sample_every,
            rate_limit_per_second,
            rate_limit_burst,
            flush_timeout: Duration::from_millis(flush_timeout_ms),
            otlp,
            lineage,
            field_policies,
        })
    }
}

impl OtlpConfig {
    fn resolve(
        &self,
        flush_timeout_ms: u64,
    ) -> Result<ResolvedOtlpPolicy, ObservabilityConfigError> {
        if self.endpoint.trim().is_empty() {
            return Err(ObservabilityConfigError::invalid(
                "observability.otlp.endpoint",
                "must contain bounded raw Collector endpoint text",
                "set `endpoint = \"https://collector.example.com\"`",
            ));
        }
        if self.endpoint.len() > MAX_ENDPOINT_BYTES {
            return Err(ObservabilityConfigError::invalid(
                "observability.otlp.endpoint",
                "exceeds the 2048-byte raw-text limit",
                "set `endpoint = \"https://collector.example.com\"` to a shorter origin",
            ));
        }

        let connect_timeout_ms = bounded_nonzero_u64(
            "observability.otlp.connect_timeout_ms",
            self.connect_timeout_ms,
            MAX_TIMEOUT_MS,
            "set `connect_timeout_ms = 1000`",
        )?;
        let request_timeout_ms = bounded_nonzero_u64(
            "observability.otlp.request_timeout_ms",
            self.request_timeout_ms,
            MAX_TIMEOUT_MS,
            "set `request_timeout_ms = 5000`",
        )?;
        let retry_max_attempts = bounded_nonzero_u32(
            "observability.otlp.retry_max_attempts",
            self.retry_max_attempts,
            MAX_RETRY_ATTEMPTS,
            "set `retry_max_attempts = 3`",
        )?;
        let retry_total_timeout_ms = bounded_nonzero_u64(
            "observability.otlp.retry_total_timeout_ms",
            self.retry_total_timeout_ms,
            MAX_TIMEOUT_MS,
            "set `retry_total_timeout_ms = 10000`",
        )?;
        let max_response_bytes = bounded_nonzero_u64(
            "observability.otlp.max_response_bytes",
            self.max_response_bytes.0,
            MAX_RESPONSE_BYTES,
            "set `max_response_bytes = \"64KB\"`",
        )?;

        if connect_timeout_ms > request_timeout_ms {
            return Err(ObservabilityConfigError::invalid(
                "observability.otlp.connect_timeout_ms",
                "cannot exceed `request_timeout_ms`",
                "set `connect_timeout_ms = 1000` and `request_timeout_ms = 5000`",
            ));
        }
        if request_timeout_ms > retry_total_timeout_ms {
            return Err(ObservabilityConfigError::invalid(
                "observability.otlp.request_timeout_ms",
                "cannot exceed `retry_total_timeout_ms`",
                "set `request_timeout_ms = 5000` and `retry_total_timeout_ms = 10000`",
            ));
        }
        if retry_total_timeout_ms > flush_timeout_ms {
            return Err(ObservabilityConfigError::invalid(
                "observability.otlp.retry_total_timeout_ms",
                "cannot exceed `observability.flush_timeout_ms`",
                "set `retry_total_timeout_ms = 10000` and `flush_timeout_ms = 15000`",
            ));
        }

        let auth = match (self.auth.mode, self.auth.reference.as_deref()) {
            (ObservabilityAuthMode::None, None) => ObservabilityAuth::None,
            (ObservabilityAuthMode::None, Some(_)) => {
                return Err(ObservabilityConfigError::invalid(
                    "observability.otlp.auth.reference",
                    "is not accepted when `mode = \"none\"`",
                    "use `[observability.otlp.auth]\nmode = \"none\"` with no other field",
                ));
            }
            (ObservabilityAuthMode::Reference, Some(reference)) => {
                validate_logical_reference(reference)?;
                ObservabilityAuth::Reference {
                    reference: reference.into(),
                }
            }
            (ObservabilityAuthMode::Reference, None) => {
                return Err(ObservabilityConfigError::invalid(
                    "observability.otlp.auth.reference",
                    "is required when `mode = \"reference\"`",
                    "use `[observability.otlp.auth]\nmode = \"reference\"\nreference = \"telemetry/production\"`",
                ));
            }
        };

        Ok(ResolvedOtlpPolicy {
            raw_endpoint: self.endpoint.clone().into_boxed_str(),
            auth,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            request_timeout: Duration::from_millis(request_timeout_ms),
            retry_max_attempts: NonZeroU32::new(retry_max_attempts)
                .expect("validated retry attempts are non-zero"),
            retry_total_timeout: Duration::from_millis(retry_total_timeout_ms),
            retry_initial_backoff: Duration::from_millis(RETRY_INITIAL_BACKOFF_MS),
            max_response_bytes: NonZeroU64::new(max_response_bytes)
                .expect("validated response cap is non-zero"),
        })
    }
}

impl LineageConfig {
    fn resolve(&self) -> Result<ResolvedLineageDeliveryPolicy, ObservabilityConfigError> {
        let queue_bytes = bounded_nonzero_u64(
            "observability.lineage.queue_bytes",
            self.queue_bytes.0,
            MAX_LINEAGE_QUEUE_BYTES,
            "set `queue_bytes = \"1MB\"` under `[observability.lineage]`",
        )?;
        let max_event_bytes = bounded_nonzero_u64(
            "observability.lineage.max_event_bytes",
            self.max_event_bytes.0,
            MAX_LINEAGE_EVENT_BYTES,
            "set `max_event_bytes = \"64KB\"` under `[observability.lineage]`",
        )?;
        if max_event_bytes > queue_bytes {
            return Err(ObservabilityConfigError::invalid(
                "observability.lineage.max_event_bytes",
                "cannot exceed the independently reserved lineage `queue_bytes` cap",
                "set `max_event_bytes` no larger than `queue_bytes`",
            ));
        }
        let flush_timeout_ms = bounded_nonzero_u64(
            "observability.lineage.flush_timeout_ms",
            self.flush_timeout_ms,
            MAX_TIMEOUT_MS,
            "set `flush_timeout_ms = 5000` under `[observability.lineage]`",
        )?;

        if self.dataset.len() > MAX_DATASET_BINDINGS {
            return Err(ObservabilityConfigError::invalid(
                "observability.lineage.dataset",
                "contains too many per-node bindings",
                "retain at most one binding per source or output node",
            ));
        }

        let datasets = match self.identity_mode {
            LineageIdentityMode::External => resolve_dataset_bindings(&self.dataset)?,
            LineageIdentityMode::LocalDiagnosticPaths if self.dataset.is_empty() => Vec::new(),
            LineageIdentityMode::LocalDiagnosticPaths => {
                return Err(ObservabilityConfigError::invalid(
                    "observability.lineage.dataset",
                    "is not used by explicit local diagnostic path compatibility mode",
                    "remove every `[[observability.lineage.dataset]]` table, or set `identity_mode = \"external\"`",
                ));
            }
        };

        Ok(ResolvedLineageDeliveryPolicy {
            queue_bytes: NonZeroU64::new(queue_bytes)
                .expect("validated lineage queue cap is non-zero"),
            max_event_bytes: NonZeroU64::new(max_event_bytes)
                .expect("validated lineage event cap is non-zero"),
            drop_policy: self.drop_policy,
            flush_timeout: Duration::from_millis(flush_timeout_ms),
            identity_mode: self.identity_mode,
            datasets,
        })
    }
}

fn resolve_dataset_bindings(
    configured: &[LineageDatasetConfig],
) -> Result<Vec<ResolvedLineageDataset>, ObservabilityConfigError> {
    if configured.is_empty() {
        return Err(ObservabilityConfigError::invalid(
            "observability.lineage.dataset",
            "requires one complete binding for every externally emitted source and output node",
            "add `[[observability.lineage.dataset]]` with `node` and exactly one canonical or catalog identity",
        ));
    }

    let mut by_node = BTreeMap::new();
    for binding in configured {
        validate_bounded_logical_text(
            "observability.lineage.dataset.node",
            &binding.node,
            MAX_SELECTOR_BYTES,
            "set `node = \"source_node\"` to an exact logical pipeline node name",
        )?;

        let identity = match (
            binding.canonical_datasource.as_deref(),
            binding.catalog_namespace.as_deref(),
            binding.catalog_name.as_deref(),
        ) {
            (Some(identifier), None, None) => {
                validate_bounded_logical_text(
                    "observability.lineage.dataset.canonical_datasource",
                    identifier,
                    MAX_DATASET_IDENTITY_BYTES,
                    "set one non-empty `canonical_datasource`, or use the complete catalog pair",
                )?;
                LineageDatasetIdentity::CanonicalDatasource {
                    identifier: identifier.into(),
                }
            }
            (None, Some(namespace), Some(name)) => {
                validate_bounded_logical_text(
                    "observability.lineage.dataset.catalog_namespace",
                    namespace,
                    MAX_DATASET_IDENTITY_BYTES,
                    "set both `catalog_namespace` and `catalog_name`",
                )?;
                validate_bounded_logical_text(
                    "observability.lineage.dataset.catalog_name",
                    name,
                    MAX_DATASET_IDENTITY_BYTES,
                    "set both `catalog_namespace` and `catalog_name`",
                )?;
                LineageDatasetIdentity::Catalog {
                    namespace: namespace.into(),
                    name: name.into(),
                }
            }
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                return Err(ObservabilityConfigError::invalid(
                    "observability.lineage.dataset",
                    "is ambiguous because canonical and catalog identity fields are mixed",
                    "keep only `canonical_datasource`, or only both `catalog_namespace` and `catalog_name`",
                ));
            }
            (None, Some(_), None) => {
                return Err(ObservabilityConfigError::invalid(
                    "observability.lineage.dataset.catalog_name",
                    "is required with `catalog_namespace`",
                    "add `catalog_name = \"dataset_name\"`",
                ));
            }
            (None, None, Some(_)) => {
                return Err(ObservabilityConfigError::invalid(
                    "observability.lineage.dataset.catalog_namespace",
                    "is required with `catalog_name`",
                    "add `catalog_namespace = \"catalog_namespace\"`",
                ));
            }
            (None, None, None) => {
                return Err(ObservabilityConfigError::invalid(
                    "observability.lineage.dataset",
                    "is missing an external identity",
                    "add exactly one `canonical_datasource`, or both `catalog_namespace` and `catalog_name`",
                ));
            }
        };

        let node: Box<str> = binding.node.clone().into_boxed_str();
        if by_node
            .insert(node.clone(), ResolvedLineageDataset { node, identity })
            .is_some()
        {
            return Err(ObservabilityConfigError::invalid(
                "observability.lineage.dataset.node",
                "appears more than once",
                "retain exactly one dataset binding for each logical node",
            ));
        }
    }

    Ok(by_node.into_values().collect())
}

fn resolve_field_policies(
    configured: &[FieldPolicyConfig],
) -> Result<Vec<ResolvedFieldPolicy>, ObservabilityConfigError> {
    if configured.len() > MAX_FIELD_POLICIES {
        return Err(ObservabilityConfigError::invalid(
            "observability.field_policy",
            "contains too many exact event-field rules",
            "retain no more than 256 exact field rules",
        ));
    }

    let mut seen = BTreeSet::new();
    let mut resolved = Vec::with_capacity(configured.len());
    for rule in configured {
        validate_selector(
            "observability.field_policy.event",
            &rule.event,
            "set `event = \"run.completed\"` to a dotted identifier",
        )?;
        validate_selector(
            "observability.field_policy.field",
            &rule.field,
            "set `field = \"records_written\"` to a dotted identifier",
        )?;
        if !seen.insert((rule.event.as_str(), rule.field.as_str())) {
            return Err(ObservabilityConfigError::invalid(
                "observability.field_policy",
                "contains more than one action for the same exact event-field pair",
                "retain exactly one allow, hash, or replace action for that pair",
            ));
        }

        let replacement = match (rule.action, rule.replacement.as_deref()) {
            (FieldPolicyAction::Replace, Some(value))
                if !value.is_empty() && value.len() <= MAX_REPLACEMENT_BYTES =>
            {
                Some(value.into())
            }
            (FieldPolicyAction::Replace, _) => {
                return Err(ObservabilityConfigError::invalid(
                    "observability.field_policy.replacement",
                    "is required and must fit the bounded replacement value",
                    "add `replacement = \"[redacted]\"`",
                ));
            }
            (_, Some(_)) => {
                return Err(ObservabilityConfigError::invalid(
                    "observability.field_policy.replacement",
                    "is accepted only when `action = \"replace\"`",
                    "remove `replacement`, or set `action = \"replace\"`",
                ));
            }
            (_, None) => None,
        };

        resolved.push(ResolvedFieldPolicy {
            event: rule.event.clone().into_boxed_str(),
            field: rule.field.clone().into_boxed_str(),
            action: rule.action,
            replacement,
        });
    }
    resolved.sort_by(|left, right| (&left.event, &left.field).cmp(&(&right.event, &right.field)));
    Ok(resolved)
}

fn validate_logical_reference(reference: &str) -> Result<(), ObservabilityConfigError> {
    let valid = !reference.is_empty()
        && reference.len() <= MAX_AUTH_REFERENCE_BYTES
        && reference == reference.trim()
        && reference.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':')
        });
    if valid {
        return Ok(());
    }
    Err(ObservabilityConfigError::invalid(
        "observability.otlp.auth.reference",
        "must be one non-empty bounded provider-neutral logical reference",
        "use `[observability.otlp.auth]\nmode = \"reference\"\nreference = \"telemetry/production\"`",
    ))
}

fn validate_selector(
    field: &'static str,
    value: &str,
    correction: &'static str,
) -> Result<(), ObservabilityConfigError> {
    let valid = !value.is_empty()
        && value.len() <= MAX_SELECTOR_BYTES
        && value.split('.').all(|segment| {
            let mut bytes = segment.bytes();
            matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic() || first == b'_')
                && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        });
    if valid {
        return Ok(());
    }
    Err(ObservabilityConfigError::invalid(
        field,
        "must be a bounded dotted identifier",
        correction,
    ))
}

fn validate_bounded_logical_text(
    field: &'static str,
    value: &str,
    maximum: usize,
    correction: &'static str,
) -> Result<(), ObservabilityConfigError> {
    if !value.is_empty()
        && value.len() <= maximum
        && value == value.trim()
        && !value.chars().any(char::is_control)
    {
        return Ok(());
    }
    Err(ObservabilityConfigError::invalid(
        field,
        "must be non-empty bounded logical text without control characters",
        correction,
    ))
}

/// What is left of the arena once one lane has taken `lane_bytes`.
///
/// The other lane gets it, so it has to be positive: a zero-byte lane is one
/// severity of telemetry that can never be admitted, which is a silent loss
/// rather than a bounded one.
fn remainder_of_arena(
    arena_bytes: u64,
    lane_bytes: u64,
    field: &'static str,
    correction: &'static str,
) -> Result<u64, ObservabilityConfigError> {
    arena_bytes
        .checked_sub(lane_bytes)
        .filter(|remainder| *remainder > 0)
        .ok_or_else(|| {
            ObservabilityConfigError::invalid(
                field,
                "must leave a positive remainder of `arena_bytes` for the other telemetry lane",
                correction,
            )
        })
}

fn bounded_nonzero_u64(
    field: &'static str,
    value: u64,
    maximum: u64,
    correction: &'static str,
) -> Result<u64, ObservabilityConfigError> {
    if (1..=maximum).contains(&value) {
        return Ok(value);
    }
    Err(ObservabilityConfigError::invalid(
        field,
        "must be a positive value within the documented hard ceiling",
        correction,
    ))
}

fn bounded_nonzero_u32(
    field: &'static str,
    value: u32,
    maximum: u32,
    correction: &'static str,
) -> Result<u32, ObservabilityConfigError> {
    if (1..=maximum).contains(&value) {
        return Ok(value);
    }
    Err(ObservabilityConfigError::invalid(
        field,
        "must be a positive value within the documented hard ceiling",
        correction,
    ))
}

fn auth_correction() -> &'static str {
    "use `[observability.otlp.auth]\nmode = \"none\"`, or `mode = \"reference\"` with exactly one `reference = \"telemetry/production\"`"
}

fn parse_correction(field: &str, key: Option<&str>) -> Box<str> {
    let correction = if field.starts_with("observability.otlp.auth") {
        auth_correction().to_owned()
    } else {
        match field {
            "observability.drop_policy" | "observability.lineage.drop_policy" => {
                "set `drop_policy = \"drop_newest\"`".to_owned()
            }
            "observability.lineage.identity_mode" => {
                "set `identity_mode = \"external\"`, or explicitly select `identity_mode = \"local_diagnostic_paths\"` for local-only compatibility".to_owned()
            }
            "observability.otlp.endpoint" => {
                "set `endpoint = \"https://collector.example.com\"` as bounded raw text"
                    .to_owned()
            }
            "observability.arena_bytes" => "set `arena_bytes = \"4MB\"`".to_owned(),
            "observability.ordinary_lane_bytes" => {
                "set `ordinary_lane_bytes = \"3MB\"`".to_owned()
            }
            "observability.high_severity_lane_bytes" => {
                "set `high_severity_lane_bytes = \"1MB\"`".to_owned()
            }
            "observability.max_batch_bytes" => {
                "set `max_batch_bytes = \"256KB\"`".to_owned()
            }
            "observability.max_attributes_per_event" => {
                "set `max_attributes_per_event = 32`".to_owned()
            }
            "observability.max_attribute_bytes" => {
                "set `max_attribute_bytes = \"4KB\"`".to_owned()
            }
            "observability.sample_every" => "set `sample_every = 1`".to_owned(),
            "observability.rate_limit_per_second" => {
                "set `rate_limit_per_second = 1000`".to_owned()
            }
            "observability.rate_limit_burst" => {
                "set `rate_limit_burst = 1000`".to_owned()
            }
            "observability.flush_timeout_ms" => {
                "set `flush_timeout_ms = 15000`".to_owned()
            }
            "observability.otlp.connect_timeout_ms" => {
                "set `connect_timeout_ms = 1000`".to_owned()
            }
            "observability.otlp.request_timeout_ms" => {
                "set `request_timeout_ms = 5000`".to_owned()
            }
            "observability.otlp.retry_max_attempts" => {
                "set `retry_max_attempts = 3`".to_owned()
            }
            "observability.otlp.retry_total_timeout_ms" => {
                "set `retry_total_timeout_ms = 10000`".to_owned()
            }
            "observability.otlp.max_response_bytes" => {
                "set `max_response_bytes = \"64KB\"`".to_owned()
            }
            "observability.lineage.queue_bytes" => {
                "set `queue_bytes = \"1MB\"` under `[observability.lineage]`".to_owned()
            }
            "observability.lineage.max_event_bytes" => {
                "set `max_event_bytes = \"64KB\"` under `[observability.lineage]`"
                    .to_owned()
            }
            "observability.lineage.flush_timeout_ms" => {
                "set `flush_timeout_ms = 5000` under `[observability.lineage]`".to_owned()
            }
            "observability.field_policy.action" => {
                "set `action = \"allow\"`, `\"hash\"`, or `\"replace\"`".to_owned()
            }
            _ => key.map_or_else(
                || "supply the complete documented `[observability]` policy".to_owned(),
                |key| format!("remove `{key}` or replace it with the documented exact key"),
            ),
        }
    };
    correction.into_boxed_str()
}

/// The key path of the table a line declares, or `None` when the line is not a
/// table header at that point in the document.
///
/// Answered by the TOML parser, not by matching brackets. This code runs on a
/// document the parser has already rejected, so the whole document cannot be
/// parsed -- but a line can, and a table header is a line that parses on its
/// own as a document declaring exactly one table.
///
/// That test alone is not enough, because `["read"]` is both a legal header
/// and a legal array element. The two are told apart by where they sit: a
/// header can only begin where the preceding text is complete, and text with
/// an array still open is not. `preceding` is therefore parsed too, and a
/// candidate inside an unclosed value is refused.
///
/// The path is returned in segments rather than joined, because `["a.b"]`
/// declares one table whose name contains a dot and `[a.b]` declares a nested
/// one. Joining them made a diagnostic about the first name the second.
fn declared_table(line: &str, preceding: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    if !trimmed.starts_with('[') {
        return None;
    }
    if !preceding.trim_end().is_empty() && preceding.parse::<toml::Table>().is_err() {
        return None;
    }
    let mut table = trimmed.parse::<toml::Table>().ok()?;
    let mut path = Vec::new();
    loop {
        // A header declares one table, so each level holds exactly one entry;
        // anything else is a line that merely parsed, not a header.
        if table.len() != 1 {
            return None;
        }
        let (key, value) = table.into_iter().next()?;
        path.push(key);
        table = match value {
            toml::Value::Table(inner) => inner,
            // `[[a.b]]` -- the array-of-tables spelling of the same name.
            toml::Value::Array(mut entries) if entries.len() == 1 => match entries.pop()? {
                toml::Value::Table(inner) => inner,
                _ => return None,
            },
            _ => return None,
        };
        if table.is_empty() {
            return Some(path);
        }
    }
}

/// Whether a header's key path is the table named by a dotted request.
///
/// Compared segment by segment, so a single quoted segment containing a dot is
/// never mistaken for the nested table of the same spelling.
fn path_is(path: &[String], requested: &str) -> bool {
    let mut segments = requested.split('.');
    path.iter().all(|segment| segments.next() == Some(segment)) && segments.next().is_none()
}

/// The lines under a table header, or `None` when the document has no such
/// table.
///
/// The header has to be a header: matched at the start of its own line, not
/// anywhere in the text. A plain substring search also found one inside a
/// commented-out example, and the body it then returned ended at the next real
/// header -- so a document whose actual defect was a misspelled key elsewhere
/// was reported as missing a key from a table the author had deliberately
/// commented out, with a correction naming something they never wrote.
fn table_body<'a>(text: &'a str, requested: &str) -> Option<&'a str> {
    let mut offset = 0_usize;
    let mut body_start = None;
    for line in text.split_inclusive('\n') {
        match declared_table(line, &text[..offset]) {
            Some(declared) if body_start.is_none() => {
                if path_is(&declared, requested) {
                    body_start = Some(offset + line.len());
                }
            }
            Some(_) => return Some(&text[body_start?..offset]),
            None => {}
        }
        offset += line.len();
    }
    body_start.map(|start| &text[start..])
}

fn authored_key<'a>(body: &'a str, requested: &str) -> Option<&'a str> {
    body.lines().find_map(|line| {
        let key = key_on_line(line)?;
        (key == requested).then_some(key)
    })
}

/// The table containing a byte offset, in segments, and the line at it.
fn authored_location(text: &str, offset: usize) -> (Vec<String>, &str) {
    let safe_offset = offset.min(text.len());
    let line_index = text[..safe_offset]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count();
    let lines: Vec<_> = text.lines().collect();
    let line = lines
        .get(line_index)
        .copied()
        .or_else(|| lines.last().copied())
        .unwrap_or("");

    // Through the same reader `table_body` uses, on the same text before it,
    // so the two agree on which lines are headers and on what each one names.
    let mut consumed = 0_usize;
    let table = text
        .split_inclusive('\n')
        .take(line_index.saturating_add(1))
        .filter_map(|candidate| {
            let declared = declared_table(candidate, &text[..consumed]);
            consumed += candidate.len();
            declared
        })
        .last()
        .unwrap_or_default();
    (table, line)
}

fn key_on_line(line: &str) -> Option<&str> {
    let raw = line.split_once('=')?.0.trim();
    // TOML permits a quoted key, and an author who writes `"mode" = "bearer"`
    // has written `mode`. Reading the quotes as part of the name made the
    // required-key check report a key missing that was present and correct,
    // sending the author to fix something they had already done while their
    // real mistake went unnamed.
    let key = raw
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            raw.strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(raw);
    (!key.is_empty()
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'))
    .then_some(key)
}

#[cfg(test)]
mod diagnostic_scanner_tests {
    use super::{authored_location, declared_table, is_observability_table, path_is, table_body};

    fn declared(line: &str) -> Option<Vec<String>> {
        declared_table(line, "")
    }

    /// Every spelling of a table header the format allows names the same
    /// table, and nothing else is a header. Bracket matching answered this
    /// three different ways across three repairs -- refusing legal headers,
    /// truncating names at a quoted `]`, and reading array elements inside a
    /// value as new tables -- each of which sent an author to fix a table they
    /// had not written while their real mistake went unnamed.
    #[test]
    fn a_header_is_read_the_way_the_format_defines_it() {
        for spelling in [
            "[observability.otlp.auth]",
            "[ observability.otlp.auth ]",
            "[observability.otlp.auth]  # bearer only",
            "[observability.\"otlp\".auth]",
            "[[observability.otlp.auth]]",
            "  [observability.otlp.auth]",
        ] {
            let path = declared(spelling).unwrap_or_else(|| panic!("{spelling:?} is a header"));
            assert!(
                path_is(&path, "observability.otlp.auth"),
                "{spelling:?} names observability.otlp.auth, got {path:?}"
            );
        }

        for not_a_header in [
            "[1, 2]",
            "endpoints = [",
            "  \"https://example/v1\",",
            "]",
            "mode = \"bearer\"",
            "# [observability.otlp.auth]",
            "",
        ] {
            assert_eq!(
                declared(not_a_header),
                None,
                "{not_a_header:?} declares no table"
            );
        }
    }

    /// A rendered path is TOML an author can paste. It is produced in key
    /// position, because the same serializer asked for a value spells a
    /// newline-bearing name as a multi-line string -- which TOML does not
    /// accept as a key, so the correction failed to parse in exactly the case
    /// the quoting exists to handle.
    #[test]
    fn a_rendered_path_parses_back_as_the_table_it_names() {
        for segment in ["plain", "a.b", "a\nb", "a\"b", "a\u{7f}b", "", "with space"] {
            let path = vec!["observability".to_owned(), segment.to_owned()];
            let rendered = super::render_path(&path);
            let document = format!("[{rendered}]\n");
            let parsed = super::declared_table(&document, "")
                .unwrap_or_else(|| panic!("{segment:?} renders a header TOML accepts: {document}"));
            assert_eq!(
                parsed, path,
                "{segment:?} round trips through its rendering"
            );
        }
    }

    /// A quoted segment is one name, whatever it contains. Joining the parsed
    /// path back into a string made `["observability.otlp"]` -- a single table
    /// whose name has a dot in it, and not the observability policy at all --
    /// indistinguishable from the nested table, so an unrelated error was
    /// reported as this subsystem's with a correction for keys that table
    /// never had.
    #[test]
    fn a_quoted_name_containing_a_dot_is_not_a_nested_table() {
        let literal = declared("[\"observability.otlp\"]").expect("it is a header");
        assert_eq!(literal.len(), 1, "one quoted segment is one name");
        assert!(!path_is(&literal, "observability.otlp"));
        assert!(
            !is_observability_table(&literal),
            "and it is not inside the observability policy"
        );

        let nested = declared("[observability.otlp]").expect("it is a header");
        assert!(path_is(&nested, "observability.otlp"));
        assert!(is_observability_table(&nested));

        assert_eq!(
            declared("[observability.fields.\"a]b\"]"),
            Some(vec![
                "observability".to_owned(),
                "fields".to_owned(),
                "a]b".to_owned(),
            ]),
            "and brackets inside a quoted name stay inside it"
        );
    }

    /// `["read"]` is both a legal header and a legal array element, so parsing
    /// the line alone cannot tell them apart. What separates them is that a
    /// header can only begin where the text before it is complete. Without
    /// that, an array element became a table and the diagnostic named a table
    /// nowhere in the author's file.
    #[test]
    fn an_array_element_on_its_own_line_is_not_a_header() {
        let document = "\
[observability.otlp.auth]
scopes = [
  [\"read\"]
]
token = \"\"
";
        let element = document
            .find("  [\"read\"]")
            .expect("the document contains the element");
        assert_eq!(
            declared_table("  [\"read\"]\n", &document[..element]),
            None,
            "the array above it is still open"
        );
        assert_eq!(
            declared_table("  [\"read\"]\n", ""),
            Some(vec!["read".to_owned()]),
            "the same text where a value is not open is a header"
        );

        let offset = document
            .find("token = \"\"")
            .expect("the document contains the offending key");
        let (table, line) = authored_location(document, offset);
        assert!(
            path_is(&table, "observability.otlp.auth"),
            "the error belongs to the table the author wrote, got {table:?}"
        );
        assert_eq!(line, "token = \"\"");

        let body =
            table_body(document, "observability.otlp.auth").expect("the table is in the document");
        assert!(
            body.contains("token = \"\""),
            "and its body is not truncated at the array element"
        );
    }

    /// The two readers agree by construction. They disagreed twice, and each
    /// time an error was attributed to a table the author never wrote.
    #[test]
    fn both_readers_agree_on_which_table_a_line_belongs_to() {
        let document = "\
[observability]
arena_bytes = 1024

[ observability.otlp.auth ]  # spaced and commented
token = \"\"
";
        let body = table_body(document, "observability.otlp.auth")
            .expect("a legal header spelling names its table");
        assert!(body.contains("token = \"\""));
        assert!(!body.contains("arena_bytes"));

        let offset = document
            .find("token = \"\"")
            .expect("the document contains the offending key");
        let (table, line) = authored_location(document, offset);
        assert!(path_is(&table, "observability.otlp.auth"));
        assert_eq!(line, "token = \"\"");
    }
}
