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

const DEFAULT_ARENA_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_ORDINARY_LANE_BYTES: u64 = 3 * 1024 * 1024;
const DEFAULT_HIGH_SEVERITY_LANE_BYTES: u64 = 1024 * 1024;
const DEFAULT_MAX_BATCH_BYTES: u64 = 256 * 1024;
const DEFAULT_MAX_ATTRIBUTES_PER_EVENT: u32 = 32;
const DEFAULT_MAX_ATTRIBUTE_BYTES: u64 = 4 * 1024;
const DEFAULT_SAMPLE_EVERY: u32 = 1;
const DEFAULT_RATE_LIMIT_PER_SECOND: u32 = 1_000;
const DEFAULT_RATE_LIMIT_BURST: u32 = 1_000;
const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 1_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_RETRY_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_TOTAL_TIMEOUT_MS: u64 = 10_000;
const RETRY_INITIAL_BACKOFF_MS: u64 = 100;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 64 * 1024;
const DEFAULT_LINEAGE_QUEUE_BYTES: u64 = 1024 * 1024;
const DEFAULT_LINEAGE_MAX_EVENT_BYTES: u64 = 64 * 1024;
const DEFAULT_LINEAGE_FLUSH_TIMEOUT_MS: u64 = 5_000;

const MAX_ENDPOINT_BYTES: usize = 2_048;
const MAX_AUTH_REFERENCE_BYTES: usize = 256;
const MAX_SELECTOR_BYTES: usize = 128;
const MAX_DATASET_IDENTITY_BYTES: usize = 1_024;
const MAX_REPLACEMENT_BYTES: usize = 1_024;
const MAX_FIELD_POLICIES: usize = 256;
const MAX_DATASET_BINDINGS: usize = 1_024;
const MAX_ARENA_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BATCH_BYTES: u64 = 1024 * 1024;
const MAX_ATTRIBUTES_PER_EVENT: u32 = 256;
const MAX_ATTRIBUTE_BYTES: u64 = 64 * 1024;
const MAX_SAMPLE_EVERY: u32 = 1_000_000;
const MAX_RATE_LIMIT: u32 = 1_000_000;
const MAX_TIMEOUT_MS: u64 = 60_000;
const MAX_RETRY_ATTEMPTS: u32 = 10;
const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;
const MAX_LINEAGE_QUEUE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_LINEAGE_EVENT_BYTES: u64 = 1024 * 1024;

fn default_arena_bytes() -> ByteSize {
    ByteSize(DEFAULT_ARENA_BYTES)
}

fn default_ordinary_lane_bytes() -> ByteSize {
    ByteSize(DEFAULT_ORDINARY_LANE_BYTES)
}

fn default_high_severity_lane_bytes() -> ByteSize {
    ByteSize(DEFAULT_HIGH_SEVERITY_LANE_BYTES)
}

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
    #[serde(default = "default_arena_bytes")]
    arena_bytes: ByteSize,
    #[serde(default = "default_ordinary_lane_bytes")]
    ordinary_lane_bytes: ByteSize,
    #[serde(default = "default_high_severity_lane_bytes")]
    high_severity_lane_bytes: ByteSize,
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
    otlp: OtlpConfig,
    lineage: LineageConfig,
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
pub enum ObservabilityDropPolicy {
    #[default]
    #[serde(rename = "drop-newest")]
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
        let field = match (table.as_str(), key) {
            ("observability.otlp.auth", Some(key)) => {
                format!("observability.otlp.auth.{key}")
            }
            ("observability.lineage.dataset", Some(key)) => {
                format!("observability.lineage.dataset.{key}")
            }
            ("observability.field_policy", Some(key)) => {
                format!("observability.field_policy.{key}")
            }
            (table, Some(key)) if table.starts_with("observability") => {
                format!("{table}.{key}")
            }
            (table, None) if table.starts_with("observability") => table.to_owned(),
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
    let offset = error.span().map_or(text.len(), |span| span.start);
    let (table, _) = authored_location(text, offset);
    if table.starts_with("observability") {
        return true;
    }

    let has_observability = text.lines().any(|line| line.trim() == "[observability]");
    has_observability
        && (!text.contains("[observability.otlp]")
            || !text.contains("[observability.otlp.auth]")
            || !text.contains("[observability.lineage]"))
}

impl ObservabilityConfig {
    pub(crate) fn resolve(&self) -> Result<ResolvedObservabilityPolicy, ObservabilityConfigError> {
        let arena_bytes = bounded_nonzero_u64(
            "observability.arena_bytes",
            self.arena_bytes.0,
            MAX_ARENA_BYTES,
            "set `arena_bytes = \"4MB\"`",
        )?;
        let ordinary_lane_bytes = bounded_nonzero_u64(
            "observability.ordinary_lane_bytes",
            self.ordinary_lane_bytes.0,
            MAX_ARENA_BYTES,
            "set `ordinary_lane_bytes = \"3MB\"`",
        )?;
        let high_severity_lane_bytes = bounded_nonzero_u64(
            "observability.high_severity_lane_bytes",
            self.high_severity_lane_bytes.0,
            MAX_ARENA_BYTES,
            "set `high_severity_lane_bytes = \"1MB\"`",
        )?;
        let lane_sum = ordinary_lane_bytes
            .checked_add(high_severity_lane_bytes)
            .ok_or_else(|| {
                ObservabilityConfigError::invalid(
                    "observability.arena_bytes",
                    "cannot represent the sum of the two telemetry lanes",
                    "set the two lane byte caps so their exact sum equals `arena_bytes`",
                )
            })?;
        if lane_sum != arena_bytes {
            return Err(ObservabilityConfigError::invalid(
                "observability.arena_bytes",
                "must equal the exact sum of `ordinary_lane_bytes` and `high_severity_lane_bytes`",
                "set `arena_bytes` to the exact sum of the two disjoint lane caps",
            ));
        }

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

        let otlp = self.otlp.resolve(flush_timeout_ms)?;
        let lineage = self.lineage.resolve()?;
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
            otlp: Some(otlp),
            lineage: Some(lineage),
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
                "set `drop_policy = \"drop-newest\"`".to_owned()
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

fn table_body<'a>(text: &'a str, requested: &str) -> Option<&'a str> {
    let header = format!("[{requested}]");
    let start = text.find(&header)? + header.len();
    let tail = &text[start..];
    let end = tail.find("\n[").map_or(tail.len(), |relative| relative + 1);
    Some(&tail[..end])
}

fn authored_key<'a>(body: &'a str, requested: &str) -> Option<&'a str> {
    body.lines().find_map(|line| {
        let key = key_on_line(line)?;
        (key == requested).then_some(key)
    })
}

fn authored_location(text: &str, offset: usize) -> (String, &str) {
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

    let mut table = String::new();
    for candidate in lines.iter().take(line_index.saturating_add(1)) {
        let trimmed = candidate.trim();
        if let Some(inner) = trimmed
            .strip_prefix("[[")
            .and_then(|value| value.strip_suffix("]]"))
        {
            table = inner.to_owned();
        } else if let Some(inner) = trimmed
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
        {
            table = inner.to_owned();
        }
    }
    (table, line)
}

fn key_on_line(line: &str) -> Option<&str> {
    let key = line.split_once('=')?.0.trim();
    (!key.is_empty()
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'))
    .then_some(key)
}
