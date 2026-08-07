//! Transform node inputs, validation directives, and log directives.

use indexmap::IndexMap;
use serde::de;
use serde::{Deserialize, Deserializer, Serialize};

/// Input wiring for a transform — specifies which upstream transform(s) feed records.
///
/// String values become `Single`; arrays become `Multiple`.
/// Custom deserialization handles both forms.
#[derive(Debug, Clone, Serialize)]
pub enum TransformInput {
    /// Single upstream: `"categorize.high_value"` or `"transform_name"`.
    Single(String),
    /// Multiple upstreams (union): `["branch_a", "branch_b"]`.
    Multiple(Vec<String>),
}

impl<'de> Deserialize<'de> for TransformInput {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TransformInputVisitor;

        impl<'de> de::Visitor<'de> for TransformInputVisitor {
            type Value = TransformInput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or array of strings")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(TransformInput::Single(v.to_owned()))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element::<String>()? {
                    items.push(item);
                }
                if items.is_empty() {
                    return Err(de::Error::custom(
                        "transform input array must not be empty (use a single string for one upstream, or omit for default flow)",
                    ));
                }
                Ok(TransformInput::Multiple(items))
            }
        }

        deserializer.deserialize_any(TransformInputVisitor)
    }
}

/// A declarative validation attached to a transform.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationEntry {
    pub name: Option<String>,
    pub field: Option<String>,
    pub check: String,
    pub args: Option<IndexMap<String, serde_json::Value>>,
    #[serde(default = "default_severity")]
    pub severity: ValidationSeverity,
    pub message: Option<String>,
}

fn default_severity() -> ValidationSeverity {
    ValidationSeverity::Error
}

impl ValidationEntry {
    /// Auto-derive name from field and check if not specified.
    pub fn resolved_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| match &self.field {
            Some(f) => format!("{}:{}", f, self.check),
            None => self.check.clone(),
        })
    }
}

/// Validation severity: error routes to DLQ, warn logs and continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationSeverity {
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "warn")]
    Warn,
}

const MAX_LOG_SELECTOR_BYTES: usize = 128;
const MAX_LOG_MESSAGE_BYTES: usize = 1_024;
const MAX_LOG_FIELDS: usize = 256;
const MAX_LOG_DIRECTIVES_PER_TRANSFORM: usize = 32;
const MAX_LOG_SELECTED_FIELDS_PER_TRANSFORM: usize = 256;

/// A stable structured-event declaration attached to a transform.
///
/// The message is static author text. Record-derived values cross the
/// observability boundary only through the explicitly requested `fields`, and
/// deployment policy must separately authorize each exact event-field pair.
#[derive(Debug, Clone, Serialize)]
pub struct LogDirective {
    /// Stable event name matched by deployment field policy.
    pub name: String,
    pub level: LogLevel,
    pub when: LogTiming,
    pub message: String,
    pub fields: Option<Vec<String>>,
    pub every: Option<u64>,
}

impl<'de> Deserialize<'de> for LogDirective {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct AuthoredLogDirective {
            name: String,
            level: LogLevel,
            when: LogTiming,
            message: String,
            fields: Option<Vec<String>>,
            every: Option<u64>,
            #[serde(default)]
            log_rule: RetiredLogRule,
        }

        #[derive(Default)]
        struct RetiredLogRule(bool);

        impl<'de> Deserialize<'de> for RetiredLogRule {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let _ = de::IgnoredAny::deserialize(deserializer)?;
                Ok(Self(true))
            }
        }

        let authored = AuthoredLogDirective::deserialize(deserializer)?;
        if authored.log_rule.0 {
            return Err(de::Error::custom(
                "`log_rule` is retired; declare the event directly, for example `name: transform.customer_seen`, `message: customer processed`, and `fields: [customer_id]`",
            ));
        }
        let directive = Self {
            name: authored.name,
            level: authored.level,
            when: authored.when,
            message: authored.message,
            fields: authored.fields,
            every: authored.every,
        };
        if let Some(error) = directive.validation_errors().into_iter().next() {
            return Err(de::Error::custom(error));
        }
        Ok(directive)
    }
}

impl LogDirective {
    /// Return every authoring violation without applying a permissive fallback.
    pub(crate) fn validation_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if !valid_log_selector(&self.name) {
            errors.push(
                "`name` must be a bounded dotted identifier using ASCII letters, digits, or underscores (for example `name: transform.customer_seen`)"
                    .to_string(),
            );
        }
        if self.message.contains(['{', '}']) {
            errors.push(
                "`message` must be static text without interpolation; request record values separately with `fields: [customer_id]`"
                    .to_string(),
            );
        }
        if self.message.len() > MAX_LOG_MESSAGE_BYTES {
            errors.push(format!(
                "`message` must be at most {MAX_LOG_MESSAGE_BYTES} UTF-8 bytes so every admitted event is bounded"
            ));
        }
        match (self.when, self.every) {
            (LogTiming::PerRecord, None) => errors.push(
                "`when: per_record` requires explicit `every`, including `every: 1`".to_string(),
            ),
            (LogTiming::PerRecord, Some(0)) => errors.push(
                "`every` must be at least 1 for `when: per_record` (for every record, write `every: 1`)"
                    .to_string(),
            ),
            (LogTiming::PerRecord, Some(_)) | (_, None) => {}
            (_, Some(_)) => errors.push(
                "`every` is only valid with `when: per_record`; remove `every` for lifecycle events"
                    .to_string(),
            ),
        }
        if let Some(fields) = &self.fields {
            if matches!(
                self.when,
                LogTiming::BeforeTransform | LogTiming::AfterTransform
            ) {
                errors.push(
                    "`fields` is only valid with `when: per_record` or `when: on_error`; remove `fields` from lifecycle events"
                        .to_string(),
                );
            }
            if fields.is_empty() {
                errors.push(
                    "`fields` must not be empty; omit it when no record values are requested"
                        .to_string(),
                );
            }
            if fields.len() > MAX_LOG_FIELDS {
                errors.push(format!(
                    "`fields` may request at most {MAX_LOG_FIELDS} structured attributes"
                ));
            }
            let mut seen = std::collections::BTreeSet::new();
            for field in fields {
                if !valid_log_selector(field) {
                    errors.push(format!(
                        "requested field `{field}` must be a bounded dotted identifier"
                    ));
                } else if !seen.insert(field.as_str()) {
                    errors.push(format!(
                        "requested field `{field}` appears more than once; every event attribute key must be unique"
                    ));
                }
            }
        }
        errors
    }
}

/// Return every violation across one transform's complete directive set.
///
/// This is the single admission authority for both per-directive grammar and
/// transform-wide work limits. Callers that hold typed config must use this
/// validator too: [`LogDirective`] and [`TransformBody`](super::TransformBody)
/// remain publicly constructible and mutable after deserialization.
pub(crate) fn log_directive_set_validation_errors(directives: &[LogDirective]) -> Vec<String> {
    let mut errors = Vec::new();
    if directives.len() > MAX_LOG_DIRECTIVES_PER_TRANSFORM {
        errors.push(format!(
            "`log` may declare at most {MAX_LOG_DIRECTIVES_PER_TRANSFORM} events per transform so per-record dispatch work stays bounded"
        ));
    }
    let selected_fields = directives
        .iter()
        .map(|directive| directive.fields.as_ref().map_or(0, Vec::len))
        .try_fold(0usize, usize::checked_add)
        .unwrap_or(usize::MAX);
    if selected_fields > MAX_LOG_SELECTED_FIELDS_PER_TRANSFORM {
        errors.push(format!(
            "`log` may request at most {MAX_LOG_SELECTED_FIELDS_PER_TRANSFORM} fields in aggregate across one transform"
        ));
    }
    for (index, directive) in directives.iter().enumerate() {
        errors.extend(
            directive
                .validation_errors()
                .into_iter()
                .map(|error| format!("log directive #{}: {error}", index + 1)),
        );
    }
    errors
}

/// Deserialize one transform's complete directive list through the same
/// validator used by typed validation and compilation.
pub(crate) fn deserialize_log_directives<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<LogDirective>>, D::Error>
where
    D: Deserializer<'de>,
{
    let directives = Option::<Vec<LogDirective>>::deserialize(deserializer)?;
    let Some(directives) = directives else {
        return Ok(None);
    };
    if let Some(error) = log_directive_set_validation_errors(&directives)
        .into_iter()
        .next()
    {
        return Err(de::Error::custom(error));
    }
    Ok(Some(directives))
}

/// Return whether an observability event or field selector uses the canonical
/// bounded dotted-identifier grammar used by deployment field policy.
fn valid_log_selector(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_LOG_SELECTOR_BYTES
        && value.split('.').all(|segment| {
            let mut bytes = segment.bytes();
            matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic() || first == b'_')
                && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
}

/// When a log directive fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogTiming {
    #[serde(rename = "before_transform")]
    BeforeTransform,
    #[serde(rename = "after_transform")]
    AfterTransform,
    #[serde(rename = "per_record")]
    PerRecord,
    #[serde(rename = "on_error")]
    OnError,
}

/// Log level for directives (YAML config domain).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    #[serde(rename = "trace")]
    Trace,
    #[serde(rename = "debug")]
    Debug,
    #[serde(rename = "info")]
    Info,
    #[serde(rename = "warn")]
    Warn,
    #[serde(rename = "error")]
    Error,
}

/// Lightweight read-only view over a transform-like node
/// (`Transform`, `Aggregate`, `Route`) yielded by
/// [`PipelineConfig::transform_views`]. Carries the fields external tooling
/// and schema-validation passes need; callers that need variant-specific
/// bodies (`TransformBody`, `AggregateBody`, etc.) should walk
/// [`PipelineConfig::nodes`] directly.
#[derive(Debug, Clone, Copy)]
pub struct TransformView<'a> {
    pub name: &'a str,
    pub description: Option<&'a str>,
    pub cxl_source: &'a str,
    pub notes: Option<&'a serde_json::Value>,
    pub kind: TransformViewKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformViewKind {
    Transform,
    Aggregate,
    Route,
}

impl<'a> TransformView<'a> {
    pub fn cxl_source(&self) -> &'a str {
        self.cxl_source
    }
}
