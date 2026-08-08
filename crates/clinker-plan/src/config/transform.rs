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
const MAX_LOG_CONDITION_BYTES: usize = 512;
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
    /// CXL boolean expression gating a `per_record` event, evaluated against
    /// the transform's *input* record — dispatch fires before the transform's
    /// own program runs, so the input row is the only one in scope.
    ///
    /// This reads record values but never exports them: the only thing it can
    /// change is whether the event fires. The values that actually cross the
    /// observability boundary remain exactly the `fields` list, each still
    /// default-denied until deployment policy authorizes that event-field pair.
    /// Narrowing a condition therefore can never widen exposure.
    pub condition: Option<crate::yaml::CxlSource>,
}

/// Every key a log directive accepts, in the order the reference table
/// documents them.
///
/// Retired spellings are deliberately absent. This list is quoted verbatim by
/// the unknown-key diagnostic, and a suggestion an author cannot act on is
/// worse than no suggestion: naming a key that is itself unconditionally
/// rejected sends them to a second dead end.
const LOG_DIRECTIVE_KEYS: &[&str] = &[
    "name",
    "level",
    "when",
    "message",
    "fields",
    "every",
    "condition",
];

/// A directive that exercises every accepted key, offered as the pasteable
/// corrected form whenever admission rejects an authored key.
const LOG_DIRECTIVE_EXAMPLE: &str = "- { name: transform.customer_seen, level: info, when: per_record, every: 1, message: customer processed, fields: [customer_id] }";

/// Reject a key the directive grammar does not accept, naming the offending
/// key, the accepted set, and a directive the author can paste.
fn unknown_log_directive_key(key: &str) -> String {
    let accepted = LOG_DIRECTIVE_KEYS
        .iter()
        .map(|accepted| format!("`{accepted}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "unknown log directive key `{key}`; a log directive accepts only {accepted} — for example `{LOG_DIRECTIVE_EXAMPLE}`"
    )
}

/// Bind an authored key exactly once, so a repeated key is rejected rather
/// than silently taking the last value.
fn bind_once<T, E: de::Error>(slot: &mut Option<T>, key: &'static str, value: T) -> Result<(), E> {
    if slot.is_some() {
        return Err(de::Error::duplicate_field(key));
    }
    *slot = Some(value);
    Ok(())
}

/// Hand-written to control the rejection text, following the `visit_map`
/// pattern [`PipelineNode`](super::PipelineNode) uses for the same reason. A
/// derived `deny_unknown_fields` builds its "expected one of" list from the
/// struct's declared fields, which forces every retired key the impl must
/// still recognize to be advertised as if it were usable.
impl<'de> Deserialize<'de> for LogDirective {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LogDirectiveVisitor;

        impl<'de> de::Visitor<'de> for LogDirectiveVisitor {
            type Value = LogDirective;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter
                    .write_str("a log directive mapping with `name`, `level`, `when`, `message`")
            }

            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut name = None;
                let mut level = None;
                let mut when = None;
                let mut message = None;
                let mut fields = None;
                let mut every = None;
                let mut condition = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "name" => bind_once(&mut name, "name", map.next_value()?)?,
                        "level" => bind_once(&mut level, "level", map.next_value()?)?,
                        "when" => bind_once(&mut when, "when", map.next_value()?)?,
                        "message" => bind_once(&mut message, "message", map.next_value()?)?,
                        "fields" => bind_once(&mut fields, "fields", map.next_value()?)?,
                        "every" => bind_once(&mut every, "every", map.next_value()?)?,
                        "condition" => bind_once(&mut condition, "condition", map.next_value()?)?,
                        "log_rule" => {
                            map.next_value::<de::IgnoredAny>()?;
                            return Err(de::Error::custom(
                                "`log_rule` is retired; declare the event directly, for example `name: transform.customer_seen`, `message: customer processed`, and `fields: [customer_id]`",
                            ));
                        }
                        other => {
                            return Err(de::Error::custom(unknown_log_directive_key(other)));
                        }
                    }
                }

                let directive = LogDirective {
                    name: name.ok_or_else(|| de::Error::missing_field("name"))?,
                    level: level.ok_or_else(|| de::Error::missing_field("level"))?,
                    when: when.ok_or_else(|| de::Error::missing_field("when"))?,
                    message: message.ok_or_else(|| de::Error::missing_field("message"))?,
                    fields,
                    every,
                    condition,
                };
                if let Some(error) = directive.validation_errors().into_iter().next() {
                    return Err(de::Error::custom(error));
                }
                Ok(directive)
            }
        }

        deserializer.deserialize_map(LogDirectiveVisitor)
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
        if let Some(condition) = &self.condition {
            // A gate needs a record to test. Lifecycle events carry none, and
            // `on_error` reports a failure the author already selected by
            // routing it — narrowing that further would silently hide errors.
            if self.when != LogTiming::PerRecord {
                errors.push(
                    "`condition` is only valid with `when: per_record`; remove `condition`, or change this event to `when: per_record` with an explicit `every`"
                        .to_string(),
                );
            }
            if condition.source.trim().is_empty() {
                errors.push(
                    "`condition` must be a CXL boolean expression (for example `condition: \"amount > 1000\"`); omit it to log every record"
                        .to_string(),
                );
            }
            if condition.source.len() > MAX_LOG_CONDITION_BYTES {
                errors.push(format!(
                    "`condition` must be at most {MAX_LOG_CONDITION_BYTES} UTF-8 bytes so per-record gate evaluation stays bounded"
                ));
            }
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
