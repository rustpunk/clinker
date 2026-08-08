//! Stable, serialization-neutral failure classifications.
//!
//! This module owns the small machine-facing vocabulary shared by network and
//! lineage failure producers. It deliberately does not serialize the vocabulary:
//! edge crates choose their own wire representation from the stable accessors.

/// Maximum UTF-8 byte length of a sanitized failure message.
const MAX_MESSAGE_BYTES: usize = 240;

/// Broad failure category exposed to machine and observability consumers.
///
/// The variants and their [`FailureCategory::as_str`] spellings are stable.
/// They describe policy at a coarse boundary; subsystem-specific detail stays
/// in the registered failure code.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum FailureCategory {
    /// A security boundary rejected an operation.
    SecurityPolicy,
    /// A finite source produced malformed or unsupported protocol data.
    SourceProtocol,
    /// Runtime state contradicted a validated or compiled invariant.
    InternalInvariant,
    /// Admission or deployment configuration is invalid or incomplete.
    Configuration,
    /// A runtime resource or service failed outside semantic processing.
    Infrastructure,
    /// Attempt finalization or artifact publication failed.
    Publication,
    /// Optional observability configuration or delivery failed.
    Observability,
}

impl FailureCategory {
    /// Return the stable machine spelling for this category.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SecurityPolicy => "security_policy",
            Self::SourceProtocol => "source_protocol",
            Self::InternalInvariant => "internal_invariant",
            Self::Configuration => "configuration",
            Self::Infrastructure => "infrastructure",
            Self::Publication => "publication",
            Self::Observability => "observability",
        }
    }
}

/// Explicit retry guidance for an external supervisor.
///
/// This is intentionally an exact tri-state vocabulary. A caller must choose
/// policy from the underlying registered failure family, never from an exit
/// status or by parsing a rendered message.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum RetryAdvice {
    /// Repeating the same admitted operation cannot correct the failure.
    DoNotRetry,
    /// A transient external condition may clear after bounded backoff.
    RetryWithBackoff,
    /// Deployment or operator policy must decide whether another attempt is safe.
    PolicyRequired,
}

impl RetryAdvice {
    /// Return the stable machine spelling for this retry decision.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DoNotRetry => "do_not_retry",
            Self::RetryWithBackoff => "retry_with_backoff",
            Self::PolicyRequired => "policy_required",
        }
    }
}

/// A registered, sanitized failure suitable for machine and observability edges.
///
/// Fields are private so every value passes through registry lookup and the
/// message sanitizer. Cancellation and completed-with-DLQ are terminal outcomes
/// and intentionally have no representation in this type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureClassification {
    code: &'static str,
    category: FailureCategory,
    message: String,
    retry_advice: RetryAdvice,
}

impl FailureClassification {
    /// Public bound used by edge serializers before they allocate a payload.
    pub const MAX_MESSAGE_BYTES: usize = MAX_MESSAGE_BYTES;

    /// Construct a classification from a registered code and untrusted detail.
    ///
    /// Returns `None` for an unregistered code. Sensitive, path-bearing,
    /// record-bearing, raw-debug, or empty detail is replaced by the registry's
    /// fixed safe message. Other detail is whitespace-normalized and bounded.
    pub fn new(code: &str, message: impl AsRef<str>) -> Option<Self> {
        let entry = registry_entry(code)?;
        Some(Self::from_entry(
            entry,
            sanitize_message(message.as_ref(), entry.default_message),
        ))
    }

    /// Construct a classification with the registry's fixed safe message.
    pub fn for_code(code: &str) -> Option<Self> {
        let entry = registry_entry(code)?;
        Some(Self::from_entry(entry, entry.default_message.to_owned()))
    }

    /// Map an otherwise unknown internal failure to the single safe invariant code.
    ///
    /// The detail is passed through the same sanitizer as [`Self::new`]. Raw
    /// debug payloads and sensitive context therefore collapse to the fixed
    /// invariant message instead of entering a machine event.
    pub fn unknown_internal(message: impl AsRef<str>) -> Self {
        Self::new("runtime.invariant.unknown", message)
            .expect("the append-only registry contains runtime.invariant.unknown")
    }

    /// Iterate over every registered stable code in declaration order.
    pub fn registered_codes() -> impl ExactSizeIterator<Item = &'static str> {
        REGISTRY.iter().map(|entry| entry.code)
    }

    /// Return the stable namespaced failure code.
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// Return the registered broad category.
    pub const fn category(&self) -> FailureCategory {
        self.category
    }

    /// Return the bounded sanitized message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Return the registered retry decision.
    pub const fn retry_advice(&self) -> RetryAdvice {
        self.retry_advice
    }

    fn from_entry(entry: &RegistryEntry, message: String) -> Self {
        Self {
            code: entry.code,
            category: entry.category,
            message,
            retry_advice: entry.retry_advice,
        }
    }
}

#[derive(Copy, Clone)]
struct RegistryEntry {
    code: &'static str,
    category: FailureCategory,
    retry_advice: RetryAdvice,
    default_message: &'static str,
}

macro_rules! failure_registry {
    ($($code:literal, $category:ident, $retry:ident, $message:literal;)+) => {
        const REGISTRY: &[RegistryEntry] = &[
            $(RegistryEntry {
                code: $code,
                category: FailureCategory::$category,
                retry_advice: RetryAdvice::$retry,
                default_message: $message,
            },)+
        ];
    };
}

// Registry rows and code meanings are append-only. Retired codes remain
// reserved and must never be reassigned to another condition.
failure_registry! {
    "rest.security.cross_origin", SecurityPolicy, DoNotRetry, "REST continuation violates same-origin policy";
    "rest.security.https_downgrade", SecurityPolicy, DoNotRetry, "REST continuation would downgrade transport security";
    "rest.protocol.malformed_continuation", SourceProtocol, PolicyRequired, "REST continuation metadata is malformed";
    "rest.protocol.unresolvable_continuation", SourceProtocol, PolicyRequired, "REST continuation target cannot be resolved";
    "rest.protocol.unsupported_continuation", SourceProtocol, PolicyRequired, "REST continuation form is unsupported";
    "rest.protocol.conflicting_continuation", SourceProtocol, PolicyRequired, "REST continuation targets conflict";
    "rest.protocol.page_limit_reached", SourceProtocol, PolicyRequired, "REST source reached its page limit before a terminal page";
    "runtime.invariant.plan_mismatch", InternalInvariant, PolicyRequired, "compiled plan and runtime state disagree";
    "runtime.invariant.dispatch_mismatch", InternalInvariant, PolicyRequired, "runtime dispatch invariant failed";
    "runtime.invariant.poisoned_state", InternalInvariant, PolicyRequired, "shared runtime state is unavailable";
    "runtime.invariant.unknown", InternalInvariant, PolicyRequired, "internal execution invariant failed";
    "admission.configuration.invalid", Configuration, DoNotRetry, "run configuration is invalid";
    "admission.configuration.stdout_conflict", Configuration, DoNotRetry, "machine output conflicts with another stdout mode";
    "admission.configuration.batch_id_missing", Configuration, DoNotRetry, "machine mode requires a batch identifier";
    "admission.configuration.policy_required", Configuration, PolicyRequired, "deployment policy is required before execution";
    "infrastructure.runtime.transient", Infrastructure, RetryWithBackoff, "temporary runtime infrastructure failure";
    "infrastructure.runtime.source_unavailable", Infrastructure, RetryWithBackoff, "source infrastructure is temporarily unavailable";
    "attempt.publication.registration_failed", Publication, PolicyRequired, "attempt artifact registration failed";
    "attempt.publication.finalization_failed", Publication, RetryWithBackoff, "attempt artifact finalization failed";
    "attempt.publication.manifest_failed", Publication, RetryWithBackoff, "attempt manifest persistence failed";
    "attempt.publication.promotion_failed", Publication, PolicyRequired, "artifact promotion failed";
    "attempt.retention.ownership_refused", SecurityPolicy, PolicyRequired, "attempt ownership could not be proven";
    "attempt.retention.manifest_invalid", SecurityPolicy, PolicyRequired, "attempt manifest is invalid or unreadable";
    "attempt.retention.live", Publication, PolicyRequired, "attempt is still live";
    "attempt.retention.clock_ambiguous", Publication, PolicyRequired, "attempt retention time is ambiguous";
    "attempt.retention.budget_exhausted", Publication, RetryWithBackoff, "attempt cleanup budget was exhausted";
    "attempt.retention.cleanup_failed", Infrastructure, RetryWithBackoff, "attempt cleanup did not complete";
    "observability.configuration.invalid", Observability, DoNotRetry, "observability configuration is invalid";
    "observability.configuration.policy_required", Observability, PolicyRequired, "observability policy is required";
    "observability.delivery.failed", Observability, RetryWithBackoff, "observability delivery failed";
    "observability.delivery.rejected", Observability, PolicyRequired, "observability delivery was rejected";
    "source.data.invalid", SourceProtocol, DoNotRetry, "source data does not satisfy the admitted plan";
    "rest.http.client_error", SourceProtocol, DoNotRetry, "REST source request was rejected";
    "source.endpoint.untrusted_tls", SourceProtocol, PolicyRequired, "source endpoint did not present a trusted TLS identity";
    "source.endpoint.unresolvable", SourceProtocol, PolicyRequired, "source endpoint host could not be resolved";
    "source.endpoint.unreadable_material", SourceProtocol, PolicyRequired, "source endpoint material could not be read from local storage";
    "runtime.resource.memory_budget_exceeded", Infrastructure, PolicyRequired, "runtime memory budget was exceeded";
    "admission.configuration.memory_budget_unsatisfiable", Configuration, DoNotRetry, "configured memory budget is below the runtime baseline";
    "runtime.resource.spill_failed", Infrastructure, RetryWithBackoff, "runtime spill storage failed";
    "runtime.resource.spill_cap_exceeded", Infrastructure, PolicyRequired, "configured spill budget was exceeded";
    "rest.protocol.page_body_limit_reached", SourceProtocol, PolicyRequired, "REST response exceeded the fixed page body limit";
}

fn registry_entry(code: &str) -> Option<&'static RegistryEntry> {
    REGISTRY.iter().find(|entry| entry.code == code)
}

fn sanitize_message(candidate: &str, fallback: &'static str) -> String {
    if contains_sensitive_shape(candidate) {
        return fallback.to_owned();
    }

    let normalized = candidate.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return fallback.to_owned();
    }
    truncate_utf8(&normalized, MAX_MESSAGE_BYTES)
}

fn contains_sensitive_shape(candidate: &str) -> bool {
    let lower = candidate.to_ascii_lowercase();
    let sensitive_labels = [
        "authorization",
        "proxy-authorization",
        "bearer ",
        "basic ",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "secret",
        "token=",
        "credential",
        "cookie:",
        "set-cookie",
        "record=",
        "record:",
        "row=",
        "payload=",
    ];
    if sensitive_labels.iter().any(|label| lower.contains(label)) {
        return true;
    }

    if lower.contains('{')
        || lower.contains('}')
        || lower.contains("some(")
        || lower.contains("none(")
        || lower.contains("err(")
        || lower.contains("ok(")
    {
        return true;
    }

    if lower.contains("://") {
        let url_tail = lower.split_once("://").map_or("", |(_, tail)| tail);
        let authority = url_tail.split('/').next().unwrap_or(url_tail);
        if authority.contains('@') || url_tail.contains('?') || url_tail.contains('#') {
            return true;
        }
    }

    candidate.split_whitespace().any(|word| {
        let trimmed = word.trim_matches(|character: char| {
            matches!(
                character,
                '(' | ')' | '[' | ']' | ',' | ';' | ':' | '\'' | '"'
            )
        });
        trimmed.starts_with('/')
            || trimmed.starts_with("\\\\")
            || (trimmed.len() >= 3
                && trimmed.as_bytes()[1] == b':'
                && matches!(trimmed.as_bytes()[2], b'\\' | b'/'))
    })
}

fn truncate_utf8(message: &str, max_bytes: usize) -> String {
    if message.len() <= max_bytes {
        return message.to_owned();
    }

    const ELLIPSIS: &str = "...";
    let content_limit = max_bytes.saturating_sub(ELLIPSIS.len());
    let mut end = 0;
    for (index, character) in message.char_indices() {
        let next = index + character.len_utf8();
        if next > content_limit {
            break;
        }
        end = next;
    }
    let mut bounded = message[..end].trim_end().to_owned();
    bounded.push_str(ELLIPSIS);
    bounded
}
