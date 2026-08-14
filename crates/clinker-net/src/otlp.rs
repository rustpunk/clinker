//! Bounded synchronous OTLP/HTTP JSON delivery.
//!
//! Raw deployment text crosses [`admit_otlp_endpoint`] exactly once. The
//! resulting endpoint has private representation and can only derive the three
//! fixed OTLP signal routes. Delivery remains a finite blocking operation over
//! the crate's `ureq`/rustls stack, and lives in [`delivery`] behind the
//! `transport` feature; what an endpoint is admissible for does not, so the
//! policy holds in a build with no HTTP client compiled in at all.

// The path is spelled out because `tests/otlp_http.rs` includes this file with
// `#[path]` to reach the loopback constructor, and a child module of a
// path-included file resolves against `src/` rather than against this file's
// own directory. Naming it relative to `src/` is what both spellings agree on.
#[cfg(feature = "transport")]
#[path = "otlp/delivery.rs"]
mod delivery;

use std::fmt;
#[cfg(test)]
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use clinker_core_types::FailureClassification;
use http::Uri;
use http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, EXPECT, HOST, HeaderName, HeaderValue,
    PROXY_AUTHORIZATION, TRANSFER_ENCODING,
};
use http::uri::{Authority, Scheme};

#[cfg(feature = "transport")]
pub use delivery::send_otlp_json;

const ENDPOINT_GUIDANCE: &str = "observability.otlp.endpoint must be a credential-free HTTPS origin; use https://collector.example.com";

/// A normalized OTLP collector origin admitted for fixed-route delivery.
///
/// The representation is private. Production code can obtain a value only
/// through [`admit_otlp_endpoint`].
#[derive(Clone, Eq, PartialEq)]
pub struct AdmittedOtlpEndpoint {
    origin: Uri,
    https_only: bool,
}

impl fmt::Debug for AdmittedOtlpEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmittedOtlpEndpoint")
            .field("origin", &"[admitted]")
            .finish()
    }
}

/// Sanitized failure returned when deployment endpoint text is not admissible.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OtlpEndpointAdmissionError {
    classification: FailureClassification,
}

impl OtlpEndpointAdmissionError {
    /// API classification: workspace-internal exposed API.
    ///
    /// Return the stable shared failure classification.
    pub const fn classification(&self) -> &FailureClassification {
        &self.classification
    }

    fn invalid() -> Self {
        Self {
            classification: FailureClassification::for_code("observability.configuration.invalid")
                .expect("the append-only registry contains the observability configuration code"),
        }
    }
}

impl fmt::Display for OtlpEndpointAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(ENDPOINT_GUIDANCE)
    }
}

impl std::error::Error for OtlpEndpointAdmissionError {}

/// Parse and admit one absolute credential-free HTTPS OTLP origin.
///
/// The rejected input is never retained or rendered. Paths other than `/`, a
/// query, a fragment, user information, non-HTTPS schemes, and relative forms
/// all return the same sanitized correction.
pub fn admit_otlp_endpoint(raw: &str) -> Result<AdmittedOtlpEndpoint, OtlpEndpointAdmissionError> {
    let parsed = raw
        .parse::<Uri>()
        .map_err(|_| OtlpEndpointAdmissionError::invalid())?;
    // `http::Uri` intentionally omits a URI fragment from its representation.
    // Require a lossless parser round-trip so discarded syntax cannot cross
    // admission. Case is ignored here because scheme and host case do not
    // change the represented origin.
    let parsed_text = parsed.to_string();
    let implicit_root = format!("{raw}/");
    if !raw.eq_ignore_ascii_case(&parsed_text) && !implicit_root.eq_ignore_ascii_case(&parsed_text)
    {
        return Err(OtlpEndpointAdmissionError::invalid());
    }
    let scheme = parsed
        .scheme()
        .filter(|scheme| **scheme == Scheme::HTTPS)
        .ok_or_else(OtlpEndpointAdmissionError::invalid)?;
    let authority = parsed
        .authority()
        .ok_or_else(OtlpEndpointAdmissionError::invalid)?;
    if authority.host().is_empty() || parsed.path() != "/" || parsed.query().is_some() {
        return Err(OtlpEndpointAdmissionError::invalid());
    }

    // Rebuild the authority from the parser's host/port components. A parsed
    // authority containing user information or a non-numeric port cannot
    // round-trip through this host/optional-port-only representation.
    let round_trip_authority = authority_from_parts(authority, false)?;
    let round_trip = Uri::builder()
        .scheme(scheme.clone())
        .authority(round_trip_authority)
        .path_and_query("/")
        .build()
        .map_err(|_| OtlpEndpointAdmissionError::invalid())?;
    let rebuilt = round_trip
        .authority()
        .ok_or_else(OtlpEndpointAdmissionError::invalid)?;
    if !authority.as_str().eq_ignore_ascii_case(rebuilt.as_str()) {
        return Err(OtlpEndpointAdmissionError::invalid());
    }

    let normalized_authority = authority_from_parts(authority, true)?;
    let origin = Uri::builder()
        .scheme(Scheme::HTTPS)
        .authority(normalized_authority)
        .path_and_query("/")
        .build()
        .map_err(|_| OtlpEndpointAdmissionError::invalid())?;
    Ok(AdmittedOtlpEndpoint {
        origin,
        https_only: true,
    })
}

fn authority_from_parts(
    authority: &Authority,
    normalize: bool,
) -> Result<Authority, OtlpEndpointAdmissionError> {
    let host = if normalize {
        authority.host().to_ascii_lowercase()
    } else {
        authority.host().to_owned()
    };
    let authority_text = match authority.port_u16() {
        Some(443) if normalize => host,
        Some(port) => format!("{host}:{port}"),
        None => host,
    };
    Uri::builder()
        .authority(authority_text)
        .build()
        .map_err(|_| OtlpEndpointAdmissionError::invalid())?
        .authority()
        .cloned()
        .ok_or_else(OtlpEndpointAdmissionError::invalid)
}

/// The closed set of OTLP/HTTP JSON signals supported by this transport.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OtlpSignal {
    /// Log records delivered to `/v1/logs`.
    Logs,
    /// Metric data points delivered to `/v1/metrics`.
    Metrics,
    /// Trace spans delivered to `/v1/traces`.
    Traces,
}

/// Explicit finite limits for one signal delivery call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OtlpDeliveryBudget {
    max_request_bytes: usize,
    max_response_bytes: u64,
    max_attempts: u32,
    connect_timeout: Duration,
    request_timeout: Duration,
    retry_backoff: Duration,
    total_timeout: Duration,
}

/// The authored bounds of one signal delivery, before validation.
///
/// Named rather than positional because four of the seven are a `Duration`:
/// transposing a pair of them compiles cleanly and silently changes which
/// deadline the transport enforces, which is the kind of mistake that only
/// shows up as a misclassified failure against a real collector.
///
/// Construct an [`OtlpDeliveryBudget`] from one of these with
/// [`OtlpDeliveryBudget::new`], which is where the bounds are checked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OtlpDeliveryBounds {
    /// Largest request body the transport will send.
    pub max_request_bytes: usize,
    /// Largest response body the transport will read, inclusive.
    ///
    /// A reply of exactly this many bytes is read; one byte more is refused.
    /// The request cap is inclusive in the same way.
    pub max_response_bytes: u64,
    /// Total attempts, including the first.
    pub max_attempts: u32,
    /// Deadline for establishing the connection.
    pub connect_timeout: Duration,
    /// Deadline for the request once connected.
    pub request_timeout: Duration,
    /// Delay before the first retry, doubling before each retry after it.
    ///
    /// A collector that asks for longer with `Retry-After` gets what it asked
    /// for instead. Either way `total_timeout` is the ceiling: a wait that
    /// would outlast it costs the remaining attempts, never the deadline.
    pub retry_backoff: Duration,
    /// Ceiling across every attempt.
    pub total_timeout: Duration,
}

impl OtlpDeliveryBudget {
    /// Construct a finite delivery budget, or reject bounds that are not.
    pub fn new(bounds: OtlpDeliveryBounds) -> Result<Self, OtlpDeliveryBudgetError> {
        let OtlpDeliveryBounds {
            max_request_bytes,
            max_response_bytes,
            max_attempts,
            connect_timeout,
            request_timeout,
            retry_backoff,
            total_timeout,
        } = bounds;
        if max_request_bytes == 0
            || max_response_bytes == 0
            || max_response_bytes > usize::MAX as u64
            || max_attempts == 0
            || connect_timeout.is_zero()
            || request_timeout.is_zero()
            || total_timeout.is_zero()
            || retry_backoff > total_timeout
            || Instant::now().checked_add(total_timeout).is_none()
        {
            return Err(OtlpDeliveryBudgetError::invalid());
        }
        Ok(Self {
            max_request_bytes,
            max_response_bytes,
            max_attempts,
            connect_timeout,
            request_timeout,
            retry_backoff,
            total_timeout,
        })
    }
}

/// Sanitized invalid-budget result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OtlpDeliveryBudgetError {
    classification: FailureClassification,
}

impl OtlpDeliveryBudgetError {
    fn invalid() -> Self {
        Self {
            classification: FailureClassification::for_code("observability.configuration.invalid")
                .expect("the append-only registry contains the observability configuration code"),
        }
    }

    /// API classification: workspace-internal exposed API.
    ///
    /// Return the stable shared failure classification.
    pub const fn classification(&self) -> &FailureClassification {
        &self.classification
    }
}

impl fmt::Display for OtlpDeliveryBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OTLP delivery bounds must be finite and non-zero")
    }
}

impl std::error::Error for OtlpDeliveryBudgetError {}

/// A temporary credential request capability scoped to one admitted origin.
pub struct OtlpCredentialRequest<'a> {
    origin: &'a Uri,
    headers: &'a mut Vec<(HeaderName, HeaderValue)>,
}

impl<'a> OtlpCredentialRequest<'a> {
    /// Return the normalized admitted origin used for this request.
    pub const fn admitted_origin(&self) -> &Uri {
        self.origin
    }

    /// Add one typed origin credential header.
    ///
    /// Transport-framing, host, content-type, and proxy-authorization headers
    /// are not credential-provider authority and are rejected.
    pub fn insert_header(
        &mut self,
        name: HeaderName,
        value: HeaderValue,
    ) -> Result<(), OtlpCredentialApplicationError> {
        if [
            &CONNECTION,
            &CONTENT_LENGTH,
            &CONTENT_TYPE,
            &EXPECT,
            &HOST,
            &PROXY_AUTHORIZATION,
            &TRANSFER_ENCODING,
        ]
        .contains(&&name)
        {
            return Err(OtlpCredentialApplicationError::ForbiddenHeader);
        }
        self.headers.push((name, value));
        Ok(())
    }
}

/// A borrowed run-local credential capability.
///
/// This transport does not resolve or retain credential material. A caller
/// may lend one already-resolved applicator for the duration of request
/// construction.
pub trait OtlpCredentialApplicator {
    /// Apply origin-scoped credentials to one admitted request capability.
    fn apply(
        &self,
        request: &mut OtlpCredentialRequest<'_>,
    ) -> Result<(), OtlpCredentialApplicationError>;
}

/// Credential application failed without exposing credential material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpCredentialApplicationError {
    /// The applicator attempted to control a forbidden transport or proxy header.
    ForbiddenHeader,
    /// The resolved run-local credential was unavailable or refused application.
    Unavailable,
}

impl fmt::Display for OtlpCredentialApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OTLP credential application failed")
    }
}

impl std::error::Error for OtlpCredentialApplicationError {}

/// Explicit credential-free or borrowed referenced-credential mode.
pub enum OtlpAuthentication<'a> {
    /// Construct no authorization or custom credential headers.
    None,
    /// Borrow a resolved run-local applicator after endpoint admission.
    Referenced(&'a dyn OtlpCredentialApplicator),
}

/// Accepted and rejected item counts from one collector response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OtlpDeliveryOutcome {
    signal: OtlpSignal,
    accepted: u64,
    rejected: u64,
    attempts: u32,
}

impl OtlpDeliveryOutcome {
    /// Return the delivered signal.
    pub const fn signal(self) -> OtlpSignal {
        self.signal
    }

    /// Return the count accepted by the collector.
    pub const fn accepted(self) -> u64 {
        self.accepted
    }

    /// Return the count rejected through signal-specific partial success.
    pub const fn rejected(self) -> u64 {
        self.rejected
    }

    /// Return the finite number of attempts consumed.
    pub const fn attempts(self) -> u32 {
        self.attempts
    }
}

/// Retryable reason retained after the finite attempt budget is exhausted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpRetryCause {
    /// The collector returned 429.
    CollectorThrottled,
    /// The collector returned one of the admitted transient 5xx statuses.
    CollectorUnavailable,
    /// The collector could not be connected to.
    Connect,
    /// A configured request or response deadline expired.
    Timeout,
}

/// Sanitized finite OTLP delivery result categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpDeliveryFailureKind {
    /// The payload was not the selected signal's structured OTLP JSON envelope.
    InvalidPayload,
    /// The serialized request exceeded the admitted request-body cap.
    RequestTooLarge,
    /// The borrowed credential capability refused application.
    CredentialApplication,
    /// The collector permanently rejected the request.
    CollectorRejected,
    /// A successful-status response was not valid signal-specific OTLP JSON.
    MalformedResponse,
    /// The response exceeded the admitted response-body cap.
    ResponseTooLarge,
    /// TLS setup or verification failed.
    Tls,
    /// A non-retryable transport failure occurred.
    Transport,
    /// The total delivery deadline expired outside an active attempt.
    Timeout,
    /// Shutdown was requested before the finite delivery completed.
    Shutdown,
    /// Every admitted retry attempt was consumed.
    RetryExhausted(OtlpRetryCause),
}

/// One sanitized non-authoritative OTLP delivery failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OtlpDeliveryFailure {
    signal: OtlpSignal,
    kind: OtlpDeliveryFailureKind,
    attempts: u32,
    classification: Option<FailureClassification>,
    /// Whether the collector answered 200 before this failure.
    ///
    /// A reply that cannot be read loses the confirmation, not the delivery.
    /// Counting such a batch as lost understates an export that arrived, and a
    /// supervisor comparing accepted against the run's record counts would read
    /// the shortfall as records the collector never received.
    reached_collector: bool,
}

impl OtlpDeliveryFailure {
    /// Return the signal whose optional delivery failed.
    pub const fn signal(&self) -> OtlpSignal {
        self.signal
    }

    /// Return the finite sanitized failure category.
    pub const fn kind(&self) -> OtlpDeliveryFailureKind {
        self.kind
    }

    /// Return the number of attempts consumed.
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Whether the collector accepted the batch before this failure.
    ///
    /// True only after a 200, where what failed was reading the reply. Callers
    /// accounting for delivered items must count such a batch as accepted.
    pub const fn reached_collector(&self) -> bool {
        self.reached_collector
    }

    /// API classification: workspace-internal exposed API.
    ///
    /// Return the registered classification when this is a failure rather than shutdown.
    pub const fn classification(&self) -> Option<&FailureClassification> {
        self.classification.as_ref()
    }
}

impl fmt::Display for OtlpDeliveryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.classification {
            Some(classification) => write!(
                formatter,
                "{}: {}",
                classification.code(),
                classification.message()
            ),
            None => formatter.write_str("OTLP delivery stopped by shutdown"),
        }
    }
}

impl std::error::Error for OtlpDeliveryFailure {}

/// Construct a loopback-only HTTP endpoint for deterministic socket tests.
///
/// This item is absent from ordinary builds. Integration tests include this
/// module directly under `cfg(test)` so the production crate never exposes an
/// HTTP admission path.
#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn admitted_loopback_endpoint(address: SocketAddr) -> AdmittedOtlpEndpoint {
    let authority = Authority::from_maybe_shared(address.to_string())
        .expect("a loopback socket address is a valid URI authority");
    let origin = Uri::builder()
        .scheme(Scheme::HTTP)
        .authority(authority)
        .path_and_query("/")
        .build()
        .expect("the fixed loopback URI is valid");
    AdmittedOtlpEndpoint {
        origin,
        https_only: false,
    }
}
