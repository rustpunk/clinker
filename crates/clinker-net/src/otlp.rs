//! Bounded synchronous OTLP/HTTP JSON delivery.
//!
//! Raw deployment text crosses [`admit_otlp_endpoint`] exactly once. The
//! resulting endpoint has private representation and can only derive the three
//! fixed OTLP signal routes. Delivery remains a finite blocking operation over
//! the crate's existing `ureq`/rustls stack.

use std::fmt;
#[cfg(test)]
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use clinker_core_types::FailureClassification;
use ureq::http::Uri;
use ureq::http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, EXPECT, HOST, HeaderName, HeaderValue,
    PROXY_AUTHORIZATION, TRANSFER_ENCODING,
};
use ureq::http::uri::{Authority, Scheme};

const OTLP_CONTENT_TYPE: &str = "application/json";
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

impl OtlpSignal {
    const fn route(self) -> &'static str {
        match self {
            Self::Logs => "/v1/logs",
            Self::Metrics => "/v1/metrics",
            Self::Traces => "/v1/traces",
        }
    }

    const fn envelope_key(self) -> &'static str {
        match self {
            Self::Logs => "resourceLogs",
            Self::Metrics => "resourceMetrics",
            Self::Traces => "resourceSpans",
        }
    }

    const fn rejected_key(self) -> &'static str {
        match self {
            Self::Logs => "rejectedLogRecords",
            Self::Metrics => "rejectedDataPoints",
            Self::Traces => "rejectedSpans",
        }
    }
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

impl OtlpDeliveryBudget {
    /// Construct a finite delivery budget.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_request_bytes: usize,
        max_response_bytes: u64,
        max_attempts: u32,
        connect_timeout: Duration,
        request_timeout: Duration,
        retry_backoff: Duration,
        total_timeout: Duration,
    ) -> Result<Self, OtlpDeliveryBudgetError> {
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

    /// API classification: workspace-internal exposed API.
    ///
    /// Return the registered classification when this is a failure rather than shutdown.
    pub const fn classification(&self) -> Option<&FailureClassification> {
        self.classification.as_ref()
    }

    fn new(signal: OtlpSignal, kind: OtlpDeliveryFailureKind, attempts: u32) -> Self {
        let code = match kind {
            OtlpDeliveryFailureKind::InvalidPayload | OtlpDeliveryFailureKind::RequestTooLarge => {
                Some("observability.configuration.invalid")
            }
            OtlpDeliveryFailureKind::CollectorRejected
            | OtlpDeliveryFailureKind::CredentialApplication
            | OtlpDeliveryFailureKind::MalformedResponse => Some("observability.delivery.rejected"),
            OtlpDeliveryFailureKind::ResponseTooLarge
            | OtlpDeliveryFailureKind::Tls
            | OtlpDeliveryFailureKind::Transport
            | OtlpDeliveryFailureKind::Timeout
            | OtlpDeliveryFailureKind::RetryExhausted(_) => Some("observability.delivery.failed"),
            OtlpDeliveryFailureKind::Shutdown => None,
        };
        Self {
            signal,
            kind,
            attempts,
            classification: code.map(|code| {
                FailureClassification::for_code(code)
                    .expect("the append-only registry contains OTLP failure codes")
            }),
        }
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

/// Deliver one already-serialized, bounded OTLP JSON signal batch.
///
/// The payload is parsed through `serde_json` before request construction and
/// must match the selected signal envelope. Collector outcomes are returned as
/// optional-observability results only; this API has no ETL, publication,
/// terminal, or exit authority.
pub fn send_otlp_json(
    endpoint: &AdmittedOtlpEndpoint,
    signal: OtlpSignal,
    payload: &[u8],
    budget: &OtlpDeliveryBudget,
    shutdown_requested: &dyn Fn() -> bool,
    authentication: OtlpAuthentication<'_>,
) -> Result<OtlpDeliveryOutcome, OtlpDeliveryFailure> {
    if shutdown_requested() {
        return Err(OtlpDeliveryFailure::new(
            signal,
            OtlpDeliveryFailureKind::Shutdown,
            0,
        ));
    }
    if payload.len() > budget.max_request_bytes {
        return Err(OtlpDeliveryFailure::new(
            signal,
            OtlpDeliveryFailureKind::RequestTooLarge,
            0,
        ));
    }
    let item_count = validate_and_count_payload(signal, payload).ok_or_else(|| {
        OtlpDeliveryFailure::new(signal, OtlpDeliveryFailureKind::InvalidPayload, 0)
    })?;
    let target = endpoint
        .route(signal)
        .map_err(|_| OtlpDeliveryFailure::new(signal, OtlpDeliveryFailureKind::Transport, 0))?;
    let deadline = Instant::now()
        .checked_add(budget.total_timeout)
        .ok_or_else(|| OtlpDeliveryFailure::new(signal, OtlpDeliveryFailureKind::Timeout, 0))?;

    let mut attempts = 0_u32;
    loop {
        if shutdown_requested() {
            return Err(OtlpDeliveryFailure::new(
                signal,
                OtlpDeliveryFailureKind::Shutdown,
                attempts,
            ));
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(OtlpDeliveryFailure::new(
                signal,
                OtlpDeliveryFailureKind::Timeout,
                attempts,
            ));
        };
        attempts = attempts.saturating_add(1);
        match send_attempt(
            endpoint,
            &target,
            payload,
            budget,
            remaining,
            &authentication,
        ) {
            Ok(mut response) => {
                let status = response.status().as_u16();
                if status == 200 {
                    let body = match response
                        .body_mut()
                        .with_config()
                        .limit(budget.max_response_bytes)
                        .read_to_vec()
                    {
                        Ok(body) => body,
                        // Reading the body means the handshake already
                        // succeeded, so a reset here is the wire failing, not
                        // a peer that was never a TLS endpoint.
                        Err(error) => match retryable_transport(&error, false) {
                            Some(_cause) if attempts < budget.max_attempts => {
                                wait_for_retry(
                                    signal,
                                    attempts,
                                    budget.retry_backoff,
                                    deadline,
                                    shutdown_requested,
                                )?;
                                continue;
                            }
                            Some(cause) => {
                                return Err(OtlpDeliveryFailure::new(
                                    signal,
                                    OtlpDeliveryFailureKind::RetryExhausted(cause),
                                    attempts,
                                ));
                            }
                            None => return Err(map_body_error(signal, attempts, &error)),
                        },
                    };
                    let rejected = parse_response(signal, &body, item_count).ok_or_else(|| {
                        OtlpDeliveryFailure::new(
                            signal,
                            OtlpDeliveryFailureKind::MalformedResponse,
                            attempts,
                        )
                    })?;
                    return Ok(OtlpDeliveryOutcome {
                        signal,
                        accepted: item_count - rejected,
                        rejected,
                        attempts,
                    });
                }
                let retry_cause = retryable_status(status);
                if let Some(cause) = retry_cause {
                    if attempts >= budget.max_attempts {
                        return Err(OtlpDeliveryFailure::new(
                            signal,
                            OtlpDeliveryFailureKind::RetryExhausted(cause),
                            attempts,
                        ));
                    }
                    wait_for_retry(
                        signal,
                        attempts,
                        budget.retry_backoff,
                        deadline,
                        shutdown_requested,
                    )?;
                    continue;
                }
                return Err(OtlpDeliveryFailure::new(
                    signal,
                    OtlpDeliveryFailureKind::CollectorRejected,
                    attempts,
                ));
            }
            Err(error) => match retryable_transport(&error, endpoint.https_only) {
                Some(_cause) if attempts < budget.max_attempts => {
                    wait_for_retry(
                        signal,
                        attempts,
                        budget.retry_backoff,
                        deadline,
                        shutdown_requested,
                    )?;
                }
                Some(cause) => {
                    return Err(OtlpDeliveryFailure::new(
                        signal,
                        OtlpDeliveryFailureKind::RetryExhausted(cause),
                        attempts,
                    ));
                }
                None => {
                    return Err(map_transport_error(
                        signal,
                        attempts,
                        endpoint.https_only,
                        &error,
                    ));
                }
            },
        }
    }
}

impl AdmittedOtlpEndpoint {
    fn route(&self, signal: OtlpSignal) -> Result<Uri, ureq::http::Error> {
        Uri::builder()
            .scheme(
                self.origin
                    .scheme()
                    .expect("admitted origins always retain a scheme")
                    .clone(),
            )
            .authority(
                self.origin
                    .authority()
                    .expect("admitted origins always retain an authority")
                    .clone(),
            )
            .path_and_query(signal.route())
            .build()
    }
}

fn send_attempt(
    endpoint: &AdmittedOtlpEndpoint,
    target: &Uri,
    payload: &[u8],
    budget: &OtlpDeliveryBudget,
    remaining: Duration,
    authentication: &OtlpAuthentication<'_>,
) -> Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    let attempt_timeout = remaining.min(
        budget
            .connect_timeout
            .saturating_add(budget.request_timeout),
    );
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .https_only(endpoint.https_only)
        .max_redirects(0)
        .http_status_as_error(false)
        .timeout_global(Some(attempt_timeout))
        .timeout_connect(Some(budget.connect_timeout.min(remaining)))
        .timeout_send_request(Some(budget.request_timeout.min(remaining)))
        .timeout_send_body(Some(budget.request_timeout.min(remaining)))
        .timeout_recv_response(Some(budget.request_timeout.min(remaining)))
        .timeout_recv_body(Some(budget.request_timeout.min(remaining)))
        .build()
        .into();

    let mut credential_headers = Vec::new();
    if let OtlpAuthentication::Referenced(applicator) = authentication {
        let mut credential_request = OtlpCredentialRequest {
            origin: &endpoint.origin,
            headers: &mut credential_headers,
        };
        applicator.apply(&mut credential_request).map_err(|_| {
            ureq::Error::Other(Box::new(OtlpCredentialApplicationError::Unavailable))
        })?;
    }

    let mut request = agent.post(target.clone()).content_type(OTLP_CONTENT_TYPE);
    for (name, value) in credential_headers {
        request = request.header(name, value);
    }
    request.send(payload)
}

fn retryable_status(status: u16) -> Option<OtlpRetryCause> {
    match status {
        429 => Some(OtlpRetryCause::CollectorThrottled),
        502..=504 => Some(OtlpRetryCause::CollectorUnavailable),
        _ => None,
    }
}

/// Whether `error` is a peer that is not speaking TLS on an origin that
/// requires it.
///
/// Shared by the retry decision and the terminal classification so the two
/// cannot disagree about the same error. The peer answered, so this is not a
/// reachability failure; what it is instead depends on how far the exchange
/// got and on the host's socket behaviour, which is why the answer cannot be
/// one error kind. A plaintext HTTP reply reaches rustls as invalid data or
/// as a protocol parse failure; a peer that closes first arrives as
/// end-of-stream, a reset, or an abort; and writing a handshake to a socket
/// the peer has already closed is a broken pipe on the BSD-derived hosts and
/// buffered until the read on Linux. All of them say the remote is not a TLS
/// endpoint, which no amount of retrying will change.
///
/// Reachability failures — refused, unresolved, or a deadline that expired
/// before a connection existed — are deliberately not here: those describe
/// getting to the peer rather than what the peer turned out to be.
fn is_tls_endpoint_mismatch(error: &ureq::Error, https_only: bool) -> bool {
    if !https_only {
        return false;
    }
    match error {
        ureq::Error::Protocol(_) => true,
        ureq::Error::Io(io) => matches!(
            io.kind(),
            std::io::ErrorKind::InvalidData
                | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::NotConnected
        ),
        _ => false,
    }
}

fn retryable_transport(error: &ureq::Error, https_only: bool) -> Option<OtlpRetryCause> {
    // Decline before the reachability arms below. A reset or an abort during
    // the handshake is indistinguishable from one on the wire by kind alone,
    // so without this the retry decision claims a TLS mismatch as a connect
    // failure and the terminal classification never runs.
    if is_tls_endpoint_mismatch(error, https_only) {
        return None;
    }
    match error {
        // A deadline that expires before a connection exists is a failure to
        // reach the collector, not a slow one; what separates the two is the
        // phase the deadline expired in, not that it was a deadline. Hosts
        // differ in whether an unreachable port is refused outright or simply
        // never answers, so classifying by phase keeps one unreachable
        // collector from being reported two different ways.
        ureq::Error::Timeout(ureq::Timeout::Connect | ureq::Timeout::Resolve) => {
            Some(OtlpRetryCause::Connect)
        }
        ureq::Error::Timeout(_) => Some(OtlpRetryCause::Timeout),
        ureq::Error::HostNotFound
        | ureq::Error::ConnectionFailed
        | ureq::Error::ConnectProxyFailed(_) => Some(OtlpRetryCause::Connect),
        ureq::Error::Io(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::TimedOut
            ) =>
        {
            Some(if error.kind() == std::io::ErrorKind::TimedOut {
                OtlpRetryCause::Timeout
            } else {
                OtlpRetryCause::Connect
            })
        }
        _ => None,
    }
}

fn map_transport_error(
    signal: OtlpSignal,
    attempts: u32,
    https_only: bool,
    error: &ureq::Error,
) -> OtlpDeliveryFailure {
    let kind = match error {
        ureq::Error::Tls(_) | ureq::Error::Rustls(_) | ureq::Error::TlsRequired => {
            OtlpDeliveryFailureKind::Tls
        }
        // Errors that fail before a handshake can begin (refused, unreachable)
        // stay Transport, because those describe reachability rather than
        // protocol.
        error if is_tls_endpoint_mismatch(error, https_only) => OtlpDeliveryFailureKind::Tls,
        ureq::Error::Timeout(_) => OtlpDeliveryFailureKind::Timeout,
        ureq::Error::Other(other) if other.is::<OtlpCredentialApplicationError>() => {
            OtlpDeliveryFailureKind::CredentialApplication
        }
        _ => OtlpDeliveryFailureKind::Transport,
    };
    OtlpDeliveryFailure::new(signal, kind, attempts)
}

fn map_body_error(signal: OtlpSignal, attempts: u32, error: &ureq::Error) -> OtlpDeliveryFailure {
    let kind = match error {
        ureq::Error::BodyExceedsLimit(_) => OtlpDeliveryFailureKind::ResponseTooLarge,
        ureq::Error::Timeout(_) => OtlpDeliveryFailureKind::Timeout,
        _ => OtlpDeliveryFailureKind::Transport,
    };
    OtlpDeliveryFailure::new(signal, kind, attempts)
}

fn wait_for_retry(
    signal: OtlpSignal,
    attempts: u32,
    backoff: Duration,
    deadline: Instant,
    shutdown_requested: &dyn Fn() -> bool,
) -> Result<(), OtlpDeliveryFailure> {
    let retry_deadline = Instant::now().checked_add(backoff).unwrap_or(deadline);
    loop {
        if shutdown_requested() {
            return Err(OtlpDeliveryFailure::new(
                signal,
                OtlpDeliveryFailureKind::Shutdown,
                attempts,
            ));
        }
        let now = Instant::now();
        if now >= retry_deadline {
            return Ok(());
        }
        if now >= deadline {
            return Err(OtlpDeliveryFailure::new(
                signal,
                OtlpDeliveryFailureKind::Timeout,
                attempts,
            ));
        }
        thread::sleep(
            retry_deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(10)),
        );
    }
}

fn validate_and_count_payload(signal: OtlpSignal, payload: &[u8]) -> Option<u64> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    let resources = object.get(signal.envelope_key())?.as_array()?;
    match signal {
        OtlpSignal::Logs => count_nested(resources, "scopeLogs", "logRecords"),
        OtlpSignal::Traces => count_nested(resources, "scopeSpans", "spans"),
        OtlpSignal::Metrics => count_metrics(resources),
    }
}

fn count_nested(resources: &[serde_json::Value], scopes_key: &str, items_key: &str) -> Option<u64> {
    let mut total = 0_u64;
    for resource in resources {
        let resource = resource.as_object()?;
        let scopes = match resource.get(scopes_key) {
            Some(scopes) => scopes.as_array()?,
            None => continue,
        };
        for scope in scopes {
            let scope = scope.as_object()?;
            let items = match scope.get(items_key) {
                Some(items) => items.as_array()?,
                None => continue,
            };
            total = total.checked_add(u64::try_from(items.len()).ok()?)?;
        }
    }
    Some(total)
}

fn count_metrics(resources: &[serde_json::Value]) -> Option<u64> {
    let mut total = 0_u64;
    for resource in resources {
        let resource = resource.as_object()?;
        let scopes = match resource.get("scopeMetrics") {
            Some(scopes) => scopes.as_array()?,
            None => continue,
        };
        for scope in scopes {
            let scope = scope.as_object()?;
            let metrics = match scope.get("metrics") {
                Some(metrics) => metrics.as_array()?,
                None => continue,
            };
            for metric in metrics {
                let metric = metric.as_object()?;
                let mut matching_kinds = 0_u8;
                let mut metric_points = 0_u64;
                for kind in [
                    "gauge",
                    "sum",
                    "histogram",
                    "exponentialHistogram",
                    "summary",
                ] {
                    if let Some(data) = metric.get(kind) {
                        matching_kinds = matching_kinds.saturating_add(1);
                        let points = data.as_object()?.get("dataPoints")?.as_array()?;
                        metric_points = u64::try_from(points.len()).ok()?;
                    }
                }
                if matching_kinds != 1 {
                    return None;
                }
                total = total.checked_add(metric_points)?;
            }
        }
    }
    Some(total)
}

fn parse_response(signal: OtlpSignal, body: &[u8], sent: u64) -> Option<u64> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let response = value.as_object()?;
    if response.is_empty() {
        return Some(0);
    }
    if response.len() != 1 {
        return None;
    }
    let partial = response.get("partialSuccess")?.as_object()?;
    if partial
        .keys()
        .any(|key| key != signal.rejected_key() && key != "errorMessage")
    {
        return None;
    }
    let rejected = match partial.get(signal.rejected_key()) {
        None => 0,
        Some(serde_json::Value::String(value)) => value.parse::<u64>().ok()?,
        Some(serde_json::Value::Number(value)) => value.as_u64()?,
        Some(_) => return None,
    };
    (rejected <= sent).then_some(rejected)
}

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
