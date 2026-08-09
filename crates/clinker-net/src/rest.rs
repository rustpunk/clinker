//! Paginated HTTP GET finite-pull source.
//!
//! [`RestRecordSource`] issues synchronous GETs against a base URL,
//! advancing through pages by one of the [`RestPagination`] strategies
//! until the strategy reports no next page OR a hard page/record cap
//! trips. Each response body is decoded through the source's declared
//! on-disk format (`json`/`xml`) by reusing the byte-stream
//! [`FormatReader`](clinker_format::traits::FormatReader) over a
//! `Cursor<Vec<u8>>`, then reprojected onto the authored schema by the
//! same [`CoercingReader`] the file arm uses — so per-row coercion is
//! lenient at the reader and per-row failures route to the DLQ at the
//! Transform stage exactly like file sources.
//!
//! Finiteness is a HARD reader property: [`RestSourceConfig::max_pages`]
//! is mandatory and the reader fails closed at the cap if the server keeps
//! offering a next page. The pull runs on its own `std::thread` (the
//! ingest thread) driving a blocking `ureq` client to exhaustion, with
//! no async runtime.

mod continuation;

use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use clinker_exec::pipeline::schema_coerce::CoercingReader;
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_exec::source::RecordSource;
use clinker_format::Column;
use clinker_format::traits::FormatReader;
use clinker_format::{EnvelopeConfig, FormatError};
use clinker_plan::config::pipeline_node::{OnUnmapped, WIDENED_SIDECAR_COLUMN};
use clinker_plan::config::{InputFormat, RestAuth, RestPagination, RestSourceConfig, SourceConfig};
use clinker_record::{FieldMetadata, Record, Schema, SchemaBuilder, Value};
use indexmap::IndexMap;

use crate::schema_err;

use continuation::{AuthorizedUrl, ContinuationError, Origin};

fn continuation_format_error(error: ContinuationError) -> FormatError {
    FormatError::classified(error.classification_code(), error.to_string())
}

/// Per-body cap. Each individual page body is bounded so a misbehaving
/// server cannot stream an unbounded body into one `read_to_vec`. 64 MiB
/// is generous for one page of a paginated API; the reader's overall
/// finiteness comes from `max_pages` / `max_records`.
const MAX_PAGE_BYTES: u64 = 64 * 1024 * 1024;

/// Redirect budget for one logical page request. The transport agent follows
/// zero redirects itself; every hop is resolved and authorized here.
const MAX_REDIRECTS: u32 = 10;

/// Synchronous paginated HTTP source. One per declared `rest` Source;
/// owned by that source's ingest thread. Constructed through
/// [`crate::build_rest_source`], which boxes it as a `dyn RecordSource`.
pub(crate) struct RestRecordSource {
    agent: ureq::Agent,
    cfg: RestSourceConfig,
    admitted_origin: Origin,
    base_url: AuthorizedUrl,
    format: InputFormat,
    multi_value: MultiValueRead,
    schema_decl: Vec<Column>,
    on_unmapped: OnUnmapped,
    source_name: String,
    /// Output schema = authored columns (+ `$widened` sidecar under
    /// AutoWiden). Resolved once from the declaration so every page's
    /// `CoercingReader` projects onto an identical schema.
    output_schema: Arc<Schema>,
    /// Decoder for the page currently being drained. `None` before the
    /// first page is fetched and after the cursor exhausts.
    current_page: Option<Box<dyn FormatReader>>,
    /// Records emitted from the page currently being drained. Reset on
    /// each fetch and read when the page drains to decide, for the offset
    /// strategy, whether it was a short (final) page. Counting the rows
    /// the emit path actually produces — rather than re-decoding the body
    /// with a second reader — keeps the short-page signal consistent with
    /// the records that were yielded, regardless of body shape
    /// (top-level array, `{"data":[…]}` wrapper, or XML).
    current_page_records: u32,
    /// Pagination cursor state: the next page's continuation handle, or
    /// `None` once the strategy reports no more pages.
    next: NextPage,
    pages_fetched: u32,
    records_emitted: u64,
    /// Authorized logical page URLs already requested in this finite pull.
    /// Reappearance is a continuation cycle and fails before a repeat request.
    visited_pages: HashSet<String>,
    shutdown: Option<ShutdownToken>,
}

/// How to fetch the next page, derived from the pagination strategy and
/// updated after each response.
enum NextPage {
    /// Offset/limit: `?offset=N&limit=L`, advancing `offset` by `limit`.
    Offset {
        offset_param: String,
        limit_param: String,
        limit: u32,
        offset: u32,
        /// Cleared once a page returns fewer than `limit` records.
        more: bool,
    },
    /// Cursor-token: send `?<cursor_param>=<token>`; the next token is
    /// read from a JSON pointer in each body. `None` token means the
    /// first request carries no cursor.
    CursorToken {
        cursor_param: String,
        next_token_pointer: String,
        token: Option<String>,
        /// Cleared once the body carries no next token.
        more: bool,
    },
    /// RFC 5988 Link header: the absolute/relative URL of the next page,
    /// or `None` once no `rel="next"` link is present. The first request
    /// uses the base URL.
    LinkHeader {
        next_url: Option<AuthorizedUrl>,
        more: bool,
    },
    /// Single GET, no pagination.
    Single { fetched: bool },
}

impl RestRecordSource {
    /// Build a REST source from its transport config + the source node's
    /// declared `schema:` / `on_unmapped` / format. The HTTP agent is
    /// configured with the per-request timeout and manual status handling
    /// so 5xx can be retried distinctly from a fatal 4xx.
    pub(crate) fn new(
        cfg: RestSourceConfig,
        source: &SourceConfig,
        schema_decl: &[Column],
        on_unmapped: OnUnmapped,
    ) -> Result<Self, FormatError> {
        let (admitted_origin, base_url) =
            continuation::authorize_initial(&cfg.url).map_err(continuation_format_error)?;
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(cfg.timeout_secs)))
            // Inspect status codes manually: 5xx is a retry, 4xx is fatal.
            .http_status_as_error(false)
            // Redirects are a server-directed request boundary. Disable ureq's
            // automatic following so every hop passes through origin policy
            // before request construction and credential application.
            .max_redirects(0)
            .build()
            .into();

        let next = match &cfg.pagination {
            RestPagination::None => NextPage::Single { fetched: false },
            RestPagination::Offset {
                offset_param,
                limit_param,
                limit,
            } => NextPage::Offset {
                offset_param: offset_param.clone(),
                limit_param: limit_param.clone(),
                limit: *limit,
                offset: 0,
                more: true,
            },
            RestPagination::CursorToken {
                cursor_param,
                next_token_pointer,
            } => NextPage::CursorToken {
                cursor_param: cursor_param.clone(),
                next_token_pointer: next_token_pointer.clone(),
                token: None,
                more: true,
            },
            RestPagination::LinkHeader => NextPage::LinkHeader {
                next_url: None,
                more: true,
            },
        };

        // The output schema is the authored columns plus the `$widened`
        // engine-stamped sidecar under AutoWiden — identical to the schema
        // every page's `CoercingReader` projects onto, and independent of
        // any body contents. Build it directly from the declaration so no
        // throwaway request or body decode is needed before the first
        // page is fetched.
        let output_schema = build_output_schema(schema_decl, &on_unmapped);

        Ok(Self {
            agent,
            cfg,
            admitted_origin,
            base_url,
            format: source.format.clone(),
            multi_value: MultiValueRead::from_source(source, schema_decl),
            schema_decl: schema_decl.to_vec(),
            on_unmapped,
            source_name: source.name.clone(),
            output_schema,
            current_page: None,
            current_page_records: 0,
            next,
            pages_fetched: 0,
            records_emitted: 0,
            visited_pages: HashSet::new(),
            shutdown: None,
        })
    }

    /// Whether an explicit record cap or shutdown request ends the pull.
    /// Page-bound exhaustion is handled separately after checking whether the
    /// server actually offered another continuation.
    fn stop_requested(&self) -> bool {
        if let Some(max) = self.cfg.max_records
            && self.records_emitted >= max
        {
            return true;
        }
        self.shutdown.as_ref().is_some_and(|t| t.is_requested())
    }

    /// Fetch the next page if the cursor offers one and no cap is hit,
    /// installing its decoder as `current_page`. Returns `Ok(false)` when
    /// the cursor is exhausted (clean EOF).
    fn fetch_next_page(&mut self) -> Result<bool, FormatError> {
        if self.stop_requested() {
            return Ok(false);
        }
        let url = match self.build_request_url()? {
            Some(url) => url,
            None => return Ok(false),
        };
        if self.pages_fetched >= self.cfg.max_pages {
            return Err(FormatError::classified(
                "rest.protocol.page_limit_reached",
                format!(
                    "{}; rest source {:?}: page_limit_reached max_pages={} next_page={} target={}",
                    ContinuationError::for_code("rest.protocol.page_limit_reached"),
                    self.source_name,
                    self.cfg.max_pages,
                    self.pages_fetched.saturating_add(1),
                    url.diagnostic_target(),
                ),
            ));
        }
        if !self.visited_pages.insert(url.as_str().to_owned()) {
            return Err(continuation_format_error(ContinuationError::for_code(
                "rest.protocol.unsupported_continuation",
            )));
        }

        let bytes = self.get_with_retry(&url)?;
        self.pages_fetched += 1;

        // Advance the cursor for the strategies whose continuation signal
        // is carried directly by this response (next token in the body,
        // `rel="next"` link in the headers). The offset strategy's signal
        // is the row count, which is only known once the page drains, so
        // its advance is deferred to `on_page_drained`.
        self.advance_cursor(&bytes)?;

        let reader = decode_body(&self.format, &self.multi_value, bytes.body)?;
        // A REST body decodes to native/untyped records (JSON), so this is the
        // sole coercion pass (`pretyped: false`) — declared types and each
        // column's `format:` are applied here.
        let coercing = CoercingReader::new(
            reader,
            &self.schema_decl,
            self.on_unmapped.clone(),
            &self.source_name,
            false,
        )?;
        self.current_page = Some(Box::new(coercing));
        self.current_page_records = 0;
        Ok(true)
    }

    /// React to the current page draining. For the offset strategy this is
    /// where the last-page decision lands: a page that yielded fewer rows
    /// than `limit` is the final page; a full page advances the offset for
    /// the next GET. The count is the number of records the emit path
    /// actually produced for this page, so it stays consistent with the
    /// rows yielded for every body shape. Other strategies advanced their
    /// cursor already in [`Self::advance_cursor`].
    fn on_page_drained(&mut self) {
        if let NextPage::Offset {
            limit,
            offset,
            more,
            ..
        } = &mut self.next
        {
            if self.current_page_records < *limit {
                *more = false;
            } else {
                *offset = offset.saturating_add(*limit);
            }
        }
    }

    /// Compute the URL for the next request, or `None` when the cursor is
    /// exhausted.
    fn build_request_url(&self) -> Result<Option<AuthorizedUrl>, FormatError> {
        let raw = match &self.next {
            NextPage::Single { fetched } => (!fetched).then(|| self.base_url.as_str().to_owned()),
            NextPage::Offset {
                offset_param,
                limit_param,
                limit,
                offset,
                more,
            } => more.then(|| {
                append_query(
                    self.base_url.as_str(),
                    &[
                        (offset_param, &offset.to_string()),
                        (limit_param, &limit.to_string()),
                    ],
                )
            }),
            NextPage::CursorToken {
                cursor_param,
                token,
                more,
                ..
            } => more.then(|| match token {
                Some(t) => append_query(self.base_url.as_str(), &[(cursor_param, t)]),
                None => self.base_url.as_str().to_owned(),
            }),
            NextPage::LinkHeader { next_url, more } => {
                return Ok(more.then(|| next_url.clone().unwrap_or_else(|| self.base_url.clone())));
            }
        };
        raw.map(|raw| {
            continuation::resolve_and_authorize(&self.base_url, &raw, &self.admitted_origin)
                .map_err(continuation_format_error)
        })
        .transpose()
    }

    fn continuation_failure(code: &'static str) -> FormatError {
        continuation_format_error(ContinuationError::for_code(code))
    }

    fn sanitized_request_failure(
        &self,
        url: &AuthorizedUrl,
        attempt: u32,
        failure: RequestFailure,
    ) -> FormatError {
        let message = format!(
            "rest source {:?}: request_failed class={} attempt={} page={} target={}",
            self.source_name,
            failure.as_str(),
            attempt,
            self.pages_fetched.saturating_add(1),
            url.diagnostic_target(),
        );
        let code = failure.classification_code();
        FormatError::classified(code, message)
    }

    fn request_for(
        &self,
        url: &AuthorizedUrl,
    ) -> ureq::RequestBuilder<ureq::typestate::WithoutBody> {
        match &self.cfg.auth {
            RestAuth::None => self.agent.get(url.as_str()),
            RestAuth::Bearer { token } => self
                .agent
                .get(url.as_str())
                .header("Authorization", format!("Bearer {token}")),
            RestAuth::Header { name, value } => self
                .agent
                .get(url.as_str())
                .header(name.as_str(), value.as_str()),
        }
    }

    /// Update the pagination cursor from the most recent response, for the
    /// strategies whose continuation signal the response carries directly.
    /// The offset strategy is deferred to [`Self::on_page_drained`] because
    /// its signal is the page's row count.
    fn advance_cursor(&mut self, resp: &PageResponse) -> Result<(), FormatError> {
        match &mut self.next {
            NextPage::Single { fetched } => {
                *fetched = true;
            }
            // Offset advances at page-drain time, not here — see
            // `on_page_drained`.
            NextPage::Offset { .. } => {}
            NextPage::CursorToken {
                next_token_pointer,
                token,
                more,
                ..
            } => match read_json_pointer_string(&resp.body, next_token_pointer)? {
                Some(t) => *token = Some(t),
                None => *more = false,
            },
            NextPage::LinkHeader { next_url, more } => match resp.next_link.clone() {
                Some(link) => *next_url = Some(link),
                None => *more = false,
            },
        }
        Ok(())
    }

    /// GET with bounded transient-failure retry. 5xx and connect/timeout
    /// errors retry up to `cfg.retries`; a 4xx is a fatal hard error
    /// (the request is malformed, retrying cannot help). Polls the
    /// shutdown token between attempts so cancellation lands promptly.
    fn get_with_retry(&self, start_url: &AuthorizedUrl) -> Result<PageResponse, FormatError> {
        let mut attempt: u32 = 0;
        loop {
            if self.shutdown.as_ref().is_some_and(|t| t.is_requested()) {
                return Err(FormatError::Interrupted);
            }
            let mut url = start_url.clone();
            let mut redirects = HashSet::from([url.as_str().to_owned()]);
            let mut redirect_count = 0_u32;
            // Whether the peer answered before this attempt failed. The call
            // site knows it: a failure from `call()` means no response line
            // ever arrived, and one from the body read means it did. Deriving
            // it afterwards from the error value cannot work — a dropped
            // connection surfaces as a protocol error, an unexpected
            // end-of-file, or the unnamed catch-all depending on how far the
            // exchange got and which host it ran on, and the same variants are
            // reachable from a peer that answered.
            let (retry_failure, peer_answered) = loop {
                let request = self.request_for(&url);
                let mut response = match request.call() {
                    Ok(response) => response,
                    Err(error) => {
                        let failure = RequestFailure::from_transport(&error);
                        // Every 4xx returns here, including the two that ask
                        // for later rather than refusing: this loop has no
                        // delay to offer them, so retrying would only hurry
                        // the server that asked us to slow down. They are
                        // classified retryable so the supervisor waits.
                        if matches!(failure, RequestFailure::HttpStatus(400..=499)) {
                            return Err(self.sanitized_request_failure(
                                &url,
                                attempt.saturating_add(1),
                                failure,
                            ));
                        }
                        break (failure, false);
                    }
                };
                let status = response.status().as_u16();
                if (300..400).contains(&status) {
                    if redirect_count >= MAX_REDIRECTS {
                        return Err(Self::continuation_failure(
                            "rest.protocol.unsupported_continuation",
                        ));
                    }
                    let target = continuation::redirect_location(
                        response.headers(),
                        &url,
                        &self.admitted_origin,
                    )
                    .map_err(continuation_format_error)?;
                    if !redirects.insert(target.as_str().to_owned()) {
                        return Err(Self::continuation_failure(
                            "rest.protocol.unsupported_continuation",
                        ));
                    }
                    redirect_count = redirect_count.saturating_add(1);
                    url = target;
                    continue;
                }
                // Deliberately not retried in this loop, which sleeps for
                // nothing between attempts. Re-requesting a throttled endpoint
                // `retries` times without pause answers "please wait" by
                // asking again immediately, and providers that escalate turn
                // that into a ban — worse than either giving up or waiting.
                //
                // The classification still says retry-with-backoff, which is
                // advice to whoever supervises this run and does have a delay
                // to apply. Honouring it here needs a bounded, shutdown-aware
                // wait that this reader does not have; adding one is worth
                // doing and is a change of its own, because the 5xx path below
                // retries with no pause either.
                if (500..600).contains(&status) {
                    break (RequestFailure::HttpStatus(status), true);
                }
                if !(200..300).contains(&status) {
                    return Err(self.sanitized_request_failure(
                        &url,
                        attempt.saturating_add(1),
                        RequestFailure::HttpStatus(status),
                    ));
                }
                let next_link = if matches!(self.next, NextPage::LinkHeader { .. }) {
                    continuation::next_link(response.headers(), &url, &self.admitted_origin)
                        .map_err(continuation_format_error)?
                } else {
                    None
                };
                let body = match response
                    .body_mut()
                    .with_config()
                    .limit(MAX_PAGE_BYTES)
                    .read_to_vec()
                {
                    Ok(body) => body,
                    Err(error) => {
                        let failure = RequestFailure::from_transport(&error).in_response_phase();
                        if failure.retryable_body_read() {
                            break (failure, true);
                        }
                        return Err(self.sanitized_request_failure(
                            &url,
                            attempt.saturating_add(1),
                            failure,
                        ));
                    }
                };
                return Ok(PageResponse { body, next_link });
            };
            // Cancellation abandons a retry that was going to happen, and it
            // explains a teardown of the request we were inside — a failure
            // that arrived before the peer answered. What it does not do is
            // erase a verdict already reached: an answer means the exchange
            // completed, and once no attempt remains that outage is the run's
            // outcome whether or not a signal arrived afterwards. Reporting it
            // as a clean cancellation left the outage in no terminal at all
            // and had the orchestrator re-queue the batch.
            let cancelled = self.shutdown.as_ref().is_some_and(|t| t.is_requested());
            if cancelled
                && (attempt < self.cfg.retries
                    || (!peer_answered && retry_failure.is_cancellable_transport()))
            {
                return Err(FormatError::Interrupted);
            }
            if !cancelled && attempt < self.cfg.retries {
                attempt = attempt.saturating_add(1);
                continue;
            }
            return Err(self.sanitized_request_failure(
                &url,
                attempt.saturating_add(1),
                retry_failure,
            ));
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RequestFailure {
    HttpStatus(u16),
    /// A deadline that expired before the peer answered.
    Timeout,
    /// A deadline that expired after the peer answered — while its headers or
    /// body were being read. The exchange had already begun, so a signal
    /// arriving afterwards does not explain it.
    ResponseTimeout,
    /// An I/O failure while reading a reply the peer had begun sending.
    ResponseIo(std::io::ErrorKind),
    HostNotFound,
    Tls,
    ProxyConnection,
    Connection,
    /// A reply that did not parse as HTTP, or a connection that ended before
    /// one arrived. Which of the two it was depends on how far the exchange
    /// got, so this says nothing about whether the request was well formed.
    Protocol,
    /// A request this client could not route: a URI it could not parse, a
    /// proxy address it could not use, a redirect it could not follow. The
    /// material is local and permanent, so every attempt fails identically
    /// and a shutdown arriving alongside it explains nothing.
    Unroutable,
    BodyLimit,
    Io(std::io::ErrorKind),
    Transport,
}

impl RequestFailure {
    /// Whether a pending shutdown explains this failure.
    ///
    /// True only where the peer never answered, which is what tearing down a
    /// request in flight produces. A status means the exchange completed and
    /// the peer rejected it, and that verdict is independent of any signal
    /// that arrives afterwards.
    fn is_cancellable_transport(self) -> bool {
        // `Transport` is included, but only because every permanent failure
        // that used to hide behind it is now named. It reached this catch-all
        // for TLS errors once, and a certificate nobody trusts was reported as
        // a clean cancellation; the fix was naming the TLS variants, not
        // refusing the catch-all. What remains under it is a failure this
        // mapping cannot identify, and when a shutdown is pending the
        // cancellation is the one thing actually known — a peer that dropped
        // the connection mid-request surfaces here on some hosts and as an I/O
        // error on others, and both are the same operator action.
        if let Self::Io(kind) = self {
            // Not every local I/O failure. Reading a client certificate, a
            // trust store, or a socket path the process may not open fails
            // permanently and identically on every attempt, and a shutdown
            // that happens to be pending does not explain it — the same harm
            // the named verdicts exist to prevent.
            // The same set the verdict calls unreadable local material, and
            // no more. `InvalidData` and `InvalidInput` were left here after
            // being removed there, so a cancelled run whose last error was a
            // plaintext TLS handshake reported a retryable failure instead of
            // an abort — the two rules disagreeing about one error kind.
            return !matches!(
                kind,
                std::io::ErrorKind::PermissionDenied
                    | std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::IsADirectory
                    | std::io::ErrorKind::ReadOnlyFilesystem
            );
        }
        // `Protocol` is here because the caller only consults this rule when
        // the peer never answered, and a connection that ends before a reply
        // parses is spelled as a protocol error on some hosts and as an I/O
        // error on others. `Unroutable` is not: it is local, permanent
        // material, and it reached the same variant until this split — which
        // is how a cancelled run whose URL was malformed reported an abort.
        matches!(
            self,
            Self::Timeout
                | Self::Connection
                | Self::ProxyConnection
                | Self::Protocol
                | Self::Transport
        )
    }

    /// Re-label a failure observed while reading a reply the peer had already
    /// begun sending.
    ///
    /// The request-phase spellings and the response-phase ones are the same
    /// error kinds, so only the call site knows which happened. A response-
    /// phase failure means the peer answered, and a signal arriving afterwards
    /// does not explain it.
    const fn in_response_phase(self) -> Self {
        match self {
            Self::Timeout => Self::ResponseTimeout,
            Self::Io(kind) => Self::ResponseIo(kind),
            other => other,
        }
    }

    const fn classification_code(self) -> &'static str {
        match self {
            // Ahead of the 4xx range, which would otherwise swallow these.
            // Throttling and a request timeout are the server asking for
            // later, not refusing: grouping them with the rest told a
            // supervisor the batch could never succeed, so a rate-limited API
            // permanently abandoned the day's records — while the OTLP
            // transport in this same branch treats 429 as retryable.
            Self::HttpStatus(408 | 429) => "infrastructure.runtime.source_unavailable",
            Self::HttpStatus(400..=499) => "rest.http.client_error",
            Self::BodyLimit => "rest.protocol.page_body_limit_reached",
            // A certificate the client will not trust, and a hostname that
            // does not resolve, are settled facts about the endpoint as it is
            // configured. Reporting them as a temporarily unavailable source
            // told a supervisor to keep re-queuing a batch that cannot succeed
            // until a human changes something, which is the same advice the
            // cancellation rule was corrected to stop giving.
            Self::Tls => "source.endpoint.untrusted_tls",
            Self::HostNotFound => "source.endpoint.unresolvable",
            // The same permanent local failures the cancellation rule now
            // declines to explain away. Excluding them there without changing
            // this left them reported as a temporarily unavailable source, so
            // a certificate the process cannot read was still re-queued
            // forever — the caller was corrected and the callee that produces
            // the operator-visible verdict was not.
            // Only the kinds that can mean nothing but a local file this
            // process cannot use. `InvalidData` and `InvalidInput` are absent
            // deliberately: invalid data is how a plaintext reply to a TLS
            // handshake arrives on the Unix hosts, so claiming it here sent a
            // Linux operator to inspect certificate files that were fine while
            // the identical misconfiguration stayed retryable on Windows,
            // where it arrives as a reset. One deployment error, two verdicts.
            //
            // `ResponseIo` carries the same kinds once a reply had begun, and
            // a certificate that cannot be read does not become readable
            // because the peer answered first.
            Self::Io(
                std::io::ErrorKind::PermissionDenied
                | std::io::ErrorKind::NotFound
                | std::io::ErrorKind::IsADirectory
                | std::io::ErrorKind::ReadOnlyFilesystem,
            )
            | Self::ResponseIo(
                std::io::ErrorKind::PermissionDenied
                | std::io::ErrorKind::NotFound
                | std::io::ErrorKind::IsADirectory
                | std::io::ErrorKind::ReadOnlyFilesystem,
            ) => "source.endpoint.unreadable_material",
            _ => "infrastructure.runtime.source_unavailable",
        }
    }

    fn from_transport(error: &ureq::Error) -> Self {
        match error {
            ureq::Error::StatusCode(status) => Self::HttpStatus(*status),
            // Split by phase. Collapsing every deadline into one made a body
            // read that expired after the peer answered look like a peer that
            // never did, so a signal arriving later erased the outage.
            // Only the body read happens after the peer answered. Waiting for
            // a response, sending the request, and waiting on a continue are
            // all deadlines that expire with nothing received — a peer that
            // accepted the connection and then said nothing has not answered,
            // so a cancellation still explains them.
            ureq::Error::Timeout(ureq::Timeout::RecvBody) => Self::ResponseTimeout,
            ureq::Error::Timeout(_) => Self::Timeout,
            ureq::Error::HostNotFound => Self::HostNotFound,
            // Every TLS-layer failure, not just the one variant. An expired or
            // untrusted certificate arrives as `Rustls`, which fell through to
            // the catch-all and was then treated as a peer that never answered.
            // Every TLS-layer failure this build can produce, including the
            // one that is really the certificate material failing to parse.
            // All of them mean this endpoint's identity cannot be established,
            // which is a person's problem rather than a moment's. The
            // remaining TLS variants belong to backends this build does not
            // enable, so naming them would not compile.
            ureq::Error::Tls(_)
            | ureq::Error::Rustls(_)
            | ureq::Error::TlsRequired
            | ureq::Error::Pem(_) => Self::Tls,
            // Settled facts about how the request was configured: a URL that
            // will not parse, a proxy address that will not parse, a redirect
            // that cannot be followed or never ends. These reached the
            // catch-all, where a pending shutdown made them look like a
            // cancellation and an orchestrator re-queued a batch that cannot
            // succeed until the configuration changes.
            ureq::Error::BadUri(_)
            | ureq::Error::Http(_)
            | ureq::Error::InvalidProxyUrl
            | ureq::Error::RedirectFailed
            | ureq::Error::TooManyRedirects => Self::Unroutable,
            ureq::Error::ConnectProxyFailed(_) => Self::ProxyConnection,
            ureq::Error::ConnectionFailed => Self::Connection,
            ureq::Error::Protocol(_) => Self::Protocol,
            ureq::Error::BodyExceedsLimit(_) => Self::BodyLimit,
            ureq::Error::Io(error) => Self::Io(error.kind()),
            _ => Self::Transport,
        }
    }

    fn as_str(self) -> String {
        match self {
            Self::HttpStatus(status) => format!("http_status_{status}"),
            Self::Timeout => "timeout".to_owned(),
            Self::ResponseTimeout => "response_timeout".to_owned(),
            Self::ResponseIo(kind) => format!("response_io_{kind:?}").to_lowercase(),
            Self::HostNotFound => "host_not_found".to_owned(),
            Self::Tls => "tls".to_owned(),
            Self::ProxyConnection => "proxy_connection".to_owned(),
            Self::Connection => "connection".to_owned(),
            Self::Protocol | Self::Unroutable => "protocol".to_owned(),
            Self::BodyLimit => "body_limit".to_owned(),
            Self::Io(kind) => format!("io_{kind:?}").to_ascii_lowercase(),
            Self::Transport => "transport".to_owned(),
        }
    }

    fn retryable_body_read(self) -> bool {
        matches!(
            self,
            Self::Timeout
                | Self::ResponseTimeout
                | Self::Connection
                | Self::ResponseIo(
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::WouldBlock
                )
                | Self::Io(
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::WouldBlock
                )
        )
    }
}

/// One fetched page: the raw body bytes plus the parsed `rel="next"`
/// link URL (Link-header strategy only).
struct PageResponse {
    body: Vec<u8>,
    next_link: Option<AuthorizedUrl>,
}

impl RecordSource for RestRecordSource {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.output_schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        loop {
            if let Some(max) = self.cfg.max_records
                && self.records_emitted >= max
            {
                return Ok(None);
            }
            // Stop promptly when shutdown is requested — a clean None,
            // the EOF-style stop the dispatch loop expects.
            if self.shutdown.as_ref().is_some_and(|t| t.is_requested())
                && self.current_page.is_none()
            {
                return Ok(None);
            }
            match self.current_page.as_mut() {
                Some(page) => match page.next_record()? {
                    Some(rec) => {
                        self.records_emitted += 1;
                        self.current_page_records = self.current_page_records.saturating_add(1);
                        return Ok(Some(rec));
                    }
                    None => {
                        // Page drained — settle the offset cursor from the
                        // rows it actually produced, then drop it and try
                        // the next page.
                        self.on_page_drained();
                        self.current_page = None;
                    }
                },
                None => {
                    if !self.fetch_next_page()? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    fn prepare_document(
        &mut self,
        _config: &EnvelopeConfig,
    ) -> Result<IndexMap<Box<str>, Value>, FormatError> {
        // Envelope sections span a whole document; a paginated REST pull
        // has no single document envelope, so it carries none.
        Ok(IndexMap::new())
    }

    fn set_shutdown_token(&mut self, token: ShutdownToken) {
        self.shutdown = Some(token);
    }
}

/// Build the source's output schema from its declared columns and the
/// `on_unmapped` policy, mirroring [`CoercingReader`]'s own output schema:
/// the authored columns followed by the `$widened` engine-stamped sidecar
/// column when the policy reserves it (`AutoWiden`).
fn build_output_schema(schema_decl: &[Column], on_unmapped: &OnUnmapped) -> Arc<Schema> {
    let mut builder = SchemaBuilder::with_capacity(schema_decl.len() + 1);
    for c in schema_decl {
        builder = builder.with_field(c.name.as_str());
    }
    if on_unmapped.reserves_widened_sidecar() {
        builder = builder.with_field_meta(WIDENED_SIDECAR_COLUMN, FieldMetadata::widened_sidecar());
    }
    builder.build()
}

/// A source's multi-value read declarations, resolved once at construction:
/// the schema columns declared `multiple: true` plus the source's
/// `split_to_rows` / `split_values` entries. Held on the source so every page's
/// body decodes through the same declaration.
struct MultiValueRead {
    fields: Vec<String>,
    split_to_rows: Vec<clinker_format::SplitToRows>,
    split_values: Vec<clinker_format::SplitValues>,
}

impl MultiValueRead {
    fn from_source(source: &SourceConfig, schema_decl: &[Column]) -> Self {
        MultiValueRead {
            fields: schema_decl
                .iter()
                .filter(|c| c.is_multiple())
                .map(|c| c.physical_name().to_string())
                .collect(),
            split_to_rows: source.split_to_rows.clone().unwrap_or_default(),
            split_values: source.split_values.clone().unwrap_or_default(),
        }
    }
}

/// Decode a response body into a byte-stream format reader. Only the
/// multi-record byte formats (`json`/`xml`) are valid for REST bodies;
/// the config validator (E220) rejects others before this runs.
fn decode_body(
    format: &InputFormat,
    multi_value: &MultiValueRead,
    body: Vec<u8>,
) -> Result<Box<dyn FormatReader>, FormatError> {
    let cursor = Cursor::new(body);
    match format {
        InputFormat::Json(opts) => {
            let config = build_json_config(opts.as_ref(), multi_value);
            Ok(Box::new(
                clinker_format::json::reader::JsonReader::from_reader(cursor, config)?,
            ))
        }
        InputFormat::Xml(opts) => {
            let config = build_xml_config(opts.as_ref(), multi_value);
            Ok(Box::new(
                clinker_format::xml::reader::XmlReader::from_reader(cursor, config)?,
            ))
        }
        other => Err(schema_err(format!(
            "rest body decode requires json or xml format, got {}",
            other.format_name()
        ))),
    }
}

/// Read a string value at a JSON pointer (RFC 6901) in a body. Returns
/// `None` when the path is absent or the value is null — the
/// cursor-token "no more pages" signal.
fn read_json_pointer_string(body: &[u8], pointer: &str) -> Result<Option<String>, FormatError> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| schema_err(format!("rest cursor body is not valid JSON: {e}")))?;
    match value.pointer(pointer) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
        Some(other) => Ok(Some(other.to_string())),
    }
}

/// Append query parameters to a URL, choosing `?` or `&` based on whether
/// the URL already has a query string. Values are percent-encoded for the
/// reserved characters that commonly appear in tokens.
fn append_query(base: &str, params: &[(&str, &str)]) -> String {
    let mut url = base.to_string();
    let mut sep = if url.contains('?') { '&' } else { '?' };
    for (k, v) in params {
        url.push(sep);
        url.push_str(k);
        url.push('=');
        url.push_str(&percent_encode(v));
        sep = '&';
    }
    url
}

/// Minimal percent-encoding for query-parameter values: encodes the
/// characters that would otherwise break the query string. ASCII
/// alphanumerics and the unreserved set pass through.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
        }
    }
    out
}

fn build_json_config(
    opts: Option<&clinker_plan::config::JsonInputOptions>,
    multi_value: &MultiValueRead,
) -> clinker_format::json::reader::JsonReaderConfig {
    use clinker_format::json::reader::{JsonMode, JsonReaderConfig};
    let mut config = JsonReaderConfig::default();
    if let Some(opts) = opts {
        config.format = opts.format.as_ref().map(|f| match f {
            clinker_plan::config::JsonFormat::Array => JsonMode::Array,
            clinker_plan::config::JsonFormat::Ndjson => JsonMode::Ndjson,
            clinker_plan::config::JsonFormat::Object => JsonMode::Object,
        });
        config.record_path = opts.record_path.clone();
    }
    config.multi_value_fields = multi_value.fields.clone();
    config.split_to_rows = multi_value.split_to_rows.clone();
    config.split_values = multi_value.split_values.clone();
    config
}

/// Build the XML reader config for a REST body.
///
/// Mirrors the file-source XML config build: every declared option the reader
/// takes is carried, so a source that moves between a file path and a REST
/// endpoint reads its documents the same way. The multi-value declarations in
/// particular have to be carried — `E361` rates `xml` as a format that produces
/// repetition natively and so never fires on a REST XML source, and a dropped
/// `multiple: true` would leave the planner binding an array against a reader
/// delivering the first occurrence as a bare scalar.
///
/// `max_index_bytes` is left at the reader default: the envelope pre-scan it
/// caps runs over a buffered document, and a REST body has no `$doc` envelope
/// to extract.
fn build_xml_config(
    opts: Option<&clinker_plan::config::XmlInputOptions>,
    multi_value: &MultiValueRead,
) -> clinker_format::xml::reader::XmlReaderConfig {
    use clinker_format::xml::reader::{NamespaceMode, XmlReaderConfig};
    let mut config = XmlReaderConfig::default();
    if let Some(opts) = opts {
        config.record_path = opts.record_path.clone();
        if let Some(ref prefix) = opts.attribute_prefix {
            config.attribute_prefix = prefix.clone();
        }
        config.namespace_handling = match opts.namespace_handling {
            Some(clinker_plan::config::NamespaceHandling::Qualify) => NamespaceMode::Qualify,
            _ => NamespaceMode::Strip,
        };
    }
    config.multi_value_fields = multi_value.fields.clone();
    config.split_to_rows = multi_value.split_to_rows.clone();
    config.split_values = multi_value.split_values.clone();
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Which deadlines mean the peer never answered.
    ///
    /// The cancellation rule turns entirely on this, and getting it backwards
    /// is silent: every behaviour test happened to exercise only `Connect` and
    /// `RecvBody`, the two that were classified correctly, so an inversion of
    /// the other three passed the whole suite. Each phase is named here so a
    /// future edit has to state which side it belongs on.
    #[test]
    fn only_the_body_read_happens_after_the_peer_answered() {
        for phase in [
            ureq::Timeout::Connect,
            ureq::Timeout::Resolve,
            ureq::Timeout::SendRequest,
            ureq::Timeout::SendBody,
            ureq::Timeout::Await100,
            ureq::Timeout::RecvResponse,
            ureq::Timeout::Global,
        ] {
            let failure = RequestFailure::from_transport(&ureq::Error::Timeout(phase));
            assert!(
                failure.is_cancellable_transport(),
                "{phase:?} expires with nothing received, so a cancellation explains it"
            );
        }

        let body = RequestFailure::from_transport(&ureq::Error::Timeout(ureq::Timeout::RecvBody));
        assert!(
            !body.is_cancellable_transport(),
            "a body read follows an answer, so the peer's verdict stands"
        );

        // Not only deadlines. Covering just the timeout variants is what let a
        // permanent TLS failure reach the catch-all and be read as a peer that
        // never answered.
        for permanent in [
            RequestFailure::Tls,
            RequestFailure::HttpStatus(503),
            RequestFailure::Unroutable,
            RequestFailure::BodyLimit,
            RequestFailure::HostNotFound,
        ] {
            assert!(
                !permanent.is_cancellable_transport(),
                "{permanent:?} is a verdict the peer gave or material this run \
                 supplied, and a shutdown alongside it explains neither"
            );
        }

        // The catch-all is the opposite case: a failure this mapping cannot
        // name. A dropped connection mid-request lands here on some hosts and
        // as an I/O error on others, so refusing it reported one operator
        // action two ways depending on the kernel.
        assert!(
            RequestFailure::Transport.is_cancellable_transport(),
            "an unnamed transport failure under a pending shutdown is that shutdown"
        );
    }

    #[test]
    fn a_request_the_client_could_not_route_is_never_a_cancellation() {
        // Both spellings of "no reply parsed" reach the cancellation rule with
        // the peer having said nothing, so the rule cannot tell a dropped
        // connection from a malformed URL by the error kind alone. It tells
        // them apart because they are different variants — they were the same
        // one until this split, and a run cancelled while pointed at a bad URI
        // reported a clean abort instead of naming the configuration.
        assert!(RequestFailure::Protocol.is_cancellable_transport());
        assert!(!RequestFailure::Unroutable.is_cancellable_transport());
        assert_eq!(
            RequestFailure::Unroutable.classification_code(),
            RequestFailure::Protocol.classification_code(),
            "the split is about what a shutdown explains, not about what the \
             run is told went wrong",
        );
    }

    /// Relabelling a failure by phase must not change whether it can be
    /// retried. Moving body-read failures onto their own variant silently
    /// dropped three kinds, so a transient that used to survive a retry began
    /// failing the run and discarding everything already pulled.
    #[test]
    fn a_phase_label_never_changes_what_is_retryable() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::WouldBlock,
        ] {
            let request_phase = RequestFailure::Io(kind);
            assert_eq!(
                request_phase.retryable_body_read(),
                request_phase.in_response_phase().retryable_body_read(),
                "{kind:?} must be equally retryable whichever phase observed it"
            );
        }
    }

    #[test]
    fn append_query_picks_separator() {
        assert_eq!(append_query("http://h/r", &[("a", "1")]), "http://h/r?a=1");
        assert_eq!(
            append_query("http://h/r?x=9", &[("a", "1")]),
            "http://h/r?x=9&a=1"
        );
    }

    #[test]
    fn append_query_percent_encodes_token() {
        assert_eq!(
            append_query("http://h/r", &[("c", "a b/c")]),
            "http://h/r?c=a%20b%2Fc"
        );
    }

    #[test]
    fn read_json_pointer_string_reads_and_stops() {
        let body = br#"{"meta":{"next":"tok-42"},"data":[]}"#;
        assert_eq!(
            read_json_pointer_string(body, "/meta/next")
                .unwrap()
                .as_deref(),
            Some("tok-42")
        );
        let body2 = br#"{"meta":{"next":null},"data":[]}"#;
        assert_eq!(read_json_pointer_string(body2, "/meta/next").unwrap(), None);
        let body3 = br#"{"data":[]}"#;
        assert_eq!(read_json_pointer_string(body3, "/meta/next").unwrap(), None);
    }

    /// An `xml` format carrying a record path, the shape a REST XML source
    /// declares in practice.
    fn xml_at(record_path: &str) -> InputFormat {
        InputFormat::Xml(Some(clinker_plan::config::XmlInputOptions {
            record_path: Some(record_path.to_string()),
            ..Default::default()
        }))
    }

    /// A REST XML body has to honor the source's multi-value declarations. The
    /// gate that would otherwise catch the mismatch (E361) rates `xml` as a
    /// format producing repetition natively, so it never fires on this
    /// transport — and a dropped declaration would leave the planner binding an
    /// array against a reader that collapses repeats to the first value.
    #[test]
    fn rest_xml_body_honors_the_sources_multi_value_declarations() {
        use clinker_record::Value;

        let multi_value = MultiValueRead {
            fields: vec!["Tag".to_string()],
            split_to_rows: Vec::new(),
            split_values: Vec::new(),
        };
        let body = br#"<Orders><Order><id>1</id><Tag>a</Tag><Tag>b</Tag></Order></Orders>"#;
        let mut reader =
            decode_body(&xml_at("Orders/Order"), &multi_value, body.to_vec()).expect("decode");
        let _schema = reader.schema().expect("schema");
        let rec = reader.next_record().expect("read").expect("one record");
        assert_eq!(
            rec.get("Tag"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into())
            ])),
            "repeated elements must collect into the declared array"
        );
    }

    /// The fan-out declaration is carried over the same transport.
    #[test]
    fn rest_xml_body_honors_split_to_rows() {
        let multi_value = MultiValueRead {
            fields: Vec::new(),
            split_to_rows: vec![clinker_format::SplitToRows::bare("Item")],
            split_values: Vec::new(),
        };
        let body =
            br#"<Orders><Order><id>1</id><Item><sku>x</sku></Item><Item><sku>y</sku></Item></Order></Orders>"#;
        let mut reader =
            decode_body(&xml_at("Orders/Order"), &multi_value, body.to_vec()).expect("decode");
        let _schema = reader.schema().expect("schema");
        let mut count = 0;
        while reader.next_record().expect("read").is_some() {
            count += 1;
        }
        assert_eq!(count, 2, "each `Item` occurrence becomes its own record");
    }

    #[test]
    fn continuation_classification_preserves_the_registered_code() {
        let error = continuation_format_error(ContinuationError::for_code(
            "rest.protocol.malformed_continuation",
        ));
        assert_eq!(
            error.classification_code(),
            Some("rest.protocol.malformed_continuation")
        );
    }

    #[test]
    fn page_body_limit_is_a_policy_required_protocol_failure() {
        use clinker_core_types::{FailureCategory, FailureClassification, RetryAdvice};

        let transport_error = ureq::Error::BodyExceedsLimit(MAX_PAGE_BYTES);
        let request_failure = RequestFailure::from_transport(&transport_error);
        assert!(matches!(request_failure, RequestFailure::BodyLimit));

        let code = request_failure.classification_code();
        assert_eq!(code, "rest.protocol.page_body_limit_reached");
        let classification =
            FailureClassification::for_code(code).expect("registered page body limit");
        assert_eq!(classification.category(), FailureCategory::SourceProtocol);
        assert_eq!(classification.retry_advice(), RetryAdvice::PolicyRequired);
    }
}
