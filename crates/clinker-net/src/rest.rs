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
//! is mandatory and the reader stops at the cap even if the server keeps
//! offering a next page. The pull runs on its own `std::thread` (the
//! ingest thread) driving a blocking `ureq` client to exhaustion, with
//! no async runtime.

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

use crate::{io_err, schema_err};

/// Per-body cap. Each individual page body is bounded so a misbehaving
/// server cannot stream an unbounded body into one `read_to_vec`. 64 MiB
/// is generous for one page of a paginated API; the reader's overall
/// finiteness comes from `max_pages` / `max_records`.
const MAX_PAGE_BYTES: u64 = 64 * 1024 * 1024;

/// Synchronous paginated HTTP source. One per declared `rest` Source;
/// owned by that source's ingest thread. Constructed through
/// [`crate::build_rest_source`], which boxes it as a `dyn RecordSource`.
pub(crate) struct RestRecordSource {
    agent: ureq::Agent,
    cfg: RestSourceConfig,
    format: InputFormat,
    array_paths: Option<Vec<clinker_plan::config::ArrayPathConfig>>,
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
        next_url: Option<String>,
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
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(cfg.timeout_secs)))
            // Inspect status codes manually: 5xx is a retry, 4xx is fatal.
            .http_status_as_error(false)
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
            format: source.format.clone(),
            array_paths: source.array_paths.clone(),
            schema_decl: schema_decl.to_vec(),
            on_unmapped,
            source_name: source.name.clone(),
            output_schema,
            current_page: None,
            current_page_records: 0,
            next,
            pages_fetched: 0,
            records_emitted: 0,
            shutdown: None,
        })
    }

    /// Whether the page/record cap or a shutdown request forbids fetching
    /// another page.
    fn cap_reached(&self) -> bool {
        if self.pages_fetched >= self.cfg.max_pages {
            return true;
        }
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
        if self.cap_reached() {
            return Ok(false);
        }
        let url = match self.build_request_url() {
            Some(url) => url,
            None => return Ok(false),
        };

        let bytes = self.get_with_retry(&url)?;
        self.pages_fetched += 1;

        // Advance the cursor for the strategies whose continuation signal
        // is carried directly by this response (next token in the body,
        // `rel="next"` link in the headers). The offset strategy's signal
        // is the row count, which is only known once the page drains, so
        // its advance is deferred to `on_page_drained`.
        self.advance_cursor(&bytes)?;

        let reader = decode_body(&self.format, self.array_paths.as_deref(), bytes.body)?;
        let coercing = CoercingReader::new(
            reader,
            &self.schema_decl,
            self.on_unmapped.clone(),
            &self.source_name,
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
    fn build_request_url(&self) -> Option<String> {
        match &self.next {
            NextPage::Single { fetched } => (!fetched).then(|| self.cfg.url.clone()),
            NextPage::Offset {
                offset_param,
                limit_param,
                limit,
                offset,
                more,
            } => more.then(|| {
                append_query(
                    &self.cfg.url,
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
                Some(t) => append_query(&self.cfg.url, &[(cursor_param, t)]),
                None => self.cfg.url.clone(),
            }),
            NextPage::LinkHeader { next_url, more } => {
                more.then(|| next_url.clone().unwrap_or_else(|| self.cfg.url.clone()))
            }
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
    fn get_with_retry(&self, url: &str) -> Result<PageResponse, FormatError> {
        let mut attempt: u32 = 0;
        loop {
            if self.shutdown.as_ref().is_some_and(|t| t.is_requested()) {
                return Err(io_err(format!(
                    "rest source {:?}: shutdown requested mid-request",
                    self.source_name
                )));
            }
            let request = match &self.cfg.auth {
                RestAuth::None => self.agent.get(url),
                RestAuth::Bearer { token } => self
                    .agent
                    .get(url)
                    .header("Authorization", format!("Bearer {token}")),
                RestAuth::Header { name, value } => {
                    self.agent.get(url).header(name.as_str(), value.as_str())
                }
            };
            match request.call() {
                Ok(mut response) => {
                    let status = response.status().as_u16();
                    if (500..600).contains(&status) {
                        if attempt < self.cfg.retries {
                            attempt += 1;
                            continue;
                        }
                        return Err(io_err(format!(
                            "rest source {:?}: server error {status} after {} retries (url {url})",
                            self.source_name, self.cfg.retries
                        )));
                    }
                    if !(200..300).contains(&status) {
                        return Err(io_err(format!(
                            "rest source {:?}: HTTP {status} (url {url})",
                            self.source_name
                        )));
                    }
                    let next_link = response
                        .headers()
                        .get_all(ureq::http::header::LINK)
                        .iter()
                        .filter_map(|v| v.to_str().ok())
                        .find_map(parse_next_link);
                    let body = response
                        .body_mut()
                        .with_config()
                        .limit(MAX_PAGE_BYTES)
                        .read_to_vec()
                        .map_err(|e| {
                            io_err(format!(
                                "rest source {:?}: reading body: {e}",
                                self.source_name
                            ))
                        })?;
                    return Ok(PageResponse { body, next_link });
                }
                Err(e) => {
                    // Transport-level failure (connect / timeout / TLS).
                    // Transient — retry within the bound.
                    if attempt < self.cfg.retries {
                        attempt += 1;
                        continue;
                    }
                    return Err(io_err(format!(
                        "rest source {:?}: request failed after {} retries: {e} (url {url})",
                        self.source_name, self.cfg.retries
                    )));
                }
            }
        }
    }
}

/// One fetched page: the raw body bytes plus the parsed `rel="next"`
/// link URL (Link-header strategy only).
struct PageResponse {
    body: Vec<u8>,
    next_link: Option<String>,
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

/// Decode a response body into a byte-stream format reader. Only the
/// multi-record byte formats (`json`/`xml`) are valid for REST bodies;
/// the config validator (E220) rejects others before this runs.
fn decode_body(
    format: &InputFormat,
    array_paths: Option<&[clinker_plan::config::ArrayPathConfig]>,
    body: Vec<u8>,
) -> Result<Box<dyn FormatReader>, FormatError> {
    let cursor = Cursor::new(body);
    match format {
        InputFormat::Json(opts) => {
            let config = build_json_config(opts.as_ref(), array_paths);
            Ok(Box::new(
                clinker_format::json::reader::JsonReader::from_reader(cursor, config)?,
            ))
        }
        InputFormat::Xml(opts) => {
            let config = build_xml_config(opts.as_ref(), array_paths);
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

/// Parse the `rel="next"` URL out of one RFC 5988 `Link` header value.
/// A header value is a comma-separated list of `<url>; rel="..."`
/// entries; this returns the URL whose `rel` is `next`.
fn parse_next_link(header: &str) -> Option<String> {
    for entry in header.split(',') {
        let entry = entry.trim();
        let Some((url_part, params)) = entry.split_once(';') else {
            continue;
        };
        let url = url_part
            .trim()
            .trim_start_matches('<')
            .trim_end_matches('>');
        let is_next = params.split(';').any(|p| {
            let p = p.trim();
            p == "rel=\"next\"" || p == "rel=next"
        });
        if is_next {
            return Some(url.to_string());
        }
    }
    None
}

fn build_json_config(
    opts: Option<&clinker_plan::config::JsonInputOptions>,
    array_paths: Option<&[clinker_plan::config::ArrayPathConfig]>,
) -> clinker_format::json::reader::JsonReaderConfig {
    use clinker_format::json::reader::{ArrayPathMode, ArrayPathSpec, JsonMode, JsonReaderConfig};
    let mut config = JsonReaderConfig::default();
    if let Some(opts) = opts {
        config.format = opts.format.as_ref().map(|f| match f {
            clinker_plan::config::JsonFormat::Array => JsonMode::Array,
            clinker_plan::config::JsonFormat::Ndjson => JsonMode::Ndjson,
            clinker_plan::config::JsonFormat::Object => JsonMode::Object,
        });
        config.record_path = opts.record_path.clone();
    }
    if let Some(paths) = array_paths {
        config.array_paths = paths
            .iter()
            .map(|p| ArrayPathSpec {
                path: p.path.clone(),
                mode: match p.mode {
                    clinker_plan::config::ArrayMode::Explode => ArrayPathMode::Explode,
                    clinker_plan::config::ArrayMode::Join => ArrayPathMode::Join,
                },
                separator: p.separator.clone().unwrap_or_else(|| ",".to_string()),
            })
            .collect();
    }
    config
}

fn build_xml_config(
    opts: Option<&clinker_plan::config::XmlInputOptions>,
    array_paths: Option<&[clinker_plan::config::ArrayPathConfig]>,
) -> clinker_format::xml::reader::XmlReaderConfig {
    use clinker_format::xml::reader::XmlReaderConfig;
    let mut config = XmlReaderConfig::default();
    if let Some(opts) = opts {
        config.record_path = opts.record_path.clone();
    }
    let _ = array_paths;
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_next_link_extracts_rel_next() {
        let h = r#"<https://api.example.com/r?page=2>; rel="next", <https://api.example.com/r?page=5>; rel="last""#;
        assert_eq!(
            parse_next_link(h).as_deref(),
            Some("https://api.example.com/r?page=2")
        );
    }

    #[test]
    fn parse_next_link_absent_when_no_next() {
        let h = r#"<https://api.example.com/r?page=1>; rel="prev""#;
        assert_eq!(parse_next_link(h), None);
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
}
