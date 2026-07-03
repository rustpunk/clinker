//! Network finite-pull Source readers for Clinker.
//!
//! The transport here is a synchronous client driven on the Source's own
//! `std::thread` (the ingest thread) to cursor exhaustion, then it exits —
//! the finite-pull model. No async runtime is involved: the `rest`
//! transport uses the blocking [`ureq`] client over rustls.
//!
//! The reader implements [`clinker_exec::source::RecordSource`] and is
//! handed to the executor as a [`clinker_exec::source::SourceInput::Records`],
//! so it feeds the identical crossbeam ingest channel as file sources with
//! no special-case dispatch path. Per-row coercion is lenient at the reader
//! (via the same `CoercingReader` the file arm uses), so per-row failures
//! route to the dead-letter queue at the Transform stage exactly like files.
//!
//! Finiteness is a HARD reader property: the REST transport caps its pull
//! with an explicit `max_pages`/`max_records` ceiling so an unbounded
//! endpoint cannot silently violate the finite-inputs pillar.

mod rest;

use clinker_format::FormatError;

use rest::RestRecordSource;

/// Build the REST record source for a `rest` Source from its declared
/// node config. The caller (the CLI reader-build) registers the returned
/// reader as a [`clinker_exec::source::SourceInput::Records`].
pub fn build_rest_source(
    cfg: clinker_plan::config::RestSourceConfig,
    source: &clinker_plan::config::SourceConfig,
    schema_decl: &[clinker_format::Column],
    on_unmapped: clinker_plan::config::pipeline_node::OnUnmapped,
) -> Result<Box<dyn clinker_exec::source::RecordSource>, FormatError> {
    let reader = RestRecordSource::new(cfg, source, schema_decl, on_unmapped)?;
    Ok(Box::new(reader))
}

/// Construct a `FormatError::Io` wrapping a REST read failure, tagged with
/// the `E221` diagnostic code. The ingest driver propagates a reader error
/// as a hard pipeline failure (a connect/HTTP/body-read failure is not a
/// per-row data error), so this is the right channel for those.
fn io_err(msg: String) -> FormatError {
    FormatError::Io(std::io::Error::other(format!("[E221] {msg}")))
}

/// Construct a `FormatError::SchemaInference` for a body-decode/setup
/// failure that is not an I/O error (malformed pagination body, etc.).
fn schema_err(msg: String) -> FormatError {
    FormatError::SchemaInference(msg)
}
