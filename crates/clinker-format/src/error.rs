use std::fmt;

/// Errors produced by format readers and writers.
///
/// Streaming: errors are returned per-record, not buffered. The executor
/// decides whether to abort or skip based on the error strategy.
#[derive(Debug)]
#[non_exhaustive]
pub enum FormatError {
    Io(std::io::Error),
    Csv(csv::Error),
    Json(String),
    Xml(String),
    FixedWidth(String),
    /// A character-set failure shared across formats that decode declared
    /// non-UTF-8 text (CSV, X12): an unsupported `encoding` name, bytes that
    /// are not valid in the configured repertoire, or text a single-byte
    /// repertoire cannot encode. Carries a precise message naming the
    /// offending value and the supported set.
    Charset(String),
    /// An EDIFACT parse, envelope-count, or malformed-segment failure.
    /// Carries a precise message naming the offending control segment or
    /// constraint (bad UNA, UNT/UNZ count mismatch, control-ref echo
    /// mismatch, truncation, unsupported UNG/UNE grouping).
    Edifact(String),
    /// An X12 parse, envelope-count, or malformed-segment failure. Carries
    /// a precise message naming the offending control segment or
    /// constraint (truncated ISA header, SE/GE/IEA count mismatch,
    /// control-number echo mismatch, truncation, a delimiter byte in data
    /// that X12 cannot escape).
    X12(String),
    /// An HL7 v2 parse, batch/file-count, or malformed-segment failure.
    /// Carries a precise message naming the offending segment or
    /// constraint (missing/truncated MSH header, BTS/FTS count mismatch,
    /// unbalanced batch/file envelope, truncation, non-UTF-8 charset).
    Hl7(String),
    /// A SWIFT MT parse or malformed-block failure. Carries a precise
    /// message naming the offending block or constraint (unbalanced brace,
    /// missing/non-numeric block id, missing `-}` block-4 trailer,
    /// malformed `:tag:value` line, truncation, non-UTF-8 message).
    Swift(String),
    /// An envelope trailer's declared structural COUNT did not match the
    /// body the reader streamed: X12 `SE`/`GE`/`IEA`, EDIFACT `UNT`/`UNZ`,
    /// HL7 `BTS`/`FTS`, or a multi-record flat-file trailer. Distinct from
    /// the format-specific variants above (which also cover truncation, bad
    /// delimiters, and control-number echo mismatches) so a trailer count
    /// claim never reads as genuine corruption, and distinct from
    /// [`FormatError::StructuralValidation`] so a count mismatch stays
    /// distinguishable from a document that broke a non-count structural
    /// rule. Both structural classes route to the document-level DLQ under
    /// `dlq_granularity: document` (see
    /// [`FormatError::is_document_structural`]); genuine corruption keeps
    /// aborting the run. `format` names the spec for the DLQ stage label;
    /// `message` is the precise count-mismatch description the reader built
    /// at the trailer.
    StructuralCount {
        format: &'static str,
        message: String,
    },
    /// A multi-record flat file broke a structural rule that is NOT a
    /// trailer count claim: a line's record-type discriminator matched no
    /// declared `records:` entry (E345), or a body record appeared after
    /// the trailer that closes the document. Kept apart from
    /// [`FormatError::StructuralCount`] so callers can tell a malformed tag
    /// from a count mismatch, while both classes share the document-level
    /// DLQ disposition under `dlq_granularity: document` (see
    /// [`FormatError::is_document_structural`]). `format` names the spec
    /// for the DLQ stage label; `message` is the precise description the
    /// reader built at the offending line.
    StructuralValidation {
        format: &'static str,
        message: String,
    },
    InvalidRecord {
        row: u64,
        message: String,
    },
    SchemaInference(String),
    /// A source with `on_unmapped: reject` encountered an input record
    /// carrying a key the user did not declare in the source's
    /// `schema:` block.
    UndeclaredField {
        source: String,
        field: String,
    },
    /// A declared column aliases a physical source field (its `source_name`
    /// differs from its exposed name), but the input ALSO carries a real field
    /// named the same as the exposed name. Reading the alias would expose the
    /// physical field's value under the exposed name while the real field of
    /// that name is silently relocated (widened or dropped) — a data
    /// mislocation. Raised so the collision fails loudly instead. Resolve it by
    /// choosing an exposed name that does not clash with a real input field.
    AliasNameCollision {
        source: String,
        exposed: String,
        physical: String,
    },
    /// A non-JSON writer (CSV / XML / fixed-width) was handed a record
    /// carrying a `Value::Map` payload at a regular column slot.
    /// JSON writers serialize maps natively; the other formats have
    /// no canonical scalar serialization for a map and would silently
    /// either JSON-encode the map into a single cell (CSV/XML) or
    /// emit an empty field (fixed-width) — neither is the user's
    /// intent. Raise explicitly so the user sees the misroute.
    ///
    /// Routes to fix this at the user level:
    ///
    /// - If the column is the `$widened` `auto_widen` sidecar
    ///   absorber, set `include_unmapped: true` on the Output node;
    ///   the projection layer expands the map to top-level columns
    ///   before the record reaches the writer.
    /// - If the user explicitly emitted a map (rare), call a CXL
    ///   coercion that produces a scalar (`to_string` / equivalent)
    ///   before the emit.
    UnserializableMapValue {
        format: &'static str,
        column: String,
    },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Csv(e) => write!(f, "CSV error: {e}"),
            Self::Json(msg) => write!(f, "JSON error: {msg}"),
            Self::Xml(msg) => write!(f, "XML error: {msg}"),
            Self::FixedWidth(msg) => write!(f, "fixed-width error: {msg}"),
            Self::Charset(msg) => write!(f, "charset error: {msg}"),
            Self::Edifact(msg) => write!(f, "EDIFACT error: {msg}"),
            Self::X12(msg) => write!(f, "X12 error: {msg}"),
            Self::Hl7(msg) => write!(f, "HL7 error: {msg}"),
            Self::Swift(msg) => write!(f, "SWIFT error: {msg}"),
            Self::StructuralCount { format, message } => {
                write!(f, "{format} structural count error: {message}")
            }
            Self::StructuralValidation { format, message } => {
                write!(f, "{format} structural validation error: {message}")
            }
            Self::InvalidRecord { row, message } => {
                write!(f, "invalid record at row {row}: {message}")
            }
            Self::SchemaInference(msg) => write!(f, "schema inference failed: {msg}"),
            Self::UndeclaredField { source, field } => write!(
                f,
                "source {source:?}: input record carries undeclared field {field:?} \
                 (set `on_unmapped: drop` to silently strip, or declare the field)"
            ),
            Self::AliasNameCollision {
                source,
                exposed,
                physical,
            } => write!(
                f,
                "[E236] source {source:?}: column exposed as {exposed:?} aliases physical field \
                 {physical:?}, but the input also carries a real field named {exposed:?} — \
                 reading the alias would mislocate that field. Rename the exposed column to a \
                 name that does not clash with an input field."
            ),
            Self::UnserializableMapValue { format, column } => write!(
                f,
                "{format} writer cannot serialize a `Value::Map` payload at column {column:?}. \
                 If this is the `$widened` auto_widen sidecar, set `include_unmapped: true` on \
                 the Output node to expand the map to top-level columns before write. If the \
                 user emitted a map explicitly, coerce to a scalar in CXL before the emit."
            ),
        }
    }
}

impl FormatError {
    /// Build a [`FormatError::StructuralCount`] for an X12 envelope
    /// count mismatch (`SE`/`GE`/`IEA`).
    pub fn x12_structural_count(message: impl Into<String>) -> Self {
        Self::StructuralCount {
            format: "X12",
            message: message.into(),
        }
    }

    /// Build a [`FormatError::StructuralCount`] for an EDIFACT envelope
    /// count mismatch (`UNT`/`UNZ`).
    pub fn edifact_structural_count(message: impl Into<String>) -> Self {
        Self::StructuralCount {
            format: "EDIFACT",
            message: message.into(),
        }
    }

    /// Build a [`FormatError::StructuralCount`] for an HL7 batch/file
    /// envelope count mismatch (`BTS`/`FTS`).
    pub fn hl7_structural_count(message: impl Into<String>) -> Self {
        Self::StructuralCount {
            format: "HL7",
            message: message.into(),
        }
    }

    /// Build a [`FormatError::StructuralCount`] for a multi-record flat-file
    /// trailer count mismatch (a trailer record type's declared count against
    /// the streamed body count).
    pub fn multi_record_structural_count(message: impl Into<String>) -> Self {
        Self::StructuralCount {
            format: "multi-record",
            message: message.into(),
        }
    }

    /// Build a [`FormatError::StructuralValidation`] for a multi-record
    /// flat-file structural failure that is not a trailer count claim (an
    /// unknown record-type discriminator, a body record after the trailer
    /// that closes the document).
    pub fn multi_record_structural_validation(message: impl Into<String>) -> Self {
        Self::StructuralValidation {
            format: "multi-record",
            message: message.into(),
        }
    }

    /// `true` only for a [`FormatError::StructuralCount`] — an envelope
    /// trailer's declared count disagreeing with the body the reader
    /// streamed. For the document-DLQ routing decision use
    /// [`FormatError::is_document_structural`], which also admits
    /// [`FormatError::StructuralValidation`].
    pub fn is_structural_count(&self) -> bool {
        matches!(self, Self::StructuralCount { .. })
    }

    /// `true` for the error classes the executor reclassifies from
    /// run-aborting to document-DLQ under `dlq_granularity: document`: a
    /// trailer count mismatch ([`FormatError::StructuralCount`]) or a
    /// non-count structural failure of a multi-record document
    /// ([`FormatError::StructuralValidation`]). Every other variant keeps
    /// aborting the run.
    pub fn is_document_structural(&self) -> bool {
        matches!(
            self,
            Self::StructuralCount { .. } | Self::StructuralValidation { .. }
        )
    }
}

impl std::error::Error for FormatError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Csv(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for FormatError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<csv::Error> for FormatError {
    fn from(e: csv::Error) -> Self {
        Self::Csv(e)
    }
}
