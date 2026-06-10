//! UN/EDIFACT reader/writer pair.
//!
//! EDIFACT is a flat, delimiter-structured interchange format. An
//! interchange opens with an optional `UNA` service-string advice and a
//! mandatory `UNB` header, wraps one or more `UNH..UNT` messages, and
//! closes with a `UNZ` trailer. Within a segment, data elements split on
//! the element separator (default `+`), components on the component
//! separator (default `:`); a release char (default `?`) escapes a
//! delimiter that occurs as literal data.
//!
//! This module maps one body segment to one [`crate::traits::FormatReader`]
//! record under a static positional schema (`seg_id`, `msg_ref`,
//! `msg_type`, `e01..`). Service segments are consumed by the reader to
//! validate `UNT`/`UNZ` control counts and control-reference echoes
//! inline; they are never emitted as body records. The writer mirrors
//! this, reconstructing the envelope around emitted records and
//! recomputing every control count.
//!
//! Memory model: only the interchange header is pre-scanned and retained
//! (so `$doc` envelope sections over `UNB` cost O(UNB)); the body streams
//! one segment at a time. The whole interchange is never buffered.

pub mod reader;
mod tokenizer;
pub mod writer;

pub use reader::{EdifactReader, EdifactReaderConfig};
pub use writer::{EdifactWriter, EdifactWriterConfig};

/// Engine-internal key under which the reader stashes the complete,
/// ordered `UNB` data-element list (empty middle elements included) in a
/// `$doc` envelope section, for lossless writer reconstruction via
/// `interchange_from_doc`. The `$` prefix marks it as engine-namespaced
/// so it is not a user-declarable envelope field and never surfaces in
/// CXL `$doc.<section>.<field>` typechecking.
pub(crate) const RAW_ELEMENTS_KEY: &str = "$raw";
