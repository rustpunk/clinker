//! ANSI ASC X12 reader/writer pair.
//!
//! X12 is a flat, delimiter-structured interchange format with a
//! three-tier envelope: an `ISA..IEA` interchange wraps one or more
//! `GS..GE` functional groups, each of which wraps one or more `ST..SE`
//! transaction sets. Unlike EDIFACT's optional `UNA`, X12 declares its
//! delimiters in a fixed-length 106-byte `ISA` header — the element
//! separator is the byte after the `ISA` tag, the sub-element separator is
//! `ISA16`, and the segment terminator is the byte after `ISA16`. X12 has
//! no release/escape character.
//!
//! This module maps one body segment to one [`crate::traits::FormatReader`]
//! record under a static positional schema (`seg_id`, `set_ref`,
//! `set_type`, `e01..`). Service segments (`ISA`/`IEA`/`GS`/`GE`/`ST`/`SE`)
//! are consumed by the reader to validate control counts inline and drive
//! the three-level envelope; they are never emitted as body records. The
//! writer mirrors this, reconstructing the three envelope tiers around
//! emitted records and recomputing every control count.
//!
//! The three envelope tiers surface as nested document-context levels via
//! the multi-level envelope events introduced in the engine's document
//! context: the `ISA` interchange is the file-level document (extracted in
//! the one-time pre-scan), and each `GS` group and `ST` set opens a nested
//! level whose `$doc` sections layer over the enclosing tiers. The X12
//! reader is the first producer of these nested-envelope events.
//!
//! Memory model: only the interchange header is pre-scanned and retained
//! (so `$doc` envelope sections over `ISA` cost O(ISA)); the body streams
//! one segment at a time. The whole interchange is never buffered.

pub mod reader;
mod tokenizer;
pub mod writer;

pub use crate::charset::Charset;
pub use reader::{X12Reader, X12ReaderConfig};
pub use writer::{X12Writer, X12WriterConfig};

/// Engine-internal key under which the reader stashes the complete,
/// ordered `ISA` data-element list in a `$doc` envelope section, for
/// lossless writer reconstruction via `interchange_from_doc`. The `$`
/// prefix marks it as engine-namespaced so it is not a user-declarable
/// envelope field and never surfaces in CXL `$doc.<section>.<field>`
/// typechecking.
pub(crate) const RAW_ELEMENTS_KEY: &str = "$raw";

/// Engine-internal key under which the reader stashes the discovered
/// delimiter set beside the raw `ISA` elements, as a three-element array of
/// integer bytes `[element, subelement, terminator]`. The element separator
/// and segment terminator are not `ISA` data elements — they live between
/// and after them — so the raw element list alone cannot reproduce a
/// custom-delimited wire shape; the writer adopts this set when echoing the
/// header via `interchange_from_doc`. Engine-namespaced like
/// [`RAW_ELEMENTS_KEY`]: not user-declarable and invisible to CXL `$doc`
/// typechecking.
pub(crate) const DELIMITERS_KEY: &str = "$delims";
