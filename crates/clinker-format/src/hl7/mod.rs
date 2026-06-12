//! HL7 v2.x reader/writer pair (pipe-and-hat encoding).
//!
//! An HL7 v2 message is a flat, delimiter-structured stream of segments
//! terminated by a carriage return. Each segment splits into a tag and
//! data fields on the field separator; a field splits into components on
//! the component separator, repetitions on the repetition separator, and
//! sub-components on the sub-component separator. Unlike X12's fixed
//! 106-byte `ISA`, HL7 declares its delimiters in the variable-length
//! `MSH` header: the field separator is the single byte immediately after
//! the `MSH` tag (`MSH-1`), and the encoding characters (`MSH-2`) are the
//! four bytes that follow it — component (`^`), repetition (`~`), escape
//! (`\`), and sub-component (`&`), in that fixed order. The segment
//! terminator is always a carriage return (`\r`), never producer-chosen.
//!
//! This module maps one body segment to one [`crate::traits::FormatReader`]
//! record under a static positional schema (`seg_id`, `set_ref`,
//! `set_type`, `f01..`). The `MSH` header that opens a message **is**
//! emitted as a body record (its fields carry the message's metadata);
//! optional batch/file envelope segments (`BHS`/`BTS`, `FHS`/`FTS`) are
//! consumed by the reader to validate batch/file counts inline and drive
//! nested document levels — they are never emitted as body records. The
//! writer mirrors this, reconstructing the envelope around emitted records
//! and recomputing every count.
//!
//! The optional envelope tiers surface as nested document-context levels:
//! an `FHS` file header is the file-level document (extracted in the
//! one-time pre-scan), and each `BHS` batch and `MSH` message opens a
//! nested level whose `$doc` sections layer over the enclosing tiers. All
//! tiers are optional — a bare stream of `MSH` messages with no batch or
//! file wrapping is a valid HL7 v2 file.
//!
//! Memory model: only the file header (`FHS`) is pre-scanned and retained
//! (so `$doc` envelope sections over `FHS` cost O(FHS)); the body streams
//! one segment at a time. The whole file is never buffered.

pub mod reader;
mod tokenizer;
pub mod writer;

pub use reader::{Hl7Reader, Hl7ReaderConfig};
pub use writer::{Hl7Writer, Hl7WriterConfig};

/// Engine-internal key under which the reader stashes the complete,
/// ordered `MSH` data-field list in the `transaction_set` `$doc` section,
/// for lossless writer reconstruction of an echoed `MSH` header. The `$`
/// prefix marks it as engine-namespaced so it is not a user-declarable
/// envelope field and never surfaces in CXL `$doc.<section>.<field>`
/// typechecking.
pub(crate) const RAW_FIELDS_KEY: &str = "$raw";
