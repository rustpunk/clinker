//! SWIFT MT (FIN) message reader/writer pair.
//!
//! A SWIFT MT message is a sequence of brace-balanced blocks
//! `{1:...}{2:...}{3:...}{4:...-}{5:...}`. Block 1 is the basic header, block
//! 2 the application header, block 3 the (optional) user header, block 4 the
//! message text, and block 5 the (optional) trailer. Blocks 3 and 5 may
//! carry nested `{tag:value}` sub-blocks tracked by brace depth; block 4
//! holds the message body as opaque line-structured free text whose values
//! legitimately contain `{`, `}`, and even `-}`, so it is not brace-counted
//! at all — it closes only on a line-anchored `-}` trailer (a `-}` that
//! begins a line).
//!
//! Unlike the flat EDI formats (HL7 v2, X12, EDIFACT) SWIFT framing is
//! brace-balanced, not terminator-delimited, so the block framer is
//! hand-rolled on brace depth rather than reusing the shared segment
//! tokenizer. Only the block-4 tag-line layer resembles a terminator scan.
//!
//! The reader maps one block-4 `:tag:value` line to one
//! [`crate::traits::FormatReader`] record under a static positional schema
//! (`block`, `tag`, `value`) — the same one-line-one-record shape as the X12
//! and HL7 readers, so memory scales O(1) with message size. The service
//! blocks (1/2/3/5) are consumed by the reader to serve file-level `$doc`
//! envelope sections and drive one balanced message-level document level;
//! they are never emitted as body records.
//!
//! The writer inverts the reader exactly: it re-frames the service blocks
//! `{<id>:<body>}` (body verbatim, no escaping) around the block-4 records it
//! re-emits as `:tag:value` lines, closing block 4 with the line-anchored
//! `-}` trailer. Block-4 free text is opaque, so values are written with zero
//! escaping; the reader strips exactly the structural separators the writer
//! re-adds, making the reader → writer → reader round-trip byte-faithful.

pub mod reader;
mod tokenizer;
pub mod writer;

pub use reader::{SwiftReader, SwiftReaderConfig};
pub use writer::{SwiftWriter, SwiftWriterConfig};

/// The default `$doc` section name for the basic header (block 1) when the
/// source declares no `envelope:` mapping for it. User-chosen names override
/// it; the engine reserves none — this is a stable label, not a keyword.
pub(crate) const DEFAULT_BASIC_HEADER_SECTION: &str = "basic_header";

/// The default `$doc` section name for the application header (block 2).
pub(crate) const DEFAULT_APP_HEADER_SECTION: &str = "app_header";

/// The default `$doc` section name for the user header (block 3).
pub(crate) const DEFAULT_USER_HEADER_SECTION: &str = "user_header";

/// The default `$doc` section name for the trailer (block 5).
pub(crate) const DEFAULT_TRAILER_SECTION: &str = "trailer";

/// The `body` field name a service block's verbatim text surfaces under
/// inside its `$doc` section. SWIFT service blocks carry free-form text (a
/// header string, nested `{sub:tag}` blocks), so the whole block body is one
/// addressable field rather than positional elements. The reader writes it;
/// the writer reads it back to echo a service block from `$doc`.
pub(crate) const BODY_FIELD: &str = "body";
