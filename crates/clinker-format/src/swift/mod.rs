//! SWIFT MT (FIN) message reader.
//!
//! A SWIFT MT message is a sequence of brace-balanced blocks
//! `{1:...}{2:...}{3:...}{4:...-}{5:...}`. Block 1 is the basic header, block
//! 2 the application header, block 3 the (optional) user header, block 4 the
//! message text, and block 5 the (optional) trailer. Blocks 3 and 5 may
//! carry nested `{tag:value}` sub-blocks; block 4 holds the message body as a
//! run of `:tag:value` lines closed by a `-}` trailer rather than a bare
//! brace, because its free text may contain a literal `}`.
//!
//! Unlike the flat EDI formats (HL7 v2, X12, EDIFACT) SWIFT framing is
//! brace-balanced, not terminator-delimited, so the block framer is
//! hand-rolled on brace depth rather than reusing the shared segment
//! tokenizer. Only the block-4 tag-line layer resembles a terminator scan.
//!
//! This module maps one block-4 `:tag:value` line to one
//! [`crate::traits::FormatReader`] record under a static positional schema
//! (`block`, `tag`, `value`) — the same one-line-one-record shape as the X12
//! and HL7 readers, so memory scales O(1) with message size. The service
//! blocks (1/2/3/5) are consumed by the reader to serve file-level `$doc`
//! envelope sections and drive one balanced message-level document level;
//! they are never emitted as body records.

pub mod reader;
mod tokenizer;

pub use reader::{SwiftReader, SwiftReaderConfig};
