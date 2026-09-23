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
//! and HL7 readers. The initial scan retains parsed body fields. The service
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
//!
//! Resource-free writer construction is unavailable at either module path:
//!
//! ```compile_fail,E0432
//! use clinker_format::swift::SwiftWriter;
//! let _: Option<SwiftWriter<Vec<u8>>> = None;
//! ```
//!
//! ```compile_fail,E0432
//! use clinker_format::swift::writer::SwiftWriter;
//! let _: Option<SwiftWriter<Vec<u8>>> = None;
//! ```
//!
//! Use finite resources for retained configuration and staged output:
//!
//! ```
//! use clinker_format::swift::writer::{SwiftEncoder, SwiftWriterConfig};
//! use clinker_format::preparation::{MemoryOnlyResources, PreparedWriter};
//! use clinker_format::FormatWriter;
//! use clinker_record::{Record, Schema, Value, owned_storage::SharedStorage};
//! use std::{num::NonZeroUsize, sync::Arc};
//! # fn main() -> Result<(), clinker_format::FormatError> {
//! let resources = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
//! let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["tag".into(), "value".into()])));
//! let encoder = SwiftEncoder::new(schema.clone(), &SwiftWriterConfig::default(), resources.resources())?;
//! let mut bytes = Vec::new();
//! {
//!     let mut writer = PreparedWriter::new(&mut bytes, encoder, resources.resources())?;
//!     writer.write_record(&Record::new(schema, vec![Value::String("79".into()), Value::String("one\r\ntwo".into())]))?;
//!     writer.flush()?;
//! }
//! assert_eq!(bytes, b"{4:\r\n:79:one\r\ntwo\r\n-}");
//! assert_eq!(resources.used(), 0);
//! # Ok(())
//! # }
//! ```

pub mod reader;
mod tokenizer;
pub mod writer;

pub use reader::{SwiftReader, SwiftReaderConfig};
pub use writer::{SwiftEncoder, SwiftEncoderConfig, SwiftWriterConfig};

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
