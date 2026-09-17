//! CSV readers and finite-resource prepared writers.
//!
//! Writers use [`writer::CsvEncoder`] with explicit
//! [`crate::preparation::WriterResources`]. The former resource-free writer is
//! unavailable through either public module path:
//!
//! ```compile_fail,E0432
//! use clinker_format::csv::{CsvWriter, CsvWriterConfig};
//! use clinker_record::{Schema, owned_storage::SharedStorage};
//! let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec!["value".into()])));
//! let _writer = CsvWriter::new(Vec::<u8>::new(), schema, CsvWriterConfig::default());
//! ```
//!
//! ```compile_fail,E0432
//! use clinker_format::csv::writer::{CsvWriter, CsvWriterConfig};
//! use clinker_record::{Schema, owned_storage::SharedStorage};
//! let schema = SharedStorage::from_arc(std::sync::Arc::new(Schema::new(vec!["value".into()])));
//! let _writer = CsvWriter::new(Vec::<u8>::new(), schema, CsvWriterConfig::default());
//! ```

pub mod reader;
pub mod writer;

pub use reader::{CsvReader, CsvReaderConfig};
pub use writer::CsvWriterConfig;
