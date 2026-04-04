//! Output file splitting — `SplittingWriter` with key-group-preserving rotation.
//!
//! Wraps a `FormatWriter` inside the writer thread. Uses factory pattern
//! to create new writer instances on rotation. Captures header from first
//! file and reuses for all splits.

#[cfg(test)]
mod tests;
