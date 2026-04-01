//! Test helpers for clinker-core.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

/// Thread-safe, cloneable in-memory buffer for capturing output in tests.
///
/// Two clones of the same `SharedBuffer` share the underlying `Vec<u8>`,
/// so a writer thread and the test assertion side see the same data.
#[derive(Clone, Default)]
pub struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    /// Create a new empty buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a snapshot of the buffer contents.
    pub fn contents(&self) -> Vec<u8> {
        self.0.lock().unwrap().clone()
    }

    /// Return the buffer contents as a UTF-8 string. Panics if not valid UTF-8.
    pub fn as_string(&self) -> String {
        String::from_utf8(self.contents()).unwrap()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}
