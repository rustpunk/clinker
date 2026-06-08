//! Leading UTF-8 byte-order-mark (BOM) handling for format readers.
//!
//! Excel's "CSV UTF-8" export and PowerShell `Out-File -Encoding utf8`
//! prepend a UTF-8 BOM (`U+FEFF`, bytes `EF BB BF`) to text files. Left
//! in place the BOM corrupts the first parsed token on every OS: a CSV
//! header cell becomes `\u{feff}id`, JSON auto-detect sees `0xEF` instead
//! of `[`/`{`, and `str::trim` leaves the BOM on the first NDJSON line
//! (`U+FEFF` does not carry the Unicode `White_Space` property). The
//! helpers here strip a single leading BOM once at the start of input.

use std::io::Read;

/// The UTF-8 encoding of `U+FEFF`, the byte-order mark.
pub const UTF8_BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

/// A `Read` adapter that transparently drops a single leading UTF-8 BOM
/// from the wrapped stream, then yields the remaining bytes verbatim.
///
/// Streaming readers (e.g. CSV) wrap their source in this so the BOM is
/// gone before any parsing begins, regardless of whether the first
/// downstream token is a header cell or a data field. The BOM may arrive
/// split across `read` calls, so the first read buffers up to three
/// prefix bytes to decide reliably before emitting anything.
pub struct SkipBom<R: Read> {
    inner: R,
    /// Prefix bytes already pulled from `inner` but not yet handed to the
    /// caller. After the one-time BOM check this holds any non-BOM prefix
    /// bytes that must be replayed; `head..` is the unconsumed slice.
    carry: Vec<u8>,
    head: usize,
    /// Whether the one-time leading-BOM check has run.
    checked: bool,
}

impl<R: Read> SkipBom<R> {
    /// Wraps `inner`, stripping a leading UTF-8 BOM on first read.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            carry: Vec::new(),
            head: 0,
            checked: false,
        }
    }

    /// Reads up to three prefix bytes from `inner` and, if they are the
    /// UTF-8 BOM, discards them; otherwise retains them for replay.
    fn check_bom(&mut self) -> std::io::Result<()> {
        let mut prefix = [0u8; 3];
        let mut filled = 0;
        while filled < prefix.len() {
            let n = self.inner.read(&mut prefix[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        let prefix = &prefix[..filled];
        if prefix != UTF8_BOM {
            self.carry.extend_from_slice(prefix);
        }
        self.checked = true;
        Ok(())
    }
}

impl<R: Read> Read for SkipBom<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if !self.checked {
            self.check_bom()?;
        }
        if self.head < self.carry.len() {
            let n = (self.carry.len() - self.head).min(out.len());
            out[..n].copy_from_slice(&self.carry[self.head..self.head + n]);
            self.head += n;
            return Ok(n);
        }
        self.inner.read(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn read_all<R: Read>(mut r: R) -> Vec<u8> {
        let mut v = Vec::new();
        r.read_to_end(&mut v).unwrap();
        v
    }

    #[test]
    fn skip_bom_drops_leading_marker() {
        let mut input = UTF8_BOM.to_vec();
        input.extend_from_slice(b"hello world");
        assert_eq!(read_all(SkipBom::new(&input[..])), b"hello world");
    }

    #[test]
    fn skip_bom_passes_non_bom_input_through() {
        assert_eq!(read_all(SkipBom::new(&b"hello world"[..])), b"hello world");
    }

    #[test]
    fn skip_bom_handles_bom_split_across_reads() {
        // A reader that yields one byte per `read` call must still have
        // its BOM detected and stripped — the adapter buffers the full
        // three-byte prefix before deciding.
        struct OneByteAtATime(std::io::Cursor<Vec<u8>>);
        impl Read for OneByteAtATime {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if buf.is_empty() {
                    return Ok(0);
                }
                self.0.read(&mut buf[..1])
            }
        }
        let mut input = UTF8_BOM.to_vec();
        input.extend_from_slice(b"payload");
        let src = OneByteAtATime(std::io::Cursor::new(input));
        assert_eq!(read_all(SkipBom::new(src)), b"payload");
    }

    #[test]
    fn skip_bom_preserves_partial_prefix_that_is_not_a_bom() {
        // First two bytes match the BOM but the third differs — none of
        // the three prefix bytes may be lost.
        let input = [0xEF, 0xBB, 0x21, b'a'];
        assert_eq!(read_all(SkipBom::new(&input[..])), input);
    }

    #[test]
    fn skip_bom_on_short_input_shorter_than_bom() {
        let input = [0xEFu8, 0xBB];
        assert_eq!(read_all(SkipBom::new(&input[..])), input);
    }

    #[test]
    fn skip_bom_on_empty_input() {
        assert_eq!(read_all(SkipBom::new(&b""[..])), b"");
    }
}
