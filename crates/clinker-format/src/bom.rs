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
/// prefix bytes to decide reliably before emitting anything. These bytes
/// remain buffered across I/O errors; interrupted prefix reads are retried.
/// Prefix storage is fixed at three bytes and requires no allocation.
pub struct SkipBom<R: Read> {
    inner: R,
    /// Prefix bytes already pulled from `inner` but not yet handed to the
    /// caller, including a partial prefix when a read fails. After the
    /// one-time BOM check, `head..filled` is the non-BOM slice to replay.
    prefix: [u8; 3],
    filled: usize,
    head: usize,
    /// Whether the one-time leading-BOM check has run.
    checked: bool,
}

impl<R: Read> SkipBom<R> {
    /// Wraps `inner`, stripping a leading UTF-8 BOM on first read.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            prefix: [0; 3],
            filled: 0,
            head: 0,
            checked: false,
        }
    }

    /// Reads up to three prefix bytes from `inner` and, if they are the
    /// UTF-8 BOM, discards them; otherwise retains them for replay.
    fn check_bom(&mut self) -> std::io::Result<()> {
        while self.filled < self.prefix.len() {
            match self.inner.read(&mut self.prefix[self.filled..]) {
                Ok(0) => break,
                Ok(n) => self.filled += n,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        if self.prefix[..self.filled] == UTF8_BOM {
            self.head = self.filled;
        }
        self.checked = true;
        Ok(())
    }
}

impl<R: Read> Read for SkipBom<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if !self.checked {
            self.check_bom()?;
        }
        if self.head < self.filled {
            let n = (self.filled - self.head).min(out.len());
            out[..n].copy_from_slice(&self.prefix[self.head..self.head + n]);
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

    struct FaultReader<'a> {
        input: &'a [u8],
        position: usize,
        fault: Option<(usize, std::io::ErrorKind)>,
        calls: usize,
    }

    impl<'a> FaultReader<'a> {
        fn new(input: &'a [u8], fault: Option<(usize, std::io::ErrorKind)>) -> Self {
            Self {
                input,
                position: 0,
                fault,
                calls: 0,
            }
        }
    }

    impl Read for FaultReader<'_> {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            self.calls += 1;
            if let Some((position, kind)) = self.fault
                && self.position == position
            {
                self.fault = None;
                return Err(kind.into());
            }
            let boundary = self
                .fault
                .map_or(self.input.len(), |(position, _)| position);
            let chunk = [1, 2, 1, 3][(self.calls - 1) % 4];
            let n = out.len().min(chunk).min(boundary - self.position);
            out[..n].copy_from_slice(&self.input[self.position..self.position + n]);
            self.position += n;
            Ok(n)
        }
    }

    #[test]
    fn skip_bom_retries_interrupted_at_each_prefix_position() {
        for position in 0..3 {
            let source = FaultReader::new(
                b"\xEF\xBB\xBFvalue",
                Some((position, std::io::ErrorKind::Interrupted)),
            );
            assert_eq!(read_all(SkipBom::new(source)), b"value", "{position}");
        }
    }

    #[test]
    fn skip_bom_retries_interrupted_before_returning_prefix() {
        let source = FaultReader::new(b"abc", Some((1, std::io::ErrorKind::Interrupted)));
        let mut reader = SkipBom::new(source);
        let mut out = [0; 3];
        let n = reader.read(&mut out).unwrap();
        assert_eq!(&out[..n], b"abc");
    }

    #[test]
    fn skip_bom_preserves_prefix_after_propagated_error() {
        for input in [
            &b"\xEF\xBB\xBFvalue"[..],
            &b"abcvalue"[..],
            &b"\xEF\xBB!value"[..],
            &b"\xEF!\xBFvalue"[..],
        ] {
            for position in 0..=3 {
                let source =
                    FaultReader::new(input, Some((position, std::io::ErrorKind::ConnectionReset)));
                let mut reader = SkipBom::new(source);
                let mut out = Vec::new();
                assert_eq!(
                    reader.read_to_end(&mut out).unwrap_err().kind(),
                    std::io::ErrorKind::ConnectionReset,
                );
                reader.read_to_end(&mut out).unwrap();
                let expected = input.strip_prefix(&UTF8_BOM).unwrap_or(input);
                assert_eq!(out, expected, "{input:?}, error at {position}");
            }
        }
    }

    #[test]
    fn skip_bom_preserves_short_prefix_at_eof_after_errors() {
        for input in [&b""[..], &b"\xEF"[..], &b"\xEF\xBB"[..], &b"ab"[..]] {
            for position in 0..=input.len() {
                for kind in [
                    std::io::ErrorKind::Interrupted,
                    std::io::ErrorKind::ConnectionReset,
                ] {
                    let mut reader = SkipBom::new(FaultReader::new(input, Some((position, kind))));
                    let mut out = Vec::new();
                    if kind == std::io::ErrorKind::ConnectionReset {
                        assert_eq!(reader.read_to_end(&mut out).unwrap_err().kind(), kind);
                    }
                    reader.read_to_end(&mut out).unwrap();
                    assert_eq!(out, input, "{input:?}, {kind:?} at {position}");
                }
            }
        }
    }

    #[test]
    fn skip_bom_preserves_non_bom_prefix_after_interruptions() {
        for input in [&b"abc"[..], &b"\xEF!x"[..], &b"\xEF\xBB!"[..]] {
            for position in 0..3 {
                let source =
                    FaultReader::new(input, Some((position, std::io::ErrorKind::Interrupted)));
                assert_eq!(read_all(SkipBom::new(source)), input, "{position}");
            }
        }
    }

    #[test]
    fn skip_bom_strips_once_with_changing_input_and_output_chunks() {
        for input in [&b"\xEF\xBB\xBF\xEF\xBB\xBFvalue"[..], &b"a\xEF\xBB\xBF"[..]] {
            let mut reader = SkipBom::new(FaultReader::new(input, None));
            let mut out = Vec::new();
            for size in [1, 2, 4].into_iter().cycle() {
                let mut buffer = [0; 4];
                let n = reader.read(&mut buffer[..size]).unwrap();
                if n == 0 {
                    break;
                }
                out.extend_from_slice(&buffer[..n]);
            }
            assert_eq!(out, input.strip_prefix(&UTF8_BOM).unwrap_or(input));
        }
    }

    #[test]
    fn skip_bom_empty_output_does_not_touch_input() {
        let mut source = FaultReader::new(b"abcdef", None);
        let mut reader = SkipBom::new(&mut source);
        for _ in 0..2 {
            assert_eq!(reader.read(&mut []).unwrap(), 0);
        }
        assert_eq!(reader.inner.calls, 0);
        let mut byte = [0];
        assert_eq!(reader.read(&mut byte).unwrap(), 1);
        assert_eq!(byte, [b'a']);
        let calls = reader.inner.calls;
        assert_eq!(reader.read(&mut []).unwrap(), 0);
        assert_eq!(reader.inner.calls, calls);
        assert_eq!(read_all(&mut reader), b"bcdef");
        let calls = reader.inner.calls;
        assert_eq!(reader.read(&mut []).unwrap(), 0);
        assert_eq!(reader.inner.calls, calls);
    }
}
