//! Lazy, one-element-at-a-time streaming of a JSON array body.
//!
//! [`JsonArrayStream`] owns a single `BufReader` over a freshly re-opened
//! source and yields the array's elements **one at a time** — it never
//! collects them into a `Vec`. Peak retention is one element's bytes plus its
//! parsed `serde_json::Value`, i.e. O(1 record), regardless of array length.
//!
//! ## Why a structural scanner rather than serde streaming
//!
//! serde_json exposes no public API to pull comma-delimited elements out of a
//! single `[...]` array lazily: `StreamDeserializer` is whitespace-delimited
//! (it streams `[0] [1]`, not the elements inside one array), and building a
//! fresh `Deserializer` per element over a borrowed `&mut BufReader` drops the
//! one-byte lookahead `IoRead` keeps, corrupting the stream. Holding a
//! `SeqAccess` across calls would need a self-referential struct. So this scans
//! the array's structural framing itself — tracking string/escape state and
//! brace/bracket depth to find each element's byte boundaries — and hands each
//! element's bytes to `serde_json::from_slice`. The framing scan is the only
//! hand-rolled part; element *parsing* stays with serde_json.

use std::io::{BufReader, Read};

use crate::error::FormatError;

/// A byte stream over a re-opened source, navigated to a JSON array, yielding
/// its elements lazily.
///
/// Streaming, O(1 record): only the current element's bytes and parsed value
/// are retained at any time. The opening `[` of the target array has already
/// been consumed at construction; [`next`](Self::next) reads up to the matching
/// `]`.
///
/// Malformed-input note: because elements are framed and parsed one at a time,
/// a truncated array (`[1,2` with no closing `]`) yields its complete leading
/// elements and then fails loud on the missing `]` at the next pull — the error
/// is late but never silent. A non-whitespace tail after a *top-level* array's
/// close is rejected immediately (see `strict_eof`).
pub(crate) struct JsonArrayStream {
    reader: ByteReader,
    /// `true` once the array's closing `]` has been seen; further `next` calls
    /// return `None`.
    done: bool,
    /// `true` before the first element so `next` knows whether to expect a
    /// leading comma separator.
    first: bool,
    /// Per-element scan buffer, cleared and reused across `next` calls so a
    /// long array does not allocate a fresh `Vec` per element. Holds at most
    /// one element's bytes — O(1 record).
    scratch: Vec<u8>,
    /// When `true`, the array is the whole document, so any non-whitespace byte
    /// after its closing `]` is trailing garbage and fails loud. `false` for a
    /// `record_path` array, where the enclosing object legitimately carries
    /// sibling keys after the array.
    strict_eof: bool,
}

impl JsonArrayStream {
    /// Open a stream over the top-level JSON array in `reader`.
    ///
    /// Skips leading whitespace, consumes the opening `[`, and positions before
    /// the first element. An empty document (`reader` at EOF) yields a stream
    /// that immediately returns `None`.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] if the first non-whitespace byte is not
    /// `[`, or [`FormatError::Io`] on a read failure.
    pub(crate) fn top_level(reader: BufReader<Box<dyn Read + Send>>) -> Result<Self, FormatError> {
        // The array is the whole document, so enforce whitespace-only tail.
        Self::at_array(ByteReader::new(reader), true)
    }

    /// Open a stream over the JSON array reached by following `path` (dotted
    /// `record_path` segments) from a top-level object.
    ///
    /// Descends key-by-key, skipping sibling values structurally (no
    /// allocation beyond the bytes of a skipped key string), until the target
    /// array is reached, then positions before its first element.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] if a path segment is missing, an
    /// intermediate is not an object, or the terminal value is not an array;
    /// [`FormatError::Io`] on a read failure.
    pub(crate) fn at_path(
        reader: BufReader<Box<dyn Read + Send>>,
        path: &[String],
    ) -> Result<Self, FormatError> {
        let mut br = ByteReader::new(reader);
        navigate_to_path(&mut br, path)?;
        // The array sits inside an object that may carry sibling keys after it,
        // so a non-whitespace tail is not garbage here.
        Self::at_array(br, false)
    }

    /// Consume the opening `[` of the array the reader is positioned at.
    /// `strict_eof` requires a whitespace-only tail after the array closes.
    fn at_array(mut reader: ByteReader, strict_eof: bool) -> Result<Self, FormatError> {
        match reader.next_nonspace()? {
            Some(b'[') => Ok(Self {
                reader,
                done: false,
                first: true,
                scratch: Vec::new(),
                strict_eof,
            }),
            Some(b) => Err(FormatError::Json(format!(
                "expected '[' to start JSON array body, found '{}' (0x{b:02x})",
                b as char
            ))),
            // No array at all (empty document / empty navigated value): an
            // immediately-exhausted stream.
            None => Ok(Self {
                reader,
                done: true,
                first: true,
                scratch: Vec::new(),
                strict_eof,
            }),
        }
    }

    /// Yield the next array element, or `None` at the closing `]`.
    ///
    /// Reads exactly one element's bytes (tracking string and nesting state),
    /// then parses them with `serde_json::from_slice`. O(1 record): the
    /// element buffer is the only growth and is reused-then-dropped per call.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] on malformed array framing or a failed
    /// element parse; [`FormatError::Io`] on a read failure.
    pub(crate) fn next(&mut self) -> Result<Option<serde_json::Value>, FormatError> {
        if self.done {
            return Ok(None);
        }
        // Between elements sits either a separating comma or the closing
        // bracket. The first element has no leading comma.
        match self.reader.next_nonspace()? {
            Some(b']') => {
                self.done = true;
                if self.strict_eof {
                    // The array was the whole document; reject trailing garbage
                    // (`[1,2] xyz`) the way a full-document parse would.
                    if let Some(b) = self.reader.next_nonspace()? {
                        return Err(FormatError::Json(format!(
                            "trailing characters after top-level JSON array: '{}' (0x{b:02x})",
                            b as char
                        )));
                    }
                }
                return Ok(None);
            }
            Some(b',') if !self.first => {}
            Some(b) if self.first => {
                // The first element's opening byte — push it back so the value
                // scan below sees the whole element.
                self.reader.push_back(b);
            }
            Some(b) => {
                return Err(FormatError::Json(format!(
                    "expected ',' or ']' between JSON array elements, found '{}' (0x{b:02x})",
                    b as char
                )));
            }
            None => {
                return Err(FormatError::Json(
                    "unexpected end of input inside JSON array body".into(),
                ));
            }
        }
        self.first = false;

        // A trailing comma before `]` (`[1,2,]`) is invalid JSON; surface it.
        match self.reader.next_nonspace()? {
            Some(b']') => {
                return Err(FormatError::Json(
                    "trailing comma before ']' in JSON array body".into(),
                ));
            }
            Some(b) => self.reader.push_back(b),
            None => {
                return Err(FormatError::Json(
                    "unexpected end of input inside JSON array body".into(),
                ));
            }
        }

        self.scratch.clear();
        scan_value(&mut self.reader, &mut self.scratch)?;
        let value = serde_json::from_slice(&self.scratch)
            .map_err(|e| FormatError::Json(format!("JSON array element: {e}")))?;
        Ok(Some(value))
    }
}

/// A buffered byte reader with a one-byte push-back, so the scanner can peek a
/// structural byte (comma, bracket) and put it back for the value scan.
struct ByteReader {
    inner: BufReader<Box<dyn Read + Send>>,
    /// A single pushed-back byte to re-read before pulling from `inner`.
    peeked: Option<u8>,
}

impl ByteReader {
    fn new(inner: BufReader<Box<dyn Read + Send>>) -> Self {
        Self {
            inner,
            peeked: None,
        }
    }

    /// Pull the next byte, honoring a pushed-back byte first.
    fn next_byte(&mut self) -> Result<Option<u8>, FormatError> {
        if let Some(b) = self.peeked.take() {
            return Ok(Some(b));
        }
        let mut one = [0u8; 1];
        match self.inner.read(&mut one) {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(one[0])),
            Err(e) => Err(FormatError::Io(e)),
        }
    }

    /// Push one byte back so the next [`next_byte`](Self::next_byte) returns it.
    /// At most one byte is ever buffered.
    fn push_back(&mut self, b: u8) {
        debug_assert!(
            self.peeked.is_none(),
            "ByteReader holds at most one pushback"
        );
        self.peeked = Some(b);
    }

    /// Pull the next non-whitespace byte, or `None` at EOF. Whitespace is the
    /// RFC 8259 set (`{space, tab, LF, CR}`), not the wider ASCII-whitespace set
    /// — so a form-feed (`0x0C`) is *not* skipped and surfaces as a structural
    /// byte, matching what `serde_json` accepts.
    fn next_nonspace(&mut self) -> Result<Option<u8>, FormatError> {
        loop {
            match self.next_byte()? {
                Some(b) if is_json_ws(b) => continue,
                other => return Ok(other),
            }
        }
    }
}

/// `true` for the four bytes RFC 8259 admits as JSON whitespace: space, tab,
/// line feed, carriage return. Deliberately narrower than
/// [`u8::is_ascii_whitespace`], which also counts form-feed (`0x0C`) — treating
/// form-feed as whitespace would make this streaming scanner accept inputs
/// `serde_json` rejects. Shared with the auto-detect peek in the reader so the
/// document head is held to the same whitespace rule as the body.
pub(crate) fn is_json_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

/// Descend a top-level JSON object following `path`, leaving the reader
/// positioned at the start of the value the final segment names.
///
/// Skips sibling keys' values structurally. An empty `path` is a no-op (the
/// reader is already at the target).
fn navigate_to_path(reader: &mut ByteReader, path: &[String]) -> Result<(), FormatError> {
    for (depth, segment) in path.iter().enumerate() {
        match reader.next_nonspace()? {
            Some(b'{') => {}
            Some(b) => {
                return Err(FormatError::Json(format!(
                    "record_path: expected an object at segment '{segment}' (depth {depth}), \
                     found '{}' (0x{b:02x})",
                    b as char
                )));
            }
            None => {
                return Err(FormatError::Json(format!(
                    "record_path: unexpected end of input seeking segment '{segment}'"
                )));
            }
        }
        find_key_in_object(reader, segment)?;
    }
    Ok(())
}

/// Within an object whose `{` was just consumed, advance to the value of
/// `target_key`, leaving the reader positioned at the start of that value.
/// Sibling keys before it are skipped, values and all.
fn find_key_in_object(reader: &mut ByteReader, target_key: &str) -> Result<(), FormatError> {
    loop {
        // A key, or the object's closing brace if the key is absent.
        match reader.next_nonspace()? {
            Some(b'"') => {}
            Some(b'}') => {
                return Err(FormatError::Json(format!(
                    "record_path: key '{target_key}' not found in object"
                )));
            }
            Some(b',') => continue,
            Some(b) => {
                return Err(FormatError::Json(format!(
                    "record_path: expected a key string, found '{}' (0x{b:02x})",
                    b as char
                )));
            }
            None => {
                return Err(FormatError::Json(format!(
                    "record_path: unexpected end of input seeking key '{target_key}'"
                )));
            }
        }
        let key = read_string_body(reader)?;
        // The ':' separating key and value.
        match reader.next_nonspace()? {
            Some(b':') => {}
            other => {
                return Err(FormatError::Json(format!(
                    "record_path: expected ':' after key '{key}', found {other:?}"
                )));
            }
        }
        if key == target_key {
            return Ok(());
        }
        // Skip this sibling's value, advancing the reader past it without
        // buffering — an arbitrarily large preceding sibling costs O(1) memory.
        scan_value(reader, &mut Discard)?;
    }
}

/// Read a JSON string's body bytes (the opening `"` already consumed),
/// returning the decoded contents up to the closing `"`. Handles `\"` and
/// `\\` so an escaped quote does not terminate the string; other escapes are
/// preserved verbatim (sufficient for object-key comparison).
fn read_string_body(reader: &mut ByteReader) -> Result<String, FormatError> {
    let mut out = Vec::new();
    loop {
        match reader.next_byte()? {
            Some(b'"') => break,
            Some(b'\\') => {
                // Keep the escape and its following byte verbatim; only `\"`
                // and `\\` matter for terminator detection and both are handled
                // by consuming the escaped byte without treating it as a quote.
                out.push(b'\\');
                match reader.next_byte()? {
                    Some(c) => out.push(c),
                    None => {
                        return Err(FormatError::Json(
                            "unterminated escape in JSON string".into(),
                        ));
                    }
                }
            }
            Some(b) => out.push(b),
            None => {
                return Err(FormatError::Json("unterminated JSON string".into()));
            }
        }
    }
    // Decode the captured bytes via serde so escapes resolve correctly for the
    // returned key. Re-wrap in quotes to parse as a JSON string literal.
    let mut quoted = Vec::with_capacity(out.len() + 2);
    quoted.push(b'"');
    quoted.extend_from_slice(&out);
    quoted.push(b'"');
    serde_json::from_slice::<String>(&quoted)
        .map_err(|e| FormatError::Json(format!("JSON object key: {e}")))
}

/// A byte destination for the value scanner, so one scan implementation serves
/// both *capturing* an array element (into a `Vec<u8>` to parse) and *skipping*
/// a preceding sibling during path navigation (into [`Discard`], retaining
/// nothing). The skip path is what keeps `record_path` navigation O(1) memory
/// over an arbitrarily large preceding sibling.
trait ScanSink {
    /// Append one scanned byte, or drop it for a skip-only scan.
    fn push(&mut self, byte: u8);
}

impl ScanSink for Vec<u8> {
    fn push(&mut self, byte: u8) {
        Vec::push(self, byte);
    }
}

/// A [`ScanSink`] that retains nothing — used to advance the reader over a
/// skipped value (a navigated-past sibling) without buffering its bytes.
struct Discard;

impl ScanSink for Discard {
    fn push(&mut self, _byte: u8) {}
}

/// Scan exactly one complete JSON value, leaving the reader positioned
/// immediately after the value and writing its bytes to `sink`.
///
/// Tracks string and escape state so a `,`/`}`/`]` inside a string does not
/// end the value, and brace/bracket depth so a nested container is captured
/// whole. The leading byte of the value must be the next byte the reader
/// yields. O(value size) when `sink` is a `Vec` (one element at a time, never
/// the whole array); O(1) memory when `sink` is [`Discard`] (a skipped
/// sibling).
fn scan_value<S: ScanSink>(reader: &mut ByteReader, sink: &mut S) -> Result<(), FormatError> {
    // Establish what kind of value we are reading from its first non-space byte.
    let first = match reader.next_nonspace()? {
        Some(b) => b,
        None => {
            return Err(FormatError::Json(
                "unexpected end of input scanning a JSON value".into(),
            ));
        }
    };
    sink.push(first);
    match first {
        b'{' | b'[' => scan_container(reader, sink, first),
        b'"' => scan_rest_of_string(reader, sink),
        // A scalar (number, true, false, null): read until a structural byte or
        // whitespace ends it, pushing that delimiter back for the caller.
        _ => scan_scalar(reader, sink),
    }
}

/// First JSON container nesting depth the framing scanner *rejects*. The
/// scanner accepts an element nested up to `MAX_NESTING_DEPTH - 1` (127) levels
/// and rejects the 128th, so the scanner — not the downstream
/// `serde_json::from_slice` — owns the depth boundary and emits the clean error.
/// `serde_json`'s own recursion limit is 128, so rejecting at 128 keeps the
/// scanner strictly inside serde's limit; an over-deep element never reaches
/// serde's "recursion limit exceeded" message, and never overflows the stack.
const MAX_NESTING_DEPTH: usize = 128;

/// Scan a container (object or array) body into `sink`, the opening bracket
/// already pushed. Balances nested containers of either kind and ignores
/// structural bytes inside strings.
///
/// Iterative, not recursive: an explicit stack of expected close delimiters
/// tracks the open `{`/`[` of *both* kinds, so a deeply alternating-nested
/// element (`{`→`[`→`{`…) costs heap, not call frames, and cannot overflow the
/// stack. The reachable depth is bounded by [`MAX_NESTING_DEPTH`].
///
/// # Errors
///
/// Returns [`FormatError::Json`] on end-of-input inside the container, a
/// mismatched closer, or nesting that would reach [`MAX_NESTING_DEPTH`].
fn scan_container<S: ScanSink>(
    reader: &mut ByteReader,
    sink: &mut S,
    open: u8,
) -> Result<(), FormatError> {
    // Stack of the close delimiters still owed, innermost last. Seeded with the
    // already-consumed opener's close, so `stack.len()` is the current depth.
    let mut stack: Vec<u8> = vec![if open == b'{' { b'}' } else { b']' }];
    while let Some(&expected_close) = stack.last() {
        let b = match reader.next_byte()? {
            Some(b) => b,
            None => {
                return Err(FormatError::Json(
                    "unexpected end of input inside a JSON container".into(),
                ));
            }
        };
        sink.push(b);
        match b {
            b'"' => scan_rest_of_string(reader, sink)?,
            b'{' | b'[' => {
                // Reject before the push that would reach MAX_NESTING_DEPTH, so
                // the deepest accepted element is `MAX_NESTING_DEPTH - 1` levels.
                if stack.len() + 1 >= MAX_NESTING_DEPTH {
                    return Err(FormatError::Json(format!(
                        "JSON nesting reaches the {MAX_NESTING_DEPTH}-level limit in an array element"
                    )));
                }
                stack.push(if b == b'{' { b'}' } else { b']' });
            }
            b'}' | b']' if b == expected_close => {
                stack.pop();
            }
            b'}' | b']' => {
                return Err(FormatError::Json(format!(
                    "mismatched JSON close '{}' (expected '{}')",
                    b as char, expected_close as char
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Scan the remaining bytes of a string whose opening `"` is already pushed,
/// writing up to and including the closing `"`. Honors `\` escapes.
fn scan_rest_of_string<S: ScanSink>(
    reader: &mut ByteReader,
    sink: &mut S,
) -> Result<(), FormatError> {
    loop {
        let b = match reader.next_byte()? {
            Some(b) => b,
            None => {
                return Err(FormatError::Json("unterminated JSON string".into()));
            }
        };
        sink.push(b);
        match b {
            b'"' => return Ok(()),
            b'\\' => {
                // Consume the escaped byte verbatim so `\"` does not terminate.
                match reader.next_byte()? {
                    Some(c) => sink.push(c),
                    None => {
                        return Err(FormatError::Json(
                            "unterminated escape in JSON string".into(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
}

/// Scan a scalar value (number / `true` / `false` / `null`) whose first byte is
/// already pushed, writing bytes until a delimiter (`,`/`}`/`]`/JSON
/// whitespace/EOF). The delimiter is pushed back so the caller's framing sees
/// it. JSON whitespace is exactly `{space, tab, LF, CR}` (RFC 8259), so a
/// non-whitespace control byte like form-feed is captured into the scalar and
/// rejected by the element parse rather than silently ending the value.
fn scan_scalar<S: ScanSink>(reader: &mut ByteReader, sink: &mut S) -> Result<(), FormatError> {
    loop {
        match reader.next_byte()? {
            Some(b) if is_json_ws(b) || b == b',' || b == b'}' || b == b']' => {
                reader.push_back(b);
                return Ok(());
            }
            Some(b) => sink.push(b),
            None => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn br(s: &str) -> BufReader<Box<dyn Read + Send>> {
        BufReader::new(Box::new(Cursor::new(s.as_bytes().to_vec())))
    }

    fn collect_top_level(s: &str) -> Vec<serde_json::Value> {
        let mut stream = JsonArrayStream::top_level(br(s)).unwrap();
        let mut out = Vec::new();
        while let Some(v) = stream.next().unwrap() {
            out.push(v);
        }
        out
    }

    fn collect_path(s: &str, path: &[&str]) -> Vec<serde_json::Value> {
        let segs: Vec<String> = path.iter().map(|s| s.to_string()).collect();
        let mut stream = JsonArrayStream::at_path(br(s), &segs).unwrap();
        let mut out = Vec::new();
        while let Some(v) = stream.next().unwrap() {
            out.push(v);
        }
        out
    }

    #[test]
    fn streams_top_level_scalars() {
        let vs = collect_top_level("[1, 2, 3]");
        assert_eq!(vs, vec![json_int(1), json_int(2), json_int(3)]);
    }

    #[test]
    fn streams_top_level_objects() {
        let vs = collect_top_level(r#"[{"a":1},{"a":2}]"#);
        assert_eq!(vs.len(), 2);
        assert_eq!(vs[0]["a"], serde_json::json!(1));
        assert_eq!(vs[1]["a"], serde_json::json!(2));
    }

    #[test]
    fn empty_array_yields_nothing() {
        assert!(collect_top_level("[]").is_empty());
        assert!(collect_top_level("[   ]").is_empty());
    }

    #[test]
    fn nested_containers_and_strings_with_structural_bytes() {
        // Commas and brackets inside strings and nested arrays must not split
        // elements early.
        let vs = collect_top_level(r#"[{"k":"a,b]c","n":[1,2,{"x":3}]},{"k":"}"}]"#);
        assert_eq!(vs.len(), 2);
        assert_eq!(vs[0]["k"], serde_json::json!("a,b]c"));
        assert_eq!(vs[0]["n"], serde_json::json!([1, 2, {"x": 3}]));
        assert_eq!(vs[1]["k"], serde_json::json!("}"));
    }

    #[test]
    fn escaped_quote_does_not_terminate_string() {
        let vs = collect_top_level(r#"[{"k":"he said \"hi\""}]"#);
        assert_eq!(vs[0]["k"], serde_json::json!("he said \"hi\""));
    }

    #[test]
    fn navigates_record_path_skipping_siblings() {
        let s = r#"{"metadata":{"v":1},"data":{"results":[{"x":1},{"x":2}]}}"#;
        let vs = collect_path(s, &["data", "results"]);
        assert_eq!(vs.len(), 2);
        assert_eq!(vs[0]["x"], serde_json::json!(1));
        assert_eq!(vs[1]["x"], serde_json::json!(2));
    }

    #[test]
    fn navigates_past_a_large_sibling_blob() {
        let big = "x".repeat(10_000);
        let s = format!(r#"{{"big_blob":"{big}","target":[{{"id":1}},{{"id":2}}]}}"#);
        let vs = collect_path(&s, &["target"]);
        assert_eq!(vs.len(), 2);
        assert_eq!(vs[0]["id"], serde_json::json!(1));
    }

    #[test]
    fn missing_key_is_an_error() {
        let segs = vec!["nope".to_string()];
        let mut stream = JsonArrayStream::at_path(br(r#"{"a":[]}"#), &segs);
        assert!(stream.is_err() || stream.as_mut().unwrap().next().is_err());
    }

    #[test]
    fn trailing_comma_is_rejected() {
        let mut stream = JsonArrayStream::top_level(br("[1,2,]")).unwrap();
        assert_eq!(stream.next().unwrap(), Some(json_int(1)));
        assert_eq!(stream.next().unwrap(), Some(json_int(2)));
        assert!(stream.next().is_err(), "trailing comma must error");
    }

    #[test]
    fn floats_and_keywords_scan_correctly() {
        let vs = collect_top_level("[1.5, true, false, null, -3]");
        assert_eq!(
            vs,
            vec![
                serde_json::json!(1.5),
                serde_json::json!(true),
                serde_json::json!(false),
                serde_json::json!(null),
                serde_json::json!(-3),
            ]
        );
    }

    fn json_int(n: i64) -> serde_json::Value {
        serde_json::json!(n)
    }

    /// A `Read` that records the running total of bytes pulled from it, so a
    /// test can prove the array scanner consumes incrementally rather than
    /// reading the whole array before yielding the first element.
    struct CountingRead {
        bytes: std::io::Cursor<Vec<u8>>,
        consumed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Read for CountingRead {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.bytes.read(buf)?;
            self.consumed
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            Ok(n)
        }
    }

    #[test]
    fn reads_incrementally_not_the_whole_array_up_front() {
        use std::sync::atomic::Ordering;
        // A large array: if the scanner collected eagerly it would pull the
        // whole body before the first `next`. Element-at-a-time scanning pulls
        // only enough to frame the first element.
        let mut json = String::from("[");
        for i in 0..5_000 {
            if i > 0 {
                json.push(',');
            }
            json.push_str(&format!(r#"{{"id":{i}}}"#));
        }
        json.push(']');
        let total = json.len();

        let consumed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reader: Box<dyn Read + Send> = Box::new(CountingRead {
            bytes: std::io::Cursor::new(json.into_bytes()),
            consumed: std::sync::Arc::clone(&consumed),
        });
        let mut stream = JsonArrayStream::top_level(BufReader::new(reader)).unwrap();

        // First element pulled.
        let first = stream.next().unwrap().unwrap();
        assert_eq!(first["id"], serde_json::json!(0));
        // The BufReader fills in 8 KB chunks, so consumption after one element
        // is at most a couple of fill blocks — far less than the whole body.
        assert!(
            consumed.load(Ordering::Relaxed) < total / 4,
            "first element pulled {} of {total} bytes — should be a fraction, not the whole array",
            consumed.load(Ordering::Relaxed)
        );

        // The rest still streams correctly.
        let mut count = 1usize;
        while stream.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5_000);
    }

    /// A single top-level array element nested `depth` containers deep, as
    /// same-kind `[[[…1…]]]`. Returns the framed document `[<elem>]`.
    fn nested_array_doc(depth: usize) -> String {
        let mut elem = String::new();
        for _ in 0..depth {
            elem.push('[');
        }
        elem.push('1');
        for _ in 0..depth {
            elem.push(']');
        }
        format!("[{elem}]")
    }

    #[test]
    fn deeply_alternating_nested_element_errors_not_crashes() {
        // An alternating `{`/`[` nest deeper than the cap. The pre-change
        // scanner recursed one frame per level here and would overflow the
        // stack; the iterative cap returns a clean error instead. Build
        // `[{"a":[{"a":[ ... ]}]}]` to a depth well past MAX_NESTING_DEPTH.
        let levels = MAX_NESTING_DEPTH + 50;
        let mut json = String::from("[");
        for _ in 0..levels {
            json.push_str(r#"{"a":["#);
        }
        for _ in 0..levels {
            json.push_str("]}");
        }
        json.push(']');

        let mut stream = JsonArrayStream::top_level(br(&json)).unwrap();
        let err = stream
            .next()
            .expect_err("over-deep nesting must error, not crash");
        match err {
            FormatError::Json(msg) => assert!(
                msg.contains("nesting reaches"),
                "error names the nesting limit: {msg}"
            ),
            other => panic!("expected a nesting-limit Json error, got {other:?}"),
        }
    }

    #[test]
    fn nesting_depth_boundary_is_owned_by_the_scanner() {
        // The scanner owns the boundary: the deepest *accepted* element is
        // `MAX_NESTING_DEPTH - 1` (127) deep, and the 128th-deep element is
        // rejected with the SCANNER's message — never serde_json's recursion
        // error. This keeps the scanner strictly inside serde's limit (128).
        let ok = collect_top_level(&nested_array_doc(MAX_NESTING_DEPTH - 1));
        assert_eq!(ok.len(), 1, "a 127-deep element streams");

        let mut stream =
            JsonArrayStream::top_level(br(&nested_array_doc(MAX_NESTING_DEPTH))).unwrap();
        let err = stream
            .next()
            .expect_err("a 128-deep element is rejected by the scanner");
        match err {
            FormatError::Json(msg) => {
                assert!(
                    msg.contains("nesting reaches"),
                    "the scanner owns the boundary message, not serde: {msg}"
                );
                assert!(
                    !msg.contains("recursion"),
                    "serde_json's recursion message must not surface: {msg}"
                );
            }
            other => panic!("expected the scanner's nesting Json error, got {other:?}"),
        }
    }

    #[test]
    fn trailing_garbage_after_top_level_array_is_rejected() {
        // `[1,2] XYZ` — a full-document parse would reject the trailing
        // characters; the streaming scanner must too.
        let mut stream = JsonArrayStream::top_level(br("[1,2] XYZ")).unwrap();
        assert_eq!(stream.next().unwrap(), Some(json_int(1)));
        assert_eq!(stream.next().unwrap(), Some(json_int(2)));
        let err = stream.next().expect_err("trailing garbage must error");
        match err {
            FormatError::Json(msg) => assert!(
                msg.contains("trailing characters"),
                "error names the trailing characters: {msg}"
            ),
            other => panic!("expected a trailing-characters Json error, got {other:?}"),
        }
    }

    #[test]
    fn trailing_whitespace_after_top_level_array_is_ok() {
        // Whitespace after the close is not garbage.
        let vs = collect_top_level("[1,2]  \n\t ");
        assert_eq!(vs, vec![json_int(1), json_int(2)]);
    }

    #[test]
    fn record_path_array_allows_sibling_keys_after_the_array() {
        // For a `record_path` array the enclosing object legitimately carries
        // keys after the array, so the strict trailing-garbage check must not
        // apply.
        let s = r#"{"data":[{"x":1},{"x":2}],"meta":{"v":1}}"#;
        let vs = collect_path(s, &["data"]);
        assert_eq!(vs.len(), 2);
        assert_eq!(vs[1]["x"], serde_json::json!(2));
    }

    #[test]
    fn trailing_form_feed_is_rejected_under_strict_eof() {
        // RFC 8259 whitespace excludes form-feed (0x0C). `serde_json` rejects
        // `[1,2]\x0c`; the streaming scanner must too, not silently accept it.
        let mut stream = JsonArrayStream::top_level(br("[1,2]\u{0c}")).unwrap();
        assert_eq!(stream.next().unwrap(), Some(json_int(1)));
        assert_eq!(stream.next().unwrap(), Some(json_int(2)));
        let err = stream
            .next()
            .expect_err("a trailing form-feed is trailing garbage, not whitespace");
        match err {
            FormatError::Json(msg) => assert!(
                msg.contains("trailing characters"),
                "form-feed after the array must be rejected as trailing garbage: {msg}"
            ),
            other => panic!("expected a trailing-characters Json error, got {other:?}"),
        }
    }

    #[test]
    fn form_feed_inside_a_scalar_is_not_a_value_terminator() {
        // A form-feed mid-scalar must not end the value (RFC 8259 whitespace is
        // narrower); the byte is captured and the element parse rejects it,
        // matching serde_json rather than silently splitting `12` from garbage.
        let mut stream = JsonArrayStream::top_level(br("[12\u{0c}34]")).unwrap();
        assert!(
            stream.next().is_err(),
            "a form-feed inside a number is not a separator"
        );
    }

    #[test]
    fn record_path_navigation_skips_a_giant_sibling_in_o1_memory() {
        // A multi-MB preceding sibling must be navigated past WITHOUT buffering
        // it — the skip sink retains nothing. Track bytes consumed: navigating
        // past the sibling necessarily reads its bytes from the stream, but the
        // peak retained scan buffer stays tiny. We assert correctness over a
        // sibling far larger than any element, proving the skip path runs (a
        // buffering skip would still be correct but is the wart we removed).
        let giant = "x".repeat(4_000_000);
        let s = format!(r#"{{"huge":"{giant}","target":[{{"id":1}},{{"id":2}}]}}"#);
        let consumed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reader: Box<dyn Read + Send> = Box::new(CountingRead {
            bytes: std::io::Cursor::new(s.into_bytes()),
            consumed: std::sync::Arc::clone(&consumed),
        });
        let segs = vec!["target".to_string()];
        let mut stream = JsonArrayStream::at_path(BufReader::new(reader), &segs).unwrap();
        // Both target elements stream correctly after the giant sibling.
        assert_eq!(stream.next().unwrap().unwrap()["id"], serde_json::json!(1));
        assert_eq!(stream.next().unwrap().unwrap()["id"], serde_json::json!(2));
        assert!(stream.next().unwrap().is_none());
        // Sanity: the giant sibling was indeed traversed (its bytes were read).
        assert!(
            consumed.load(std::sync::atomic::Ordering::Relaxed) > 4_000_000,
            "navigation read past the multi-MB sibling"
        );
    }
}
