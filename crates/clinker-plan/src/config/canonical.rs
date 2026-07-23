//! Canonical expansion of the multi-value shorthand surfaces.
//!
//! `clinker config --resolved` prints a pipeline config with the multi-value
//! shorthand materialized: the bare-field forms of `split_to_rows:`,
//! `split_values:`, and `join_values:` rewritten to their full mappings with
//! every default spelled out. The rewrite is **surgical** — only the bytes of
//! those shorthand sequences change; comments, key order, indentation, and every
//! other surface of the document are preserved verbatim. A full `serde`
//! reserialize would reorder keys, drop comments, and reformat unrelated
//! surfaces, which is why the expansion works on the raw text and touches
//! nothing it does not have to.
//!
//! ## How the surfaces are located
//!
//! The three shorthand types already normalize to full form at deserialize time
//! (a bare scalar fills every default; the derived `Serialize` re-emits the full
//! mapping), so the canonical value of each sequence is just its parsed
//! [`Vec`]. To find *where* each sequence lives in the source, the raw YAML is
//! parsed a second time into a lenient probe whose shorthand fields are wrapped
//! in [`Spanned`]: the span's byte offset is the reliable START of the sequence
//! (the first `-` of a block sequence, or the `[` of a flow one). serde-saphyr
//! does not report a container's END, so the end of each sequence is derived by
//! scanning — bracket matching for flow, an indentation walk for block. The
//! probe uses plain nested structs (never a `#[serde(tag)]`/`flatten` context),
//! which is the documented way to keep `Spanned` locations from collapsing to
//! `UNKNOWN`.
//!
//! Schema columns carry no shorthand: a `multiple: true` column is always
//! written explicitly (the plan-time gates reject an implied one), so the schema
//! block is already canonical and is left byte-identical.

use serde::{Deserialize, Serialize};

use clinker_format::{JoinValues, SplitToRows, SplitValues};

use crate::yaml::{self, Spanned};

/// Failure canonicalizing a config's multi-value shorthand.
#[derive(Debug)]
pub enum CanonicalError {
    /// The source did not parse as YAML.
    Parse(String),
    /// A located shorthand sequence could not be re-serialized or its extent
    /// could not be resolved. An invariant violation for input that parsed —
    /// surfaced loudly rather than emitting a silently-wrong document.
    Internal(String),
}

impl std::fmt::Display for CanonicalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CanonicalError::Parse(m) => write!(f, "could not parse config: {m}"),
            CanonicalError::Internal(m) => write!(f, "internal canonicalization error: {m}"),
        }
    }
}

impl std::error::Error for CanonicalError {}

/// Lenient view of a pipeline document that captures only the multi-value
/// shorthand sequences, each wrapped in [`Spanned`] to recover its byte offset.
///
/// Unknown fields are ignored (no `deny_unknown_fields`), so the same bytes the
/// strict [`super::PipelineConfig`] loader accepts parse here too, and any node
/// shape — source, transform, output — flows through with its irrelevant fields
/// discarded.
#[derive(Deserialize)]
struct DocProbe {
    #[serde(default)]
    nodes: Vec<NodeProbe>,
}

#[derive(Deserialize)]
struct NodeProbe {
    #[serde(default)]
    config: Option<ConfigProbe>,
}

#[derive(Deserialize)]
struct ConfigProbe {
    #[serde(default)]
    split_to_rows: Option<Spanned<Vec<SplitToRows>>>,
    #[serde(default)]
    split_values: Option<Spanned<Vec<SplitValues>>>,
    #[serde(default)]
    join_values: Option<Spanned<Vec<JoinValues>>>,
}

/// One byte-range replacement: swap `raw[start..end]` for `rendered`.
struct Edit {
    start: usize,
    end: usize,
    rendered: String,
}

/// Rewrite `raw` so every `split_to_rows` / `split_values` / `join_values`
/// shorthand sequence is expanded to its canonical full-mapping form, leaving
/// all other bytes untouched.
///
/// The output parses to a plan semantically identical to the input, and
/// re-running the expansion on the output is a no-op (each rendered sequence
/// re-parses to the same value and re-renders identically).
pub fn expand_multi_value_shorthand(raw: &str) -> Result<String, CanonicalError> {
    let doc: DocProbe = yaml::from_str(raw).map_err(|e| CanonicalError::Parse(e.to_string()))?;

    // Match the document's line ending so a CRLF file is not spliced with lone
    // `\n`s. `to_string` always emits `\n`; the renderer rewrites them.
    let newline = if raw.contains("\r\n") { "\r\n" } else { "\n" };

    let mut edits: Vec<Edit> = Vec::new();
    for node in &doc.nodes {
        let Some(cfg) = &node.config else { continue };
        push_edit(raw, cfg.split_to_rows.as_ref(), newline, &mut edits)?;
        push_edit(raw, cfg.split_values.as_ref(), newline, &mut edits)?;
        push_edit(raw, cfg.join_values.as_ref(), newline, &mut edits)?;
    }

    // Apply the replacements from the end of the document backwards so each
    // splice leaves every not-yet-applied (earlier) offset valid.
    edits.sort_by(|a, b| b.start.cmp(&a.start));
    for pair in edits.windows(2) {
        // Sorted descending: pair[0] is the later span. The two shorthand
        // surfaces are disjoint by construction; a violation means the extent
        // scan overran, which would corrupt the document — fail instead.
        if pair[1].end > pair[0].start {
            return Err(CanonicalError::Internal(
                "shorthand sequence extents overlap".to_string(),
            ));
        }
    }

    let mut out = raw.to_string();
    for edit in &edits {
        out.replace_range(edit.start..edit.end, &edit.rendered);
    }
    Ok(out)
}

/// Where a shorthand sequence's real opening token lives, relative to the
/// offset the parser reported for its value.
#[derive(Debug)]
enum SequenceStart {
    /// Splice the region beginning at this byte — guaranteed to be `-` or `[`.
    Splice(usize),
    /// The value is a YAML alias (`*anchor`); leave it byte-identical.
    PassThrough,
    /// The region does not begin with a sequence token and is not an alias —
    /// refuse to splice rather than risk corrupting it.
    Unexpected(String),
}

/// Resolve the reported value offset to the sequence's actual opening token.
///
/// serde-saphyr does not always report the opening `-` / `[`:
///
/// - a flow sequence reports the `[` itself (inline or on its own line);
/// - a YAML alias reports the `*anchor` use-site token;
/// - an anchored sequence definition (`key: &anchor` then the sequence) reports
///   its content token in this parser version, but the reported offset landing
///   on the `&anchor` token is handled defensively — the anchor is skipped to
///   the real `-` / `[` so a valid anchored config is never refused;
/// - a block sequence reports the `-` when the item is indented deeper than its
///   key, but the first item's *value* (two bytes past `- `) when the dash sits
///   at the key's own indent — the common `key:\n- item` form.
///
/// In every block case the dash lives at the indent of the line the reported
/// offset falls on, so recovering it is a fixed rule rather than a guess.
fn resolve_sequence_start(raw: &str, reported: usize) -> SequenceStart {
    let bytes = raw.as_bytes();
    match bytes[reported] {
        b'[' => SequenceStart::Splice(reported),
        b'*' => SequenceStart::PassThrough,
        b'&' => resolve_anchor_start(raw, reported),
        _ => {
            let line_start = raw[..reported].rfind('\n').map(|i| i + 1).unwrap_or(0);
            let after = &raw[line_start..];
            let indent = after.len() - after.trim_start_matches(' ').len();
            let dash = line_start + indent;
            let is_item = bytes.get(dash) == Some(&b'-')
                && matches!(
                    bytes.get(dash + 1).copied(),
                    None | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r')
                );
            if dash <= reported && is_item {
                SequenceStart::Splice(dash)
            } else {
                SequenceStart::Unexpected(format!(
                    "shorthand sequence at offset {reported} does not begin with a '-' or \
                     '[' token"
                ))
            }
        }
    }
}

/// Resolve a reported offset that lands on an anchor token (`&anchor`) to the
/// sequence's real opening `-` / `[`.
///
/// Skips the anchor name and the whitespace / line break that separates it from
/// the value, then reports the sequence token that follows. A shape with no
/// `-` / `[` after the anchor is passed through byte-identical rather than
/// erroring: an anchored value that already parsed is never corrupted, and the
/// unexpanded sequence still re-parses as authored.
fn resolve_anchor_start(raw: &str, anchor_at: usize) -> SequenceStart {
    let bytes = raw.as_bytes();
    // Skip `&` and the anchor name. A YAML anchor name runs until whitespace,
    // a line break, or a flow indicator.
    let mut i = anchor_at + 1;
    while i < bytes.len()
        && !matches!(
            bytes[i],
            b' ' | b'\t' | b'\r' | b'\n' | b'[' | b']' | b'{' | b'}' | b','
        )
    {
        i += 1;
    }
    // Skip the whitespace / newline between the anchor and its value.
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\r' | b'\n') {
        i += 1;
    }
    match bytes.get(i) {
        Some(b'[') => SequenceStart::Splice(i),
        Some(b'-')
            if matches!(
                bytes.get(i + 1).copied(),
                None | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r')
            ) =>
        {
            SequenceStart::Splice(i)
        }
        _ => SequenceStart::PassThrough,
    }
}

/// Plan the replacement for one located shorthand sequence and push it onto
/// `edits`. A `None` sequence (the key was absent) or an empty one (nothing to
/// expand) contributes nothing.
fn push_edit<T: Serialize>(
    raw: &str,
    spanned: Option<&Spanned<Vec<T>>>,
    newline: &str,
    edits: &mut Vec<Edit>,
) -> Result<(), CanonicalError> {
    let Some(sp) = spanned else { return Ok(()) };
    if sp.value.is_empty() {
        return Ok(());
    }
    // Byte offsets are always populated for a string source (they are absent
    // only when parsing from a reader), so a missing one is an invariant break.
    let reported = sp.referenced.span().byte_offset().ok_or_else(|| {
        CanonicalError::Internal("shorthand sequence has no source byte offset".to_string())
    })? as usize;
    if reported >= raw.len() {
        return Err(CanonicalError::Internal(
            "shorthand sequence offset past end of source".to_string(),
        ));
    }

    // The reported offset is not always the opening token (a same-indent dash's
    // value, or an alias use-site), so normalize it and refuse to splice
    // anything that is not a real sequence token.
    let start = match resolve_sequence_start(raw, reported) {
        SequenceStart::Splice(pos) => pos,
        SequenceStart::PassThrough => return Ok(()),
        SequenceStart::Unexpected(why) => return Err(CanonicalError::Internal(why)),
    };

    // Hard guard: never splice a region that does not open with a block (`-`)
    // or flow (`[`) token. Splicing anywhere else is exactly the corruption the
    // normalization above exists to prevent, so fail loudly if it is reached.
    let first = raw.as_bytes()[start];
    if first != b'-' && first != b'[' {
        return Err(CanonicalError::Internal(format!(
            "resolved sequence start at offset {start} is '{}', not '-' or '['",
            first as char
        )));
    }

    let end = if first == b'[' {
        flow_sequence_end(raw, start)?
    } else {
        block_sequence_end(raw, start)
    };

    // Regenerating a sequence discards any comment or blank line interleaved
    // among its items. Rather than silently drop an author's comment, leave a
    // sequence carrying interior comments/blanks untouched: it stays valid and
    // re-parses identically — only its shorthand is not expanded.
    if region_has_interior_noise(&raw[start..end]) {
        return Ok(());
    }

    let line_start = raw[..start].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let prefix = &raw[line_start..start];
    let rendered = if first == b'[' && !prefix.bytes().all(|b| b == b' ') {
        // A flow sequence inline after `key:` on one line: a block value is not
        // legal there, so keep it a flow sequence.
        render_flow(&sp.value)?
    } else {
        // A block sequence, or a flow sequence opening its own line — both
        // render as a canonical block sequence indented under `prefix`.
        render_block(&sp.value, prefix.len(), newline)?
    };

    edits.push(Edit {
        start,
        end,
        rendered,
    });
    Ok(())
}

/// Whether a sequence's byte range carries a comment or blank line that
/// regenerating it would drop. A blank line, or any line carrying a YAML
/// comment, counts — erring toward leaving the sequence untouched so no author
/// comment is ever lost.
fn region_has_interior_noise(region: &str) -> bool {
    region
        .split('\n')
        .any(|line| line.trim().is_empty() || line_has_comment(line))
}

/// Whether `line` carries a YAML comment: an unquoted `#` at the line start
/// (after indentation) or preceded by whitespace — a space **or a tab** — and
/// not inside a single- or double-quoted scalar.
///
/// This is stricter than a plain `" #"` substring on two axes: a tab-separated
/// comment (`- item\t# note`) is recognized so regenerating the sequence never
/// silently drops it, and a literal `#` inside a quoted value (e.g. a delimiter
/// spelled `" #"`) is *not* mistaken for a comment, so a sequence carrying one
/// still expands.
fn line_has_comment(line: &str) -> bool {
    let bytes = line.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    // The start of a line counts as "preceded by whitespace", so a comment-only
    // line is detected even with no leading space.
    let mut prev_ws = true;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if in_single {
            // A doubled `''` is an escaped quote, not a close.
            if c == b'\'' {
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_single = false;
            }
            prev_ws = false;
        } else if in_double {
            if c == b'\\' {
                i += 2;
                prev_ws = false;
                continue;
            }
            if c == b'"' {
                in_double = false;
            }
            prev_ws = false;
        } else {
            match c {
                b'#' if prev_ws => return true,
                b'\'' => {
                    in_single = true;
                    prev_ws = false;
                }
                b'"' => {
                    in_double = true;
                    prev_ws = false;
                }
                b' ' | b'\t' => prev_ws = true,
                _ => prev_ws = false,
            }
        }
        i += 1;
    }
    false
}

/// End (exclusive) of the flow sequence that opens at `raw[start] == '['`.
///
/// Matches brackets and braces at depth, honoring single- and double-quoted
/// scalars so a delimiter like `"]"` inside a value is not mistaken for the
/// closing bracket. Input that already parsed is balanced, so the scan
/// terminates at the matching close.
fn flow_sequence_end(raw: &str, start: usize) -> Result<usize, CanonicalError> {
    let bytes = raw.as_bytes();
    let mut depth: i32 = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut i = start;
    while i < bytes.len() {
        let c = bytes[i];
        if in_single {
            if c == b'\'' {
                // A doubled `''` is an escaped quote, not a close.
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_single = false;
            }
        } else if in_double {
            if c == b'\\' {
                i += 2;
                continue;
            }
            if c == b'"' {
                in_double = false;
            }
        } else {
            match c {
                b'\'' => in_single = true,
                b'"' => in_double = true,
                b'[' | b'{' => depth += 1,
                b']' | b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(i + 1);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    Err(CanonicalError::Internal(
        "unterminated flow sequence".to_string(),
    ))
}

/// End (exclusive) of the block sequence whose first `-` is at `raw[start]`.
///
/// The sequence spans its item lines (a `-` at the sequence's own indent) and
/// their more-indented continuations. It ends at the first line that dedents to
/// the sequence indent as a sibling key, or below it. Trailing blank and
/// comment-only lines are excluded so they stay part of the surrounding
/// document rather than being swallowed into the replaced span.
fn block_sequence_end(raw: &str, start: usize) -> usize {
    let base_indent = leading_indent(raw, start);
    let mut line_start = raw[..start].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let mut last_content_end = start;
    let mut first = true;
    loop {
        let line_end = raw[line_start..]
            .find('\n')
            .map(|i| line_start + i)
            .unwrap_or(raw.len());
        let line = &raw[line_start..line_end];
        let indent = line.len() - line.trim_start_matches(' ').len();
        let content = &line[indent..];
        let line_content_end = line_start + line.trim_end().len();

        if first {
            // The line carrying the first `-` is always part of the sequence.
            last_content_end = line_content_end;
            first = false;
        } else if content.trim().is_empty() || content.starts_with('#') {
            // Blank or comment line: never extends the span. If a later item
            // follows, that item's line re-extends past this one; if nothing
            // follows, this line and the rest belong to the document.
        } else if indent > base_indent {
            // Continuation of the current item's mapping/scalar.
            last_content_end = line_content_end;
        } else if indent == base_indent && {
            // A further item at the sequence indent (`trim_end` so a trailing
            // `\r` under CRLF does not hide the dash).
            let item = content.trim_end();
            item == "-" || item.starts_with("- ")
        } {
            last_content_end = line_content_end;
        } else {
            // A sibling key at the sequence indent, or a dedent below it.
            break;
        }

        if line_end >= raw.len() {
            break;
        }
        line_start = line_end + 1;
    }
    last_content_end
}

/// Count of leading spaces on the line containing `offset`, i.e. the column the
/// sequence's first token sits at. Assumes YAML indentation (spaces, never
/// tabs), which the parser enforces upstream.
fn leading_indent(raw: &str, offset: usize) -> usize {
    let line_start = raw[..offset].rfind('\n').map(|i| i + 1).unwrap_or(0);
    offset - line_start
}

/// Render a sequence as a canonical block sequence indented by `base_indent`.
///
/// serde-saphyr emits the sequence at column zero (`- key: val\n  key2: ...`);
/// the first line is spliced directly where the document already provides the
/// leading indent, and every subsequent line is shifted right by `base_indent`
/// so continuations and later items align under it. `newline` is the document's
/// own line ending, so a CRLF file is not spliced with lone `\n`s.
fn render_block<T: Serialize>(
    items: &[T],
    base_indent: usize,
    newline: &str,
) -> Result<String, CanonicalError> {
    let yaml = yaml::to_string(&items).map_err(CanonicalError::Internal)?;
    let body = yaml.trim_end_matches('\n');
    let pad = " ".repeat(base_indent);
    let mut out = String::new();
    for (i, line) in body.split('\n').enumerate() {
        if i > 0 {
            out.push_str(newline);
            if !line.is_empty() {
                out.push_str(&pad);
            }
        }
        out.push_str(line);
    }
    Ok(out)
}

/// Render a sequence as a single-line flow sequence.
///
/// Used only for a shorthand sequence that sits inline after `key:` on one
/// line, where a block value is not legal. Each entry is emitted as JSON, which
/// is valid YAML flow and quotes every value unambiguously, so the result
/// re-parses to the identical sequence.
fn render_flow<T: Serialize>(items: &[T]) -> Result<String, CanonicalError> {
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        parts.push(
            serde_json::to_string(item).map_err(|e| CanonicalError::Internal(e.to_string()))?,
        );
    }
    Ok(format!("[{}]", parts.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every normalized shorthand sequence a document declares, grouped by
    /// surface — the semantic content that must survive canonicalization.
    #[derive(Debug, PartialEq)]
    struct Probed {
        split_to_rows: Vec<Vec<SplitToRows>>,
        split_values: Vec<Vec<SplitValues>>,
        join_values: Vec<Vec<JoinValues>>,
    }

    /// Re-extract the normalized shorthand values from a document, so a
    /// before/after pair can be compared for semantic identity of every
    /// multi-value surface.
    fn probe(raw: &str) -> Probed {
        let doc: DocProbe = yaml::from_str(raw).expect("probe parse");
        let mut out = Probed {
            split_to_rows: Vec::new(),
            split_values: Vec::new(),
            join_values: Vec::new(),
        };
        for node in &doc.nodes {
            if let Some(cfg) = &node.config {
                if let Some(s) = &cfg.split_to_rows {
                    out.split_to_rows.push(s.value.clone());
                }
                if let Some(s) = &cfg.split_values {
                    out.split_values.push(s.value.clone());
                }
                if let Some(s) = &cfg.join_values {
                    out.join_values.push(s.value.clone());
                }
            }
        }
        out
    }

    fn assert_parse_identity(before: &str, after: &str) {
        assert_eq!(probe(before), probe(after), "shorthand values must match");
    }

    fn assert_idempotent(raw: &str) {
        let once = expand_multi_value_shorthand(raw).expect("first pass");
        let twice = expand_multi_value_shorthand(&once).expect("second pass");
        assert_eq!(once, twice, "expansion must be idempotent");
    }

    const SPLIT_TO_ROWS_BARE: &str = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows:
        - line_items
      schema:
        - { name: order_id, type: string }
";

    #[test]
    fn bare_split_to_rows_expands_with_defaults() {
        let out = expand_multi_value_shorthand(SPLIT_TO_ROWS_BARE).unwrap();
        assert!(out.contains("field: line_items"), "\n{out}");
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert!(out.contains("mode: extract"), "\n{out}");
        // The bare form is gone.
        assert!(!out.contains("- line_items"), "\n{out}");
        assert_parse_identity(SPLIT_TO_ROWS_BARE, &out);
        assert_idempotent(SPLIT_TO_ROWS_BARE);
    }

    #[test]
    fn partial_split_to_rows_mapping_materializes_missing_defaults() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: xml
      path: in.xml
      split_to_rows:
        - field: LineItem
          mode: extract
          position_column: line_no
      schema:
        - { name: line_no, type: int }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // `keep_empty` was omitted; the canonical form spells it out.
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert!(out.contains("position_column: line_no"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn split_values_bare_and_escape_and_json_expand() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values:
        - tags
        - field: notes
          delimiter: \"|\"
          escape: \"\\\\\"
        - field: payload
          json: true
      schema:
        - { name: tags, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // Bare `tags` gains the default delimiter.
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(
            out.contains("delimiter: ';'") || out.contains("delimiter: ;"),
            "\n{out}"
        );
        // The escape entry keeps its explicit escape.
        assert!(out.contains("field: notes"), "\n{out}");
        // The JSON entry keeps its flag.
        assert!(out.contains("json: true"), "\n{out}");
        assert!(!out.contains("- tags\n"), "bare form gone:\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn join_values_bare_expands() {
        let raw = "\
nodes:
  - type: output
    name: o
    input: s
    config:
      name: o
      type: csv
      path: out.csv
      join_values:
        - tags
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(out.contains("on_conflict: error"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn surrounding_document_is_preserved_verbatim() {
        let raw = "\
pipeline:
  name: demo   # a trailing comment

# A header comment describing the source.
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json   # inline comment on path
      split_to_rows:
        - line_items
      schema:
        - { name: order_id, type: string }   # keep me
error_handling:
  strategy: fail_fast
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // Every non-shorthand line survives byte-for-byte.
        assert!(
            out.contains("  name: demo   # a trailing comment"),
            "\n{out}"
        );
        assert!(
            out.contains("# A header comment describing the source."),
            "\n{out}"
        );
        assert!(
            out.contains("      path: in.json   # inline comment on path"),
            "\n{out}"
        );
        assert!(
            out.contains("        - { name: order_id, type: string }   # keep me"),
            "\n{out}"
        );
        assert!(out.contains("  strategy: fail_fast"), "\n{out}");
        // And the shorthand did expand.
        assert!(out.contains("keep_empty: true"), "\n{out}");
    }

    #[test]
    fn multiple_shorthand_blocks_in_one_node_all_expand() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: xml
      path: in.xml
      split_to_rows:
        - LineItem
      split_values:
        - cost_centres
      schema:
        - { name: cost_centres, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(out.contains("field: LineItem"), "\n{out}");
        assert!(out.contains("field: cost_centres"), "\n{out}");
        assert!(out.contains("mode: extract"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn schema_multiple_column_is_left_unchanged() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values:
        - tags
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // The schema block is already canonical and must survive untouched.
        assert!(
            out.contains("        - { name: tags, type: string, multiple: true }"),
            "\n{out}"
        );
    }

    #[test]
    fn inline_flow_sequence_expands_in_place() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows: [line_items]
      schema:
        - { name: order_id, type: string }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // Stays on one line (flow), but the field/default are materialized.
        assert!(out.contains("split_to_rows: ["), "\n{out}");
        assert!(out.contains("\"field\":\"line_items\""), "\n{out}");
        assert!(out.contains("\"keep_empty\":true"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn flow_sequence_on_its_own_line_becomes_block() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows:
        [line_items]
      schema:
        - { name: order_id, type: string }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(out.contains("- field: line_items"), "\n{out}");
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn config_without_shorthand_is_unchanged() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      schema:
        - { name: order_id, type: string }
  - type: output
    name: o
    input: s
    config:
      name: o
      type: json
      path: out.json
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(raw, out, "no shorthand present, document must be unchanged");
    }

    // ---- Splice-corruption regressions: the reported offset is not always
    // the sequence's opening token ----

    #[test]
    fn same_indent_block_single_item_expands_without_corruption() {
        // The dash sits at the parent key's own indent (the `key:\n- item`
        // form); serde-saphyr reports the item's value, not the dash.
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows:
      - line_items
      schema:
        - { name: order_id, type: string }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: line_items"), "\n{out}");
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn same_indent_block_multi_item_expands_without_corruption() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values:
      - tags
      - codes
      schema:
        - { name: tags, type: string, multiple: true }
        - { name: codes, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(out.contains("field: codes"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn same_indent_block_mapping_item_expands_without_corruption() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: xml
      path: in.xml
      split_to_rows:
      - field: LineItem
        mode: extract
      schema:
        - { name: order_id, type: string }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: LineItem"), "\n{out}");
        // The omitted default is materialized.
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn same_indent_at_nonzero_indent_expands_without_corruption() {
        // Key and dash both at indent 6 (the same-indent form at a nonzero indent).
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_values:
      - tags
      schema:
        - { name: tags, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(
            out.contains("      - field: tags"),
            "correct indent:\n{out}"
        );
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn alias_use_site_is_left_untouched() {
        // An anchored shorthand sequence reused by an alias: the definition
        // expands, the `*sv` use-site is left byte-identical (it still resolves
        // to the expanded value on re-parse).
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values: &sv
        - field: tags
          delimiter: \"|\"
      schema:
        - { name: tags, type: string, multiple: true }
  - type: output
    name: o
    input: s
    config:
      name: o
      type: csv
      path: out.csv
      join_values: *sv
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(
            out.contains("join_values: *sv"),
            "alias use-site must be byte-identical:\n{out}"
        );
        // Not corrupted by an inline block splice.
        assert!(!out.contains("join_values: *sv\n        -"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn interior_comment_between_items_is_preserved() {
        // A comment between items cannot survive regeneration, so the sequence
        // is passed through byte-identical rather than expanded — never dropping
        // the comment.
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values:
        - tags
        # keep this comment
        - codes
      schema:
        - { name: tags, type: string, multiple: true }
        - { name: codes, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(
            raw, out,
            "a sequence with an interior comment is left untouched"
        );
        assert!(out.contains("# keep this comment"), "\n{out}");
    }

    #[test]
    fn anchored_block_same_indent_single_item_expands_and_reparses() {
        // An anchor on the sequence definition, with the block at the key's own
        // indent (`key: &sv\n- item`). The definition must expand — never error
        // — and the anchor must survive so a later alias still resolves.
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values: &sv
      - tags
      schema:
        - { name: tags, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(out.contains("delimiter:"), "defaults materialized:\n{out}");
        assert!(
            out.contains("split_values: &sv"),
            "anchor must be preserved:\n{out}"
        );
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn anchored_block_deeper_multi_item_expands_and_reparses() {
        // An anchor on the sequence definition, with a deeper-indented block of
        // several items. All items expand and the anchor is preserved.
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values: &sv
        - tags
        - codes
      schema:
        - { name: tags, type: string, multiple: true }
        - { name: codes, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(out.contains("field: codes"), "\n{out}");
        assert!(
            out.contains("split_values: &sv"),
            "anchor must be preserved:\n{out}"
        );
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn resolve_sequence_start_skips_anchor_at_reported_offset() {
        // Defensive contract: if the parser ever reports the `&anchor` token as
        // the sequence value's offset, the resolver skips it to the real
        // `-` / `[` rather than classifying the region `Unexpected` (which
        // would hard-error on a valid anchored config).
        for (src, want_byte) in [
            ("key: &sv\n  - a\n", b'-'),
            ("key: &sv\n- a\n", b'-'),
            ("key: &sv [a]\n", b'['),
            ("key: &longer_name\n  - a\n", b'-'),
        ] {
            let amp = src.find('&').expect("anchor token");
            match resolve_sequence_start(src, amp) {
                SequenceStart::Splice(pos) => assert_eq!(
                    src.as_bytes()[pos],
                    want_byte,
                    "resolved to {:?}, want {:?} in {src:?}",
                    src.as_bytes()[pos] as char,
                    want_byte as char
                ),
                other => panic!("expected Splice for {src:?}, got {other:?}"),
            }
        }

        // An anchor with no sequence token after it is passed through, not
        // errored — a valid document is never corrupted.
        match resolve_sequence_start("key: &sv scalar\n", 5) {
            SequenceStart::PassThrough => {}
            other => panic!("expected PassThrough for a scalar anchor, got {other:?}"),
        }
    }

    #[test]
    fn tab_preceded_comment_is_preserved() {
        // A shorthand item ending in a TAB-separated comment must not be
        // dropped: the sequence is passed through byte-identical rather than
        // regenerated (which would discard the comment).
        let raw = "nodes:\n  - type: source\n    name: s\n    config:\n      name: s\n      \
                   type: csv\n      path: in.csv\n      split_values:\n        \
                   - tags\t# keep this\n      schema:\n        \
                   - { name: tags, type: string, multiple: true }\n";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(
            raw, out,
            "a tab-preceded comment must leave the sequence untouched:\n{out}"
        );
        assert!(out.contains("# keep this"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn quoted_hash_value_still_expands() {
        // A value containing `" #"` inside a quoted scalar must not be mistaken
        // for a comment: the sequence still expands, materializing the bare
        // item's defaults while the quoted value is preserved verbatim.
        let raw = "nodes:\n  - type: source\n    name: s\n    config:\n      name: s\n      \
                   type: csv\n      path: in.csv\n      split_values:\n        \
                   - field: notes\n          delimiter: \" #\"\n        - tags\n      \
                   schema:\n        - { name: notes, type: string, multiple: true }\n        \
                   - { name: tags, type: string, multiple: true }\n";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // The bare `- tags` gained its default delimiter — the block expanded.
        assert!(out.contains("field: tags"), "block should expand:\n{out}");
        assert!(out.contains("field: notes"), "\n{out}");
        // `assert_parse_identity` guarantees the quoted `#` delimiter survived.
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn crlf_document_keeps_crlf_line_endings() {
        let lf = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows:
        - line_items
      schema:
        - { name: order_id, type: string }
";
        let raw = lf.replace('\n', "\r\n");
        let out = expand_multi_value_shorthand(&raw).unwrap();
        assert!(out.contains("keep_empty: true"), "\n{out}");
        // No lone `\n` survives: removing every CRLF leaves no bare newline.
        assert!(
            !out.replace("\r\n", "").contains('\n'),
            "mixed line endings in:\n{out:?}"
        );
        assert_parse_identity(&raw, &out);
        // Idempotent on the CRLF form too.
        let twice = expand_multi_value_shorthand(&out).unwrap();
        assert_eq!(out, twice);
    }
}
