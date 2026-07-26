//! The record-space field-path grammar: how a flat column-name string encodes
//! an ordered sequence of path segments.
//!
//! A record-space field name is the string a [`Schema`](crate::Schema) column
//! carries. It is not opaque: an unescaped `.` separates segments, so the
//! column `Address.City` addresses the path `["Address", "City"]`. Readers
//! produce such names when they flatten nested input, writers expand them back
//! into nested output, and CXL addresses them from expressions. All three must
//! agree on one grammar or the same name means different things at different
//! ends of a pipeline.
//!
//! This module lives in `clinker-record` — the format-neutral value-model
//! vocabulary crate — rather than in `clinker-format`, because `cxl` depends on
//! `clinker-record` and not on `clinker-format`. The grammar has to be reachable
//! from the expression language as well as from every reader and writer; hosting
//! it in a format crate would put it out of CXL's reach and force a second
//! implementation there.
//!
//! The user-facing statement of this grammar is `docs/user/src/cxl/field-paths.md`.
//!
//! # Grammar
//!
//! Decoding scans left to right over Unicode scalar values:
//!
//! 1. `\` introduces an escape and must be followed by exactly one of `.`, `[`,
//!    or `\`. The pair contributes that one literal character to the current
//!    segment.
//! 2. A `\` at end of name, or before any other character, is an error. A
//!    silently passed-through `C:\temp` would decode to `C:temp`, changing a
//!    field name without saying so.
//! 3. An unescaped `.` ends the current segment and starts the next.
//! 4. Every other character — including an unescaped `[`, and `]`, `@`, `$`,
//!    `/`, whitespace — is a literal character of the current segment.
//! 5. `[` is reserved. It decodes as a literal today, but `\[` is the form that
//!    keeps meaning "a literal `[`" if bracket indexing is ever given meaning in
//!    a flat name; a bare `[` is not forward-compatible.
//! 6. Empty segments are legal: `a..b` is `["a", "", "b"]`, `""` is `[""]`. An
//!    empty map key is a real key in the value model, and the grammar does not
//!    reject what the value model can hold.
//! 7. More than [`MAX_FIELD_PATH_DEPTH`] segments is an error, so a pathological
//!    name cannot drive unbounded recursion in a tree-building writer.
//!
//! [`encode_segment`] is the inverse: it escapes `\`, `.`, and `[` so an
//! arbitrary literal survives as one segment. Decoding is deliberately more
//! permissive than encoding is productive — a bare `[` decodes but is never
//! produced — which is what leaves rule 5's seam open.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

/// Maximum number of segments a single field name may decode to.
///
/// Chosen to match the depth at which the JSON reader stops flattening, so a
/// name that reader produces is always a name a writer can expand. The XML
/// reader flattens with no depth bound, so a pathologically deep document can
/// still produce a name past this cap — the writer refuses it rather than
/// recursing without limit, which is the point of having the cap at all.
pub const MAX_FIELD_PATH_DEPTH: usize = 64;

/// Characters that carry structural meaning in a field name and therefore need
/// escaping to appear literally in a segment.
const RESERVED: [char; 3] = ['\\', '.', '['];

/// A field name that cannot be decoded, or a set of names that cannot all be
/// expanded into one object tree.
///
/// Every message renders names verbatim between backticks rather than through
/// `{:?}`. The subject of these diagnostics is backslash escaping, and Debug
/// quoting would escape the escapes — a remedy printed as `a\\.b` is a
/// different name from the `a\.b` the author needs to write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldPathError {
    /// The name ends with a `\` that escapes nothing.
    TrailingEscape { name: String },
    /// The name contains `\` followed by a character that is not `.`, `[`, or `\`.
    UnknownEscape { name: String, escape: char },
    /// The name decodes to more than `limit` segments.
    TooDeep { name: String, limit: usize },
    /// `first` terminates at a path `second` continues through — `a` alongside
    /// `a.b`, at any depth. `first` is always the shorter of the two.
    ///
    /// `literal_form` carries the all-literal spelling of `first` when writing
    /// it that way resolves the clash, and is `None` when `first` has no
    /// separator to escape and only a rename will.
    NestedUnderValue {
        first: String,
        second: String,
        literal_form: Option<String>,
    },
    /// Two names decode to the identical path, so only one could be written.
    DuplicatePath { first: String, second: String },
}

impl fmt::Display for FieldPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TrailingEscape { name } => write!(
                f,
                "field name `{name}` ends with a `\\` that escapes nothing — a `\\` must be \
                 followed by `.`, `[`, or `\\`. Write `\\\\` for a literal backslash."
            ),
            Self::UnknownEscape { name, escape } => write!(
                f,
                "field name `{name}` contains the escape `\\{escape}`, which has no meaning — a \
                 `\\` must be followed by `.`, `[`, or `\\`. Write `\\\\` for a literal backslash: \
                 a Windows path column is declared as `C:\\\\temp`."
            ),
            Self::TooDeep { name, limit } => write!(
                f,
                "field name `{name}` nests more than {limit} levels deep — flatten the name so it \
                 addresses at most {limit} path segments."
            ),
            Self::NestedUnderValue {
                first,
                second,
                literal_form,
            } => {
                write!(
                    f,
                    "field names `{first}` and `{second}` cannot both be written: `{first}` holds \
                     a value and is also the container `{second}` nests inside. Rename one of them"
                )?;
                match literal_form {
                    Some(literal) => write!(
                        f,
                        ", or — if the `.` in `{first}` is part of the name rather than a nesting \
                         separator — declare it as `{literal}`."
                    ),
                    None => write!(f, "."),
                }
            }
            Self::DuplicatePath { first, second } => write!(
                f,
                "field names `{first}` and `{second}` address the same field path, so only one \
                 could be written. Rename one of them."
            ),
        }
    }
}

impl std::error::Error for FieldPathError {}

/// Decode a field name into its ordered segments.
///
/// A segment carrying no escape borrows from `name`; only a segment with an
/// escape allocates, so an ordinary name costs one `Vec` and no string copies.
/// Always yields at least one segment: the empty name decodes to one empty
/// segment. Bounded by [`MAX_FIELD_PATH_DEPTH`] entries.
///
/// Callers decode once per schema (a writer building its expansion plan) or per
/// document (an envelope section), never per record.
pub fn decode(name: &str) -> Result<Vec<Cow<'_, str>>, FieldPathError> {
    let mut out: Vec<Cow<'_, str>> = Vec::new();
    // `decoded` stays `None` while the current segment is escape-free, so the
    // common case borrows straight out of `name`. `chunk` marks the start of the
    // verbatim run not yet copied into it.
    let mut decoded: Option<String> = None;
    let mut chunk = 0;
    let mut i = 0;
    let bytes = name.as_bytes();
    loop {
        if out.len() == MAX_FIELD_PATH_DEPTH {
            return Err(FieldPathError::TooDeep {
                name: name.to_string(),
                limit: MAX_FIELD_PATH_DEPTH,
            });
        }
        // `.`, `\` and `[` are ASCII and cannot occur inside a multi-byte UTF-8
        // sequence, so scanning by byte never splits a character.
        match bytes.get(i) {
            None => {
                out.push(close_segment(name, decoded, chunk, i));
                return Ok(out);
            }
            Some(b'.') => {
                out.push(close_segment(name, decoded.take(), chunk, i));
                i += 1;
                chunk = i;
            }
            Some(b'\\') => {
                let Some(&escape) = bytes.get(i + 1) else {
                    return Err(FieldPathError::TrailingEscape {
                        name: name.to_string(),
                    });
                };
                if !matches!(escape, b'.' | b'[' | b'\\') {
                    return Err(FieldPathError::UnknownEscape {
                        name: name.to_string(),
                        escape: name[i + 1..]
                            .chars()
                            .next()
                            .expect("a byte past the backslash implies a character"),
                    });
                }
                let buf = decoded.get_or_insert_with(String::new);
                buf.push_str(&name[chunk..i]);
                buf.push(char::from(escape));
                i += 2;
                chunk = i;
            }
            Some(_) => i += 1,
        }
    }
}

/// Close a segment: borrow it whole when nothing was escaped, otherwise append
/// the trailing verbatim run to the buffer the escapes were decoded into.
fn close_segment(name: &str, decoded: Option<String>, chunk: usize, end: usize) -> Cow<'_, str> {
    match decoded {
        None => Cow::Borrowed(&name[chunk..end]),
        Some(mut buf) => {
            buf.push_str(&name[chunk..end]);
            Cow::Owned(buf)
        }
    }
}

/// Encode one literal segment so it survives [`decode`] as exactly itself,
/// escaping `\`, `.`, and `[`.
///
/// Borrows unchanged when the literal contains none of them.
pub fn encode_segment(literal: &str) -> Cow<'_, str> {
    if !literal.contains(RESERVED) {
        return Cow::Borrowed(literal);
    }
    let mut out = String::with_capacity(literal.len() + 8);
    for ch in literal.chars() {
        if RESERVED.contains(&ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    Cow::Owned(out)
}

/// Verify that every name in `names` decodes, and that the whole set can be
/// expanded into one object tree.
///
/// Rejects a name that terminates at a path another name continues through
/// (`a` alongside `a.b`, in either order, at any depth) and two names that
/// decode to the same path. Both cases would otherwise resolve last-wins,
/// dropping a column without saying so. `a.b` and `a\.b` do not collide: they
/// decode to `["a", "b"]` and `["a.b"]`.
///
/// Order-independent: the same name set produces the same verdict whatever
/// order it arrives in. Callers run this over the full emitted column set
/// before writing any bytes, so a rejected set leaves no partial output.
///
/// The trie built here borrows from `names` and is dropped on return; nothing
/// is retained.
pub fn check_expandable<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> Result<(), FieldPathError> {
    let mut root = TrieNode::default();
    for name in names {
        let path = decode(name)?;
        let depth = path.len();
        let mut node = &mut root;
        for (at, segment) in path.into_iter().enumerate() {
            let last = at + 1 == depth;
            node = node.children.entry(segment).or_default();
            match (last, node.terminal, node.interior) {
                // A shorter name already ends where this one keeps descending.
                (false, Some(shorter), _) => return Err(nesting_clash(shorter, name)),
                // A longer name already descends through where this one ends.
                (true, None, Some(longer)) => return Err(nesting_clash(name, longer)),
                (true, Some(other), _) => {
                    return Err(FieldPathError::DuplicatePath {
                        first: other.to_string(),
                        second: name.to_string(),
                    });
                }
                (false, None, _) => {
                    node.interior.get_or_insert(name);
                }
                (true, None, None) => node.terminal = Some(name),
            }
        }
    }
    Ok(())
}

/// Build the clash between a terminal name and the longer name nesting under
/// it, offering the terminal's all-literal encoding when that would resolve it.
fn nesting_clash(terminal: &str, nested: &str) -> FieldPathError {
    let literal_form = decode(terminal)
        .ok()
        // Rejoined with plain `.` and re-escaped: the spelling that keeps the
        // whole name as one literal key.
        .map(|segs| encode_segment(&segs.join(".")).into_owned())
        .filter(|literal| literal != terminal);
    FieldPathError::NestedUnderValue {
        first: terminal.to_string(),
        second: nested.to_string(),
        literal_form,
    }
}

/// One node of the transient prefix trie [`check_expandable`] walks. `terminal`
/// names the column that ends here; `interior` names a column that passes
/// through on its way deeper. A node carrying both is the collision.
#[derive(Default)]
struct TrieNode<'a> {
    terminal: Option<&'a str>,
    interior: Option<&'a str>,
    children: HashMap<Cow<'a, str>, TrieNode<'a>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segs(name: &str) -> Vec<String> {
        decode(name)
            .unwrap_or_else(|e| panic!("{name:?} should decode, got: {e}"))
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn err(name: &str) -> FieldPathError {
        decode(name).unwrap_err()
    }

    /// The canonical name for a path: every segment escaped, joined with `.`.
    /// The inverse of [`decode`], used here to exercise the round trip in both
    /// directions.
    fn encode_path<S: AsRef<str>>(path: &[S]) -> String {
        path.iter()
            .map(|seg| encode_segment(seg.as_ref()))
            .collect::<Vec<_>>()
            .join(".")
    }

    #[test]
    fn plain_names_split_on_unescaped_dots() {
        assert_eq!(segs("a.b.c"), ["a", "b", "c"]);
        assert_eq!(segs("a"), ["a"]);
        assert_eq!(segs("Address.City"), ["Address", "City"]);
    }

    #[test]
    fn empty_segments_are_real_segments() {
        assert_eq!(segs(""), [""]);
        assert_eq!(segs("a..b"), ["a", "", "b"]);
        assert_eq!(segs(".a"), ["", "a"]);
        assert_eq!(segs("a."), ["a", ""]);
        assert_eq!(segs("."), ["", ""]);
    }

    #[test]
    fn escapes_decode_to_one_literal_character() {
        assert_eq!(segs(r"a\.b"), ["a.b"]);
        assert_eq!(segs(r"a\.b.c"), ["a.b", "c"]);
        assert_eq!(segs(r"a\\b"), [r"a\b"]);
        assert_eq!(segs(r"a\[0]"), ["a[0]"]);
        assert_eq!(segs(r"\."), ["."]);
    }

    #[test]
    fn unescaped_bracket_stays_literal() {
        // Pins the reserved-`[` seam: a bare `[` decodes as a literal today, so
        // giving bracket indexing meaning in a flat name is a visible change to
        // this expectation, and `\[` is the forward-compatible spelling.
        assert_eq!(segs("a[0]"), ["a[0]"]);
        assert_eq!(segs("a[0].b"), ["a[0]", "b"]);
        assert_eq!(segs("]"), ["]"]);
    }

    #[test]
    fn other_punctuation_is_literal() {
        assert_eq!(segs("$ck.customer_id"), ["$ck", "customer_id"]);
        assert_eq!(segs("@id"), ["@id"]);
        assert_eq!(segs("a b/c"), ["a b/c"]);
        assert_eq!(segs("héllo.wörld"), ["héllo", "wörld"]);
    }

    #[test]
    fn dangling_escape_is_rejected() {
        assert_eq!(
            err(r"a\"),
            FieldPathError::TrailingEscape {
                name: r"a\".to_string()
            }
        );
    }

    #[test]
    fn unknown_escape_is_rejected_naming_the_character() {
        assert_eq!(
            err(r"a\tb"),
            FieldPathError::UnknownEscape {
                name: r"a\tb".to_string(),
                escape: 't',
            }
        );
        // A Windows path column: loud, with the remedy in the message, rather
        // than silently decoding to `C:temp`.
        let e = err(r"C:\temp");
        assert!(matches!(
            e,
            FieldPathError::UnknownEscape { ref escape, .. } if *escape == 't'
        ));
        assert!(e.to_string().contains(r"C:\\temp"), "{e}");
        assert_eq!(
            err(r"a\]"),
            FieldPathError::UnknownEscape {
                name: r"a\]".to_string(),
                escape: ']',
            }
        );
    }

    #[test]
    fn depth_is_capped() {
        let ok = vec!["a"; MAX_FIELD_PATH_DEPTH].join(".");
        assert_eq!(segs(&ok).len(), MAX_FIELD_PATH_DEPTH);
        let too_deep = vec!["a"; MAX_FIELD_PATH_DEPTH + 1].join(".");
        assert_eq!(
            err(&too_deep),
            FieldPathError::TooDeep {
                name: too_deep.clone(),
                limit: MAX_FIELD_PATH_DEPTH,
            }
        );
    }

    #[test]
    fn encode_segment_escapes_only_the_reserved_characters() {
        assert_eq!(encode_segment("a.b"), r"a\.b");
        assert_eq!(encode_segment(r"a\b"), r"a\\b");
        assert_eq!(encode_segment("a[0]"), r"a\[0]");
        assert_eq!(encode_segment("plain"), "plain");
        assert!(matches!(encode_segment("plain"), Cow::Borrowed(_)));
        assert!(matches!(encode_segment("@id"), Cow::Borrowed(_)));
    }

    #[test]
    fn encoding_round_trips_through_decoding() {
        let corpus = [
            vec!["a"],
            vec!["a", "b", "c"],
            vec!["a.b"],
            vec![r"a\b"],
            vec!["a[0]", "b"],
            vec![""],
            vec!["a", "", "b"],
            vec![r"C:\temp", "size"],
            vec!["héllo.wörld"],
            vec!["$ck", "customer_id"],
        ];
        for path in corpus {
            let name = encode_path(&path);
            assert_eq!(segs(&name), path, "round trip through {name:?}");
        }
    }

    #[test]
    fn canonical_names_re_encode_to_themselves() {
        for name in ["a", "a.b.c", r"a\.b", r"a\\b", "", "a..b", "@id", "$ck.x"] {
            assert_eq!(encode_path(&decode(name).unwrap()), name);
        }
    }

    #[test]
    fn disjoint_names_expand() {
        assert!(check_expandable(["a", "b.c", "b.d", "e.f.g"]).is_ok());
        assert!(check_expandable(std::iter::empty()).is_ok());
    }

    #[test]
    fn a_value_that_is_also_a_container_is_rejected_in_either_order() {
        for names in [["a", "a.b"], ["a.b", "a"]] {
            let e = check_expandable(names).unwrap_err();
            assert_eq!(
                e,
                FieldPathError::NestedUnderValue {
                    first: "a".to_string(),
                    second: "a.b".to_string(),
                    // Escaping `a` changes nothing, so only a rename resolves it.
                    literal_form: None,
                },
                "the shorter name is reported first whatever order they arrive in"
            );
            let msg = e.to_string();
            assert!(msg.contains("`a`") && msg.contains("`a.b`"), "{msg}");
        }
    }

    #[test]
    fn a_deeper_clash_suggests_the_literal_spelling() {
        for names in [["a.b", "a.b.c"], ["a.b.c", "a.b"]] {
            let e = check_expandable(names).unwrap_err();
            assert_eq!(
                e,
                FieldPathError::NestedUnderValue {
                    first: "a.b".to_string(),
                    second: "a.b.c".to_string(),
                    literal_form: Some(r"a\.b".to_string()),
                }
            );
            // The remedy must survive rendering as the exact name to type.
            assert!(e.to_string().contains(r"`a\.b`"), "{e}");
        }
    }

    #[test]
    fn two_names_for_the_same_path_are_rejected() {
        assert_eq!(
            check_expandable(["a.b", "a.b"]).unwrap_err(),
            FieldPathError::DuplicatePath {
                first: "a.b".to_string(),
                second: "a.b".to_string(),
            }
        );
        // Distinct spellings of one path collide too: `[` is literal either way.
        assert_eq!(
            check_expandable(["a[b", r"a\[b"]).unwrap_err(),
            FieldPathError::DuplicatePath {
                first: "a[b".to_string(),
                second: r"a\[b".to_string(),
            }
        );
    }

    #[test]
    fn escaping_the_separator_avoids_the_collision() {
        assert!(check_expandable(["a.b", r"a\.b"]).is_ok());
        assert!(check_expandable([r"a\.b", "a", "a.b"]).is_err());
    }

    #[test]
    fn a_malformed_name_is_reported_by_the_expansion_check() {
        assert!(matches!(
            check_expandable(["ok", r"C:\temp"]).unwrap_err(),
            FieldPathError::UnknownEscape { .. }
        ));
    }
}
