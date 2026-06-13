//! Opt-in HL7 v2 composite-field splitting.
//!
//! The positional reader keeps every field verbatim by default, so a
//! composite field such as the message type `ADT^A01` rides inside one
//! `fNN` column with its component separator intact. Some consumers want
//! component-level access (the message code `MSH-9.1` vs the trigger event
//! `MSH-9.2`) without writing CXL string-splitting downstream. A source may
//! therefore opt one or more fields into structural splitting: the reader
//! explodes the named field into per-repetition / per-component /
//! per-sub-component columns, and the writer re-assembles the exact wire
//! field from them.
//!
//! HL7 nests three structural axes inside a field, from outermost to
//! innermost: repetitions (`~`), components (`^`), and sub-components (`&`).
//! A split declaration fixes the column width on each axis (how many
//! repetitions / components / sub-components to expose), so the record
//! schema stays static — it never varies with per-record data. A field
//! whose data carries more structure on any axis than the declaration
//! reserves is rejected with guidance rather than silently truncated,
//! mirroring the `max_fields` overflow posture.
//!
//! ## Column naming
//!
//! Split columns name the path from the field down to a leaf with the
//! axis letters `r` (repetition), `c` (component), and `s` (sub-component),
//! all 1-based. The default axis index (1) is elided so the common
//! component-only case stays clean, and the writer defaults a missing axis
//! back to 1:
//!
//! | Declaration (`reps`,`comps`,`subs`) | Columns for `f08`                 |
//! | ----------------------------------- | --------------------------------- |
//! | `1, 2, 1`                           | `f08_c1`, `f08_c2`                 |
//! | `1, 1, 2`                           | `f08_c1_s1`, `f08_c1_s2`          |
//! | `2, 1, 1`                           | `f08_r1_c1`, `f08_r2_c1`          |
//!
//! The grammar is self-describing: the writer parses any structured `fNN_…`
//! column name back to its `(field, repetition, component, sub-component)`
//! coordinate without consulting the source split declaration, so an
//! HL7→HL7 round-trip re-assembles correctly from the column names alone.
//!
//! ## Round-trip fidelity
//!
//! Explode and re-assemble are exact inverses: the reader splits on the
//! discovered separators and the writer re-joins on them verbatim, never
//! escaping `^`/`~`/`&`. A split → re-assemble round-trip therefore yields
//! the byte-identical wire field, exactly as the verbatim positional model
//! does for an unsplit field.

use crate::error::FormatError;
use crate::hl7::tokenizer::{Delimiters, decode_escapes};

/// A resolved split declaration for one field position.
///
/// `field_index` is the 1-based wire field position (the `NN` of `fNN`,
/// e.g. `8` for `f08`). The three counts fix how many columns each
/// structural axis contributes; each is at least 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hl7FieldSplit {
    /// 1-based wire field position to explode (`f08` → 8).
    pub field_index: usize,
    /// Number of repetition columns to expose (the `~` axis). At least 1.
    pub repetitions: usize,
    /// Number of component columns to expose (the `^` axis). At least 1.
    pub components: usize,
    /// Number of sub-component columns to expose (the `&` axis). At least 1.
    pub subcomponents: usize,
}

impl Hl7FieldSplit {
    /// The total number of leaf columns this declaration contributes:
    /// `repetitions × components × subcomponents`.
    pub(crate) fn column_count(&self) -> usize {
        self.repetitions * self.components * self.subcomponents
    }

    /// The column names this declaration contributes, in stable iteration
    /// order (repetition-major, then component, then sub-component). The
    /// order matches [`Self::split_value`] so a reader can zip names to
    /// values.
    pub(crate) fn column_names(&self) -> Vec<String> {
        let mut names = Vec::with_capacity(self.column_count());
        for r in 1..=self.repetitions {
            for c in 1..=self.components {
                for s in 1..=self.subcomponents {
                    names.push(self.column_name(r, c, s));
                }
            }
        }
        names
    }

    /// The column name for the leaf at 1-based `(repetition, component,
    /// sub-component)`. The default index (1) on the repetition and
    /// sub-component axes is elided; the component axis is always present so
    /// the name is never a bare `fNN` that would collide with the verbatim
    /// positional column.
    fn column_name(&self, rep: usize, comp: usize, sub: usize) -> String {
        let mut name = format!("f{:02}", self.field_index);
        if self.repetitions > 1 {
            name.push_str(&format!("_r{rep}"));
        }
        name.push_str(&format!("_c{comp}"));
        if self.subcomponents > 1 {
            name.push_str(&format!("_s{sub}"));
        }
        name
    }

    /// Explode a *raw* (still-escaped) field value into its decoded leaf
    /// values in the same order as [`Self::column_names`]. An absent leaf
    /// (the data carries fewer repetitions / components / sub-components than
    /// declared, or an empty segment) yields `None`; a present leaf yields
    /// its decoded text.
    ///
    /// The split runs on the raw bytes so the structural `~`/`^`/`&`
    /// separators bound the leaves before [`decode_escapes`] turns an escaped
    /// `\S\` into a literal `^` *inside* a leaf — splitting the decoded value
    /// would wrongly treat that data `^` as a component boundary. Each leaf
    /// is then decoded through the shared escape decoder, so the escape rules
    /// stay in one place.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Hl7`] when the field data carries more
    /// structure on any axis than the declaration reserves — splitting it
    /// would drop the overflow, so the reader rejects it with guidance to
    /// widen the split width, mirroring the `max_fields` overflow posture.
    pub(crate) fn split_value(
        &self,
        raw_value: &str,
        delims: &Delimiters,
    ) -> Result<Vec<Option<String>>, FormatError> {
        let repetitions = split_on(raw_value, delims.repetition as char);
        if repetitions.len() > self.repetitions {
            return Err(self.overflow("repetition", repetitions.len(), self.repetitions));
        }
        let mut out = Vec::with_capacity(self.column_count());
        for r in 0..self.repetitions {
            let rep = repetitions.get(r).copied();
            let components = rep.map(|t| split_on(t, delims.component as char));
            if let Some(comps) = &components
                && comps.len() > self.components
            {
                return Err(self.overflow("component", comps.len(), self.components));
            }
            for c in 0..self.components {
                let comp = components.as_ref().and_then(|cs| cs.get(c).copied());
                let subs = comp.map(|t| split_on(t, delims.subcomponent as char));
                if let Some(ss) = &subs
                    && ss.len() > self.subcomponents
                {
                    return Err(self.overflow("sub-component", ss.len(), self.subcomponents));
                }
                for s in 0..self.subcomponents {
                    let leaf = subs.as_ref().and_then(|ss| ss.get(s).copied());
                    out.push(leaf.map(|raw| decode_escapes(raw.as_bytes(), delims)));
                }
            }
        }
        Ok(out)
    }

    /// Build an overflow error naming the axis and the offending counts.
    fn overflow(&self, axis: &str, found: usize, declared: usize) -> FormatError {
        FormatError::Hl7(format!(
            "split field f{:02} carries {found} {axis}s, exceeding the configured {axis} width \
             of {declared}; raise the field's `{axis}s` split count or leave the field unsplit",
            self.field_index
        ))
    }
}

/// Split a string on a single-byte separator into its parts, preserving
/// empty parts (`A^^C` → `["A", "", "C"]`). A field with no separator is a
/// single part.
fn split_on(value: &str, sep: char) -> Vec<&str> {
    value.split(sep).collect()
}

/// A structured split column resolved to its wire coordinate.
///
/// Parsed from a column name by [`parse_split_column`]; consumed by the
/// writer to re-assemble each `fNN` wire field from its leaf columns. All
/// indices are 1-based.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SplitCoord {
    /// 1-based wire field position (`f08_c2` → 8).
    pub field_index: usize,
    /// 1-based repetition index (the `r` axis; defaults to 1 when elided).
    pub repetition: usize,
    /// 1-based component index (the `c` axis; always present).
    pub component: usize,
    /// 1-based sub-component index (the `s` axis; defaults to 1 when elided).
    pub subcomponent: usize,
}

/// Parse a structured split column name (`f08_c2`, `f03_r2_c1_s3`) into its
/// wire coordinate, or `None` when the name is not a split column.
///
/// A split column is `f` + the field digits + a mandatory `_c<n>` component
/// segment, with an optional leading `_r<n>` repetition segment and an
/// optional trailing `_s<n>` sub-component segment, each 1-based and at
/// least 1. The grammar is the exact inverse of
/// [`Hl7FieldSplit::column_name`], so the writer recovers the coordinate
/// without consulting the source split declaration.
pub(crate) fn parse_split_column(name: &str) -> Option<SplitCoord> {
    let rest = name.strip_prefix('f')?;
    // The field index runs up to the first axis underscore.
    let (field_digits, mut tail) = match rest.find('_') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => return None,
    };
    let field_index = parse_index(field_digits)?;

    let mut repetition = 1;
    let mut component = None;
    let mut subcomponent = 1;

    if let Some(after) = tail.strip_prefix("_r") {
        let (digits, next) = split_axis(after);
        repetition = parse_index(digits)?;
        tail = next;
    }
    if let Some(after) = tail.strip_prefix("_c") {
        let (digits, next) = split_axis(after);
        component = Some(parse_index(digits)?);
        tail = next;
    }
    if let Some(after) = tail.strip_prefix("_s") {
        let (digits, next) = split_axis(after);
        subcomponent = parse_index(digits)?;
        tail = next;
    }
    // A trailing remainder means the name carries an axis segment out of
    // order or an unknown axis letter — not a split column this writer maps.
    if !tail.is_empty() {
        return None;
    }
    Some(SplitCoord {
        field_index,
        repetition,
        component: component?,
        subcomponent,
    })
}

/// Split an axis segment's leading digit run from any following `_`-prefixed
/// segment. `2_c1` → (`2`, `_c1`); `1` → (`1`, ``).
fn split_axis(after: &str) -> (&str, &str) {
    let end = after
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(after.len());
    (&after[..end], &after[end..])
}

/// Parse a 1-based axis index: a non-empty all-ASCII-digit run that is at
/// least 1. Rejects `0` and an empty run so a malformed name is not a split
/// column.
fn parse_index(digits: &str) -> Option<usize> {
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: usize = digits.parse().ok()?;
    (n >= 1).then_some(n)
}

/// Re-assemble a field's wire text from its split leaf coordinates.
///
/// `leaves` pairs each leaf's coordinate with its text; the field is rebuilt
/// by joining sub-components on `&`, components on `^`, and repetitions on
/// `~`, with trailing empty segments on every axis trimmed so the
/// re-assembled field carries no fabricated separators — exactly the bytes
/// the reader split. The separators are written verbatim (never escaped),
/// preserving round-trip fidelity.
pub(crate) fn reassemble_field(leaves: &[(SplitCoord, String)], delims: &Delimiters) -> String {
    // Bucket leaves into a [repetition][component][subcomponent] grid sized
    // to the maximum coordinate seen, then trim empties per axis.
    let max_rep = leaves.iter().map(|(c, _)| c.repetition).max().unwrap_or(1);
    let max_comp = leaves.iter().map(|(c, _)| c.component).max().unwrap_or(1);
    let max_sub = leaves
        .iter()
        .map(|(c, _)| c.subcomponent)
        .max()
        .unwrap_or(1);

    let mut grid = vec![vec![vec![String::new(); max_sub]; max_comp]; max_rep];
    for (coord, text) in leaves {
        grid[coord.repetition - 1][coord.component - 1][coord.subcomponent - 1] = text.clone();
    }

    let component_sep = delims.component as char;
    let subcomponent_sep = delims.subcomponent as char;
    let repetition_sep = delims.repetition as char;

    let reps: Vec<String> = grid
        .into_iter()
        .map(|components| {
            let comps: Vec<String> = components
                .into_iter()
                .map(|subs| join_trimmed(subs, subcomponent_sep))
                .collect();
            join_trimmed(comps, component_sep)
        })
        .collect();
    join_trimmed(reps, repetition_sep)
}

/// Join parts on a separator after dropping trailing empty parts, so no
/// fabricated trailing separator survives. An all-empty list joins to the
/// empty string.
fn join_trimmed(mut parts: Vec<String>, sep: char) -> String {
    while matches!(parts.last(), Some(s) if s.is_empty()) {
        parts.pop();
    }
    parts.join(&sep.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delims() -> Delimiters {
        Delimiters::default_set()
    }

    fn split(field_index: usize, reps: usize, comps: usize, subs: usize) -> Hl7FieldSplit {
        Hl7FieldSplit {
            field_index,
            repetitions: reps,
            components: comps,
            subcomponents: subs,
        }
    }

    #[test]
    fn component_columns_named_cleanly() {
        let s = split(8, 1, 2, 1);
        assert_eq!(s.column_names(), vec!["f08_c1", "f08_c2"]);
    }

    #[test]
    fn subcomponent_columns_carry_s_axis() {
        let s = split(3, 1, 1, 2);
        assert_eq!(s.column_names(), vec!["f03_c1_s1", "f03_c1_s2"]);
    }

    #[test]
    fn repetition_columns_carry_r_axis() {
        let s = split(3, 2, 1, 1);
        assert_eq!(s.column_names(), vec!["f03_r1_c1", "f03_r2_c1"]);
    }

    #[test]
    fn full_grid_orders_rep_comp_sub() {
        let s = split(3, 2, 2, 2);
        assert_eq!(
            s.column_names(),
            vec![
                "f03_r1_c1_s1",
                "f03_r1_c1_s2",
                "f03_r1_c2_s1",
                "f03_r1_c2_s2",
                "f03_r2_c1_s1",
                "f03_r2_c1_s2",
                "f03_r2_c2_s1",
                "f03_r2_c2_s2",
            ]
        );
    }

    #[test]
    fn split_value_explodes_components() {
        let s = split(8, 1, 2, 1);
        let leaves = s.split_value("ADT^A01", &delims()).unwrap();
        assert_eq!(leaves, vec![Some("ADT".to_owned()), Some("A01".to_owned())]);
    }

    #[test]
    fn split_value_pads_absent_leaves_with_none() {
        // Field has one component; the second declared column is absent.
        let s = split(8, 1, 2, 1);
        let leaves = s.split_value("ADT", &delims()).unwrap();
        assert_eq!(leaves, vec![Some("ADT".to_owned()), None]);
    }

    #[test]
    fn split_value_keeps_empty_interior_component() {
        // `PATID^^^MRN` has empty interior components that must survive.
        let s = split(3, 1, 4, 1);
        let leaves = s.split_value("PATID^^^MRN", &delims()).unwrap();
        assert_eq!(
            leaves,
            vec![
                Some("PATID".to_owned()),
                Some(String::new()),
                Some(String::new()),
                Some("MRN".to_owned()),
            ]
        );
    }

    #[test]
    fn split_value_explodes_subcomponents() {
        let s = split(3, 1, 1, 2);
        let leaves = s.split_value("A&B", &delims()).unwrap();
        assert_eq!(leaves, vec![Some("A".to_owned()), Some("B".to_owned())]);
    }

    #[test]
    fn split_value_explodes_repetitions() {
        let s = split(3, 2, 1, 1);
        let leaves = s.split_value("ID1~ID2", &delims()).unwrap();
        assert_eq!(leaves, vec![Some("ID1".to_owned()), Some("ID2".to_owned())]);
    }

    #[test]
    fn escaped_component_separator_does_not_split() {
        // `A\S\B` is a single component carrying a literal `^` in data; it
        // must stay one leaf and decode to `A^B`, not split into two.
        let s = split(8, 1, 2, 1);
        let leaves = s.split_value("A\\S\\B", &delims()).unwrap();
        assert_eq!(leaves, vec![Some("A^B".to_owned()), None]);
    }

    #[test]
    fn leaf_decodes_escaped_field_separator() {
        // A `\F\` inside a component decodes to a literal `|` in the leaf.
        let s = split(8, 1, 1, 1);
        let leaves = s.split_value("a\\F\\b", &delims()).unwrap();
        assert_eq!(leaves, vec![Some("a|b".to_owned())]);
    }

    #[test]
    fn component_overflow_errors() {
        let s = split(8, 1, 1, 1);
        let err = s.split_value("ADT^A01", &delims()).unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("component")));
    }

    #[test]
    fn repetition_overflow_errors() {
        let s = split(3, 1, 1, 1);
        let err = s.split_value("ID1~ID2", &delims()).unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("repetition")));
    }

    #[test]
    fn parse_component_column() {
        assert_eq!(
            parse_split_column("f08_c2"),
            Some(SplitCoord {
                field_index: 8,
                repetition: 1,
                component: 2,
                subcomponent: 1,
            })
        );
    }

    #[test]
    fn parse_full_path_column() {
        assert_eq!(
            parse_split_column("f03_r2_c1_s3"),
            Some(SplitCoord {
                field_index: 3,
                repetition: 2,
                component: 1,
                subcomponent: 3,
            })
        );
    }

    #[test]
    fn parse_rejects_plain_positional_column() {
        // A bare `fNN` is the verbatim positional column, not a split leaf.
        assert_eq!(parse_split_column("f08"), None);
    }

    #[test]
    fn parse_rejects_missing_component_axis() {
        // A name with a repetition but no component axis is not a leaf.
        assert_eq!(parse_split_column("f03_r2"), None);
    }

    #[test]
    fn parse_rejects_index_zero() {
        assert_eq!(parse_split_column("f08_c0"), None);
    }

    #[test]
    fn parse_rejects_unknown_trailing_axis() {
        assert_eq!(parse_split_column("f08_c1_x2"), None);
    }

    #[test]
    fn reassemble_components() {
        let leaves = vec![
            (coord(8, 1, 1, 1), "ADT".to_owned()),
            (coord(8, 1, 2, 1), "A01".to_owned()),
        ];
        assert_eq!(reassemble_field(&leaves, &delims()), "ADT^A01");
    }

    #[test]
    fn reassemble_preserves_empty_interior_components() {
        let leaves = vec![
            (coord(3, 1, 1, 1), "PATID".to_owned()),
            (coord(3, 1, 2, 1), String::new()),
            (coord(3, 1, 3, 1), String::new()),
            (coord(3, 1, 4, 1), "MRN".to_owned()),
        ];
        assert_eq!(reassemble_field(&leaves, &delims()), "PATID^^^MRN");
    }

    #[test]
    fn reassemble_trims_trailing_empty_components() {
        let leaves = vec![
            (coord(8, 1, 1, 1), "ADT".to_owned()),
            (coord(8, 1, 2, 1), String::new()),
        ];
        // The trailing empty component is trimmed — no fabricated `^`.
        assert_eq!(reassemble_field(&leaves, &delims()), "ADT");
    }

    #[test]
    fn reassemble_repetitions_and_subcomponents() {
        let leaves = vec![
            (coord(3, 1, 1, 1), "A".to_owned()),
            (coord(3, 1, 1, 2), "B".to_owned()),
            (coord(3, 2, 1, 1), "C".to_owned()),
        ];
        assert_eq!(reassemble_field(&leaves, &delims()), "A&B~C");
    }

    fn coord(field_index: usize, rep: usize, comp: usize, sub: usize) -> SplitCoord {
        SplitCoord {
            field_index,
            repetition: rep,
            component: comp,
            subcomponent: sub,
        }
    }

    #[test]
    fn explode_reassemble_round_trips() {
        // A full grid of declared widths must explode and re-assemble to the
        // byte-identical original, with the discovered separators verbatim.
        let s = split(3, 2, 3, 2);
        let original = "PATID&X^^MRN~ALT&Y^^MR";
        let leaves = s.split_value(original, &delims()).unwrap();
        let names = s.column_names();
        let paired: Vec<(SplitCoord, String)> = names
            .iter()
            .zip(leaves)
            .filter_map(|(name, leaf)| leaf.map(|text| (parse_split_column(name).unwrap(), text)))
            .collect();
        assert_eq!(reassemble_field(&paired, &delims()), original);
    }
}
