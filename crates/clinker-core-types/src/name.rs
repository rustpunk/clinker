//! How diagnostics print a node name.
//!
//! Node names are unrestricted: an author may name a Source `it's src`, or
//! put a double quote or a backslash in a Sink's name. Every diagnostic that
//! quotes a node name prints it through [`QuoteName::quoted_name`], in double
//! quotes with Rust's string escaping of `"`, `\` and control characters,
//! so the name reads the same way it is written as a double-quoted YAML
//! scalar and can never be misread as ending early.

use std::fmt;

/// A name as a diagnostic prints it. See the [module docs](self).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuotedName<'a>(&'a str);

impl fmt::Display for QuotedName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

/// Print a name the way every diagnostic quotes a node name.
///
/// Implemented for `str`, so a `String`, `&str` or `Arc<str>` name calls it
/// through auto-deref: `format!("source {}", source.name.quoted_name())`.
pub trait QuoteName {
    /// The name in double quotes, escaped as a Rust string literal is.
    fn quoted_name(&self) -> QuotedName<'_>;
}

impl QuoteName for str {
    fn quoted_name(&self) -> QuotedName<'_> {
        QuotedName(self)
    }
}

#[cfg(test)]
mod tests {
    use super::QuoteName;

    #[test]
    fn a_plain_name_prints_in_double_quotes() {
        assert_eq!("orders".quoted_name().to_string(), r#""orders""#);
    }

    #[test]
    fn quotes_backslashes_and_control_characters_are_escaped() {
        assert_eq!("it's src".quoted_name().to_string(), r#""it's src""#);
        assert_eq!(r#"a"b\c"#.quoted_name().to_string(), r#""a\"b\\c""#);
        assert_eq!("tab\there".quoted_name().to_string(), r#""tab\there""#);
    }

    #[test]
    fn an_apostrophe_or_a_quote_in_a_name_stays_unambiguous() {
        assert_eq!("it's src".quoted_name().to_string(), r#""it's src""#);
        assert_eq!(r#"say "hi""#.quoted_name().to_string(), r#""say \"hi\"""#);
    }

    #[test]
    fn a_combining_accent_prints_as_written() {
        assert_eq!(
            "cafe\u{301}".quoted_name().to_string(),
            "\"cafe\u{301}\"",
            "a decomposed accent is part of the name as the author wrote it"
        );
    }

    #[test]
    fn an_invisible_character_is_shown_as_an_escape() {
        assert_eq!(
            "zero\u{200b}width".quoted_name().to_string(),
            r#""zero\u{200b}width""#
        );
    }

    #[test]
    fn owned_and_shared_names_call_it_through_deref() {
        let owned = String::from("sink");
        let shared: std::sync::Arc<str> = std::sync::Arc::from("src");
        assert_eq!(format!("{}", owned.quoted_name()), r#""sink""#);
        assert_eq!(format!("{}", shared.quoted_name()), r#""src""#);
    }
}
