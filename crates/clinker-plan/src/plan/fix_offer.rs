//! Fixes the engine generates for the author to paste, shown only once
//! they have been checked.
//!
//! A diagnostic's help may print YAML or a CXL line written for the author
//! to paste: the binding to add at a call, the `input:` line that breaks a
//! cycle, a node's line rewritten. Such a fix is shown only after the engine
//! has tried it where it applies: the edit is bound or typechecked in scratch
//! state under the same rules as a real compile, and it must clear the error
//! it was written for without raising one the compile did not already raise.
//! A fix that does not check out is replaced by one plain sentence that says
//! what to change, with no code.
//!
//! The trial covers the call or nodes the fix is for, not the whole
//! pipeline: another call of the same composition, or a node that reads a
//! column or node the fix changes, can still fail once the fix is pasted,
//! and the next compile reports it.
//!
//! [`offer_fix`] is the one place a generated fix becomes help text. A
//! [`PasteFix`] has no other way out, so a fix cannot reach a diagnostic
//! without being checked.

use clinker_core_types::Diagnostic;
use clinker_core_types::diagnostic::Severity;

/// A fix written for the author to paste: the sentence that leads to it,
/// and the YAML or CXL itself.
pub(crate) struct PasteFix {
    lead: String,
    paste: String,
}

impl PasteFix {
    /// `lead` says what the fix does and where it goes; `paste` is the
    /// text to paste, one line or several.
    pub(crate) fn new(lead: impl Into<String>, paste: impl Into<String>) -> Self {
        Self {
            lead: lead.into(),
            paste: paste.into(),
        }
    }
}

/// The help for a diagnostic whose fix the engine can generate: the first
/// of `candidates` whose edit `check` says compiles and clears the error,
/// and otherwise (or when there is none) `plain`, one sentence naming what
/// to change. `after` follows the fix or the sentence: a sentence that
/// opens with `. `, or nothing.
///
/// Each candidate is a fix to print and the edit it stands for. `check`
/// runs on the edits in order until one passes; it is where the edit is
/// bound or typechecked in scratch state, and [`fix_resolves`] says what a
/// tried edit has to show.
///
/// A one-line fix is quoted at the end of its lead, and `after` follows
/// the closing quote. A fix of several lines is a block of its own below
/// the lead, and `after` starts a new line below the block, so the block
/// holds only what to paste and can be pasted as printed.
pub(crate) fn offer_fix<E>(
    plain: impl Into<String>,
    after: &str,
    candidates: impl IntoIterator<Item = (PasteFix, E)>,
    mut check: impl FnMut(&E) -> bool,
) -> String {
    for (fix, edit) in candidates {
        if check(&edit) {
            if !fix.paste.contains('\n') {
                return format!("{}: `{}`{after}", fix.lead, fix.paste);
            }
            let mut help = format!("{}:\n{}", fix.lead, fix.paste.trim_end_matches('\n'));
            let sentence = after.trim_start_matches(['.', ' ']);
            if !sentence.is_empty() {
                help.push('\n');
                help.push_str(sentence);
            }
            return help;
        }
    }
    format!("{}{after}", plain.into())
}

/// Whether an edit, tried, resolves `target`: the tried compile no longer
/// reports `target`, and every error it reports was already reported by
/// the compile the edit was tried against (`before`).
///
/// Diagnostics are compared by code and message, the text the author reads.
/// Warnings the tried compile raises do not count against the edit, except
/// `target` itself when it is one.
pub(crate) fn fix_resolves(
    target: &Diagnostic,
    before: &[Diagnostic],
    tried: &[Diagnostic],
) -> bool {
    let same = |a: &Diagnostic, b: &Diagnostic| a.code == b.code && a.message == b.message;
    !tried.iter().any(|d| same(d, target))
        && tried
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .all(|d| before.iter().any(|seen| same(seen, d)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_core_types::LabeledSpan;
    use clinker_core_types::span::Span;

    fn error(code: &str, message: &str) -> Diagnostic {
        Diagnostic::error(code, message, LabeledSpan::primary(Span::SYNTHETIC, ""))
    }

    #[test]
    fn the_first_fix_that_checks_out_is_shown_and_none_leaves_the_sentence() {
        let candidates = || {
            ["src", "dep"].map(|node| {
                (
                    PasteFix::new("add under `inputs:`", format!("opt: {node}")),
                    node,
                )
            })
        };
        let shown = offer_fix("bind the port", "", candidates(), |node| *node == "dep");
        assert_eq!(shown, "add under `inputs:`: `opt: dep`");
        let mut tried = Vec::new();
        let refused = offer_fix("bind the port", "", candidates(), |node| {
            tried.push(*node);
            false
        });
        assert_eq!(refused, "bind the port");
        assert_eq!(tried, ["src", "dep"]);
        let none = offer_fix("bind the port", "", Vec::<(PasteFix, ())>::new(), |_| {
            panic!("nothing to check")
        });
        assert_eq!(none, "bind the port");
    }

    #[test]
    fn a_several_line_fix_is_printed_below_its_lead() {
        let shown = offer_fix(
            "plain",
            "",
            [(PasteFix::new("add", "- type: transform\n  name: t"), ())],
            |_| true,
        );
        assert_eq!(shown, "add:\n- type: transform\n  name: t");
    }

    #[test]
    fn what_follows_a_block_starts_a_new_line_and_follows_a_line_or_sentence() {
        let after = ". A port admits the type it declares";
        let block = offer_fix(
            "plain",
            after,
            [(
                PasteFix::new("add", "- type: transform\n  cxl: emit v = v.to_float()"),
                (),
            )],
            |_| true,
        );
        assert_eq!(
            block,
            "add:\n- type: transform\n  cxl: emit v = v.to_float()\nA port admits the type it declares",
            "the block's last line holds only what to paste"
        );
        let line = offer_fix(
            "plain",
            after,
            [(PasteFix::new("write", "emit v = v.to_float()"), ())],
            |_| true,
        );
        assert_eq!(
            line,
            "write: `emit v = v.to_float()`. A port admits the type it declares"
        );
        let sentence = offer_fix(
            "convert `v`",
            after,
            [(PasteFix::new("write", "emit v = v.to_float()"), ())],
            |_| false,
        );
        assert_eq!(sentence, "convert `v`. A port admits the type it declares");
    }

    #[test]
    fn an_edit_resolves_its_error_only_when_it_clears_it_and_raises_nothing_new() {
        let target = error("E104", "port opt unbound");
        let other = error("E169", "column x undeclared");
        let before = vec![target.clone(), other.clone()];
        assert!(fix_resolves(&target, &before, &[]));
        assert!(fix_resolves(&target, &before, std::slice::from_ref(&other)));
        assert!(!fix_resolves(
            &target,
            &before,
            std::slice::from_ref(&target)
        ));
        assert!(!fix_resolves(
            &target,
            &before,
            &[error("E102", "port opt needs column label")]
        ));
    }
}
