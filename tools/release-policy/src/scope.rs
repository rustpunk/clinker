//! Classify the change a CI run is validating.
//!
//! `ci.yml` skips the platform, portability, and lint work for a change that
//! touches only files under `docs/`. Rust code reads `docs/explain/` (compiled
//! into the binary) and `docs/ai/` (the crate map and architecture pages), so a
//! change there still runs the Linux test suite. A change confined to the books
//! and their theme (`docs/user/`, `docs/engine/`, `docs/theme/`) skips the Rust
//! test suite too. The workflow trust gate pins which jobs and steps may skip.
//!
//! The classification fails open. Every case it cannot establish reports
//! `false`, which runs the full workflow; a `git` process that cannot start or
//! a diff that fails is an error, which fails the scope job and leaves its
//! output empty, so the full workflow still runs.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use crate::child::{self, ChildSpec, Termination};
use crate::error::GateError;
use crate::limits::MAX_CHILD_OUTPUT_BYTES;

const GIT_DEADLINE: Duration = Duration::from_secs(60);
const DOCS_ROOT: &[u8] = b"docs/";
/// Directories under `docs/` that no Rust code reads.
const BOOK_ROOTS: [&[u8]; 3] = [b"docs/user/", b"docs/engine/", b"docs/theme/"];

/// What a change touches, as two GitHub Actions outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scope {
    /// Every path is under `docs/`.
    pub docs_only: bool,
    /// Every path is under a book root. Implies `docs_only`.
    pub book_only: bool,
}

impl Scope {
    const CODE: Self = Self {
        docs_only: false,
        book_only: false,
    };
}

/// Classify the change under test.
///
/// For `pull_request`, HEAD is the merge commit Actions checks out, so its
/// first parent is the tip of the base branch and the diff is exactly what the
/// pull request would change. For `push`, `before` is the previous tip of the
/// branch. Any other event, a missing or all-zero `before`, a base commit that
/// is not in the checkout, an empty diff, and a diff too large to retain all
/// report a code change.
///
/// Runs `git` in the current directory and blocks until it exits; the checkout
/// must hold at least two commits for either base to be present.
pub fn classify(event: &str, before: Option<&str>) -> Result<Scope, GateError> {
    let base = match (event, before) {
        ("pull_request", _) => "HEAD^1".to_owned(),
        ("push", Some(before))
            if before.len() == 40
                && before.bytes().all(|byte| byte.is_ascii_hexdigit())
                && before.bytes().any(|byte| byte != b'0') =>
        {
            before.to_owned()
        }
        _ => return Ok(Scope::CODE),
    };

    let verify = git(&[
        "rev-parse",
        "--verify",
        "--quiet",
        &format!("{base}^{{commit}}"),
    ])?;
    if verify.termination != Termination::Exited(Some(0)) {
        return Ok(Scope::CODE);
    }

    let diff = git(&["diff", "--no-renames", "--name-only", "-z", &base, "HEAD"])?;
    if diff.termination != Termination::Exited(Some(0)) {
        return Err(GateError::internal(
            "scope.diff",
            "git diff failed while classifying the change",
        ));
    }
    if diff.stdout_truncated {
        return Ok(Scope::CODE);
    }
    Ok(scope_of(&diff.stdout))
}

/// Classify a NUL-separated path list. An empty list is a code change.
/// `--no-renames` upstream means a file moved into `docs/` also lists its old
/// path.
fn scope_of(list: &[u8]) -> Scope {
    let paths = list
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return Scope::CODE;
    }
    let under = |root: &[u8], path: &[u8]| path.len() > root.len() && path.starts_with(root);
    let docs_only = paths.iter().all(|path| under(DOCS_ROOT, path));
    let book_only = paths
        .iter()
        .all(|path| BOOK_ROOTS.iter().any(|root| under(root, path)));
    Scope {
        docs_only,
        book_only,
    }
}

fn git(arguments: &[&str]) -> Result<child::ChildResult, GateError> {
    let mut environment = BTreeMap::new();
    for name in ["PATH", "TMPDIR"] {
        if let Some(value) = std::env::var_os(name) {
            environment.insert(OsString::from(name), value);
        }
    }
    child::run(ChildSpec {
        program: PathBuf::from("git"),
        arguments: arguments.iter().map(OsString::from).collect(),
        environment,
        timeout: GIT_DEADLINE,
        output_limit: MAX_CHILD_OUTPUT_BYTES,
    })
}

#[cfg(test)]
mod tests {
    use super::{Scope, scope_of};

    fn list(paths: &[&str]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for path in paths {
            bytes.extend_from_slice(path.as_bytes());
            bytes.push(0);
        }
        bytes
    }

    #[test]
    fn book_pages_docs_and_code_classify_by_folder() {
        let book = Scope {
            docs_only: true,
            book_only: true,
        };
        let docs = Scope {
            docs_only: true,
            book_only: false,
        };
        let code = Scope {
            docs_only: false,
            book_only: false,
        };
        for (paths, expected) in [
            (&["docs/user/src/nodes/source.md"][..], book),
            (
                &[
                    "docs/user/src/SUMMARY.md",
                    "docs/engine/src/x-explainer.html",
                    "docs/theme/css/general.css",
                ][..],
                book,
            ),
            (&["docs/user/src/odd\nname.md"][..], book),
            (&["docs/explain/E200.md"][..], docs),
            (&["docs/ai/20_CRATE_MAP.md"][..], docs),
            (&["docs/user/src/page.md", "docs/explain/E200.md"][..], docs),
            (&["docs/new-area/page.md"][..], docs),
            (&["docs/user"][..], docs),
            (&["docs/user/"][..], docs),
            (
                &[
                    "docs/user/src/nodes/source.md",
                    "crates/clinker/src/main.rs",
                ][..],
                code,
            ),
            (
                &["crates/cxl/src/lib.rs", "docs/ai/20_CRATE_MAP.md"][..],
                code,
            ),
            (&["examples/pipelines/orders.yaml"][..], code),
            (&["README.md"][..], code),
            (&["docsite/index.md"][..], code),
            (&["docs"][..], code),
            (&["docs/"][..], code),
            (&[".github/workflows/ci.yml"][..], code),
            (&["scripts/x\ndocs/user/y.md"][..], code),
            (&[][..], code),
        ] {
            assert_eq!(scope_of(&list(paths)), expected, "paths: {paths:?}");
        }
    }
}
