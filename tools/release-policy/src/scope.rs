//! Classify a CI run as documentation-only or not.
//!
//! `ci.yml` skips the platform, portability, and lint work for a change that
//! touches only files under `docs/`. Rust tests and both policy tools read
//! files under `docs/`, so such a change still runs the Linux test suite and
//! the policy jobs; the workflow trust gate pins which jobs may skip.
//!
//! The classification fails open. Every case it cannot establish reports
//! `false`, which runs the full workflow.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use crate::child::{self, ChildSpec, Termination};
use crate::error::GateError;
use crate::limits::MAX_CHILD_OUTPUT_BYTES;

const GIT_DEADLINE: Duration = Duration::from_secs(60);
const DOCS_ROOT: &[u8] = b"docs/";

/// Report whether the change under test touches only files under `docs/`.
///
/// For `pull_request`, HEAD is the merge commit Actions checks out, so its
/// first parent is the tip of the base branch and the diff is exactly what the
/// pull request would change. For `push`, `before` is the previous tip of the
/// branch. Any other event, a missing or all-zero `before`, a base commit that
/// is not in the checkout, and a diff too large to retain all report `false`.
///
/// Runs `git` in the current directory and blocks until it exits; the checkout
/// must hold at least two commits for either base to be present.
pub fn docs_only(event: &str, before: Option<&str>) -> Result<bool, GateError> {
    let base = match (event, before) {
        ("pull_request", _) => "HEAD^1".to_owned(),
        ("push", Some(before))
            if before.len() == 40
                && before.bytes().all(|byte| byte.is_ascii_hexdigit())
                && before.bytes().any(|byte| byte != b'0') =>
        {
            before.to_owned()
        }
        _ => return Ok(false),
    };

    let verify = git(&[
        "rev-parse",
        "--verify",
        "--quiet",
        &format!("{base}^{{commit}}"),
    ])?;
    if verify.termination != Termination::Exited(Some(0)) {
        return Ok(false);
    }

    let diff = git(&["diff", "--no-renames", "--name-only", "-z", &base, "HEAD"])?;
    if diff.termination != Termination::Exited(Some(0)) {
        return Err(GateError::internal(
            "scope.diff",
            "git diff failed while classifying the change",
        ));
    }
    if diff.stdout_truncated {
        return Ok(false);
    }
    Ok(paths_are_docs_only(&diff.stdout))
}

/// Report whether a NUL-separated path list is non-empty and every path in it
/// is a file under `docs/`. `--no-renames` upstream means a file moved into
/// `docs/` also lists its old path.
fn paths_are_docs_only(list: &[u8]) -> bool {
    let mut paths = list
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .peekable();
    paths.peek().is_some()
        && paths.all(|path| path.len() > DOCS_ROOT.len() && path.starts_with(DOCS_ROOT))
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
    use super::paths_are_docs_only;

    fn list(paths: &[&str]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for path in paths {
            bytes.extend_from_slice(path.as_bytes());
            bytes.push(0);
        }
        bytes
    }

    #[test]
    fn only_paths_under_docs_are_documentation() {
        for (paths, expected) in [
            (&["docs/user/src/nodes/source.md"][..], true),
            (
                &[
                    "docs/user/src/SUMMARY.md",
                    "docs/engine/src/x-explainer.html",
                ][..],
                true,
            ),
            (&["docs/odd\nname.md"][..], true),
            (
                &[
                    "docs/user/src/nodes/source.md",
                    "crates/clinker/src/main.rs",
                ][..],
                false,
            ),
            (
                &["crates/cxl/src/lib.rs", "docs/ai/20_CRATE_MAP.md"][..],
                false,
            ),
            (&["examples/pipelines/orders.yaml"][..], false),
            (&["README.md"][..], false),
            (&["docsite/index.md"][..], false),
            (&["docs"][..], false),
            (&["docs/"][..], false),
            (&[".github/workflows/ci.yml"][..], false),
            (&["scripts/x\ndocs/y.md"][..], false),
            (&[][..], false),
        ] {
            assert_eq!(
                paths_are_docs_only(&list(paths)),
                expected,
                "paths: {paths:?}"
            );
        }
    }
}
