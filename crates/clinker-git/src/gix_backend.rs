//! Git CLI backend — reliable git operations via the system `git` binary.
//!
//! Uses `git` CLI for all operations. This ensures credential helper
//! integration works out of the box. The `GitOps` trait is designed
//! so a future gix (gitoxide) backend can be swapped in for read
//! operations without changing callers.
//!
//! Spec: clinker-kiln-git-addendum.md §G2.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::ops::{GitError, GitOps};
use crate::types::*;

/// Git CLI-based operations.
pub struct GitCliOps {
    /// Path to the repository root (containing `.git`).
    repo_path: PathBuf,
    /// Path to the git binary.
    git_binary: String,
}

impl GitCliOps {
    /// Discover a git repository from a path (walks ancestors via `git rev-parse`).
    pub fn discover(path: &Path) -> Result<Self, GitError> {
        Self::discover_with_binary(path, "git")
    }

    /// Discover with a custom git binary path.
    pub fn discover_with_binary(path: &Path, git_binary: &str) -> Result<Self, GitError> {
        let output = Command::new(git_binary)
            .args(["rev-parse", "--show-toplevel"])
            .current_dir(path)
            .output()
            .map_err(|e| GitError::Cli(format!("failed to run git: {e}")))?;

        if !output.status.success() {
            return Err(GitError::NoRepo(format!(
                "no git repository at {}",
                path.display()
            )));
        }

        let root = String::from_utf8_lossy(&output.stdout).trim().to_string();

        Ok(Self {
            repo_path: PathBuf::from(root),
            git_binary: git_binary.to_string(),
        })
    }

    /// Get the repository root path.
    pub fn root(&self) -> &Path {
        &self.repo_path
    }

    /// Run a git command and return stdout.
    fn git(&self, args: &[&str]) -> Result<String, GitError> {
        let output = Command::new(&self.git_binary)
            .args(args)
            .current_dir(&self.repo_path)
            .output()
            .map_err(|e| GitError::Cli(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(GitError::Operation(stderr));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

impl GitOps for GitCliOps {
    fn status(&self) -> Result<RepoStatus, GitError> {
        let branch = self.current_branch()?;

        // Get ahead/behind counts
        let (ahead, behind) = self.ahead_behind()?;

        // Get file statuses
        let porcelain = self.git(&["status", "--porcelain=v1"])?;
        let files = parse_porcelain_status(&porcelain);

        Ok(RepoStatus::from_files(branch, ahead, behind, files))
    }

    fn branches(&self) -> Result<Vec<BranchInfo>, GitError> {
        let _current = self.current_branch().unwrap_or_default();

        let output = self.git(&["branch", "--list", "--no-color"])?;
        let mut branches = Vec::new();

        for line in output.lines() {
            let is_current = line.starts_with('*');
            let name = line.trim_start_matches(['*', ' ']).trim().to_string();
            if name.is_empty() {
                continue;
            }

            branches.push(BranchInfo {
                is_current,
                name,
                ahead: 0,
                behind: 0,
            });
        }

        // Fill in ahead/behind for current branch
        if let Some(current_branch) = branches.iter_mut().find(|b| b.is_current) {
            let (ahead, behind) = self.ahead_behind().unwrap_or((0, 0));
            current_branch.ahead = ahead;
            current_branch.behind = behind;
        }

        Ok(branches)
    }

    fn current_branch(&self) -> Result<String, GitError> {
        let output = self.git(&["branch", "--show-current"])?;
        let branch = output.trim().to_string();
        if branch.is_empty() {
            Ok("HEAD (detached)".to_string())
        } else {
            Ok(branch)
        }
    }

    fn log(&self, max: usize) -> Result<Vec<CommitInfo>, GitError> {
        let max_str = format!("-{max}");
        let output = self.git(&["log", &max_str, "--format=%H%n%an%n%ae%n%at%n%s%n---END---"])?;

        let mut commits = Vec::new();
        let mut lines = output.lines().peekable();

        while lines.peek().is_some() {
            let id = match lines.next() {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => break,
            };
            let author = lines.next().unwrap_or("").to_string();
            let email = lines.next().unwrap_or("").to_string();
            let timestamp: i64 = lines.next().unwrap_or("0").parse().unwrap_or(0);
            let subject = lines.next().unwrap_or("").to_string();

            // Skip until ---END---
            for line in lines.by_ref() {
                if line == "---END---" {
                    break;
                }
            }

            commits.push(CommitInfo {
                id,
                author,
                email,
                timestamp,
                subject,
                body: None,
            });
        }

        Ok(commits)
    }

    fn stage(&self, paths: &[&Path]) -> Result<(), GitError> {
        let mut args = vec!["add", "--"];
        let path_strs: Vec<String> = paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        for p in &path_strs {
            args.push(p);
        }
        self.git(&args)?;
        Ok(())
    }

    fn unstage(&self, paths: &[&Path]) -> Result<(), GitError> {
        let mut args = vec!["restore", "--staged", "--"];
        let path_strs: Vec<String> = paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        for p in &path_strs {
            args.push(p);
        }
        self.git(&args)?;
        Ok(())
    }

    fn commit(&self, message: &str) -> Result<CommitInfo, GitError> {
        self.git(&["commit", "-m", message])?;
        // Get the just-created commit info
        let log = self.log(1)?;
        log.into_iter()
            .next()
            .ok_or_else(|| GitError::Operation("commit created but log empty".to_string()))
    }

    fn push(&self) -> Result<String, GitError> {
        self.git(&["push"])
    }

    fn pull(&self) -> Result<String, GitError> {
        self.git(&["pull"])
    }

    fn fetch(&self) -> Result<String, GitError> {
        self.git(&["fetch"])
    }

    fn diff_file(&self, path: &Path) -> Result<String, GitError> {
        let path_str = path.to_string_lossy();
        self.git(&["diff", "--", &path_str])
    }

    fn stage_all(&self) -> Result<(), GitError> {
        self.git(&["add", "-A"])?;
        Ok(())
    }

    fn blame(&self, path: &Path) -> Result<Vec<BlameLine>, GitError> {
        let path_str = path.to_string_lossy();
        let output = self.git(&["blame", "--porcelain", &path_str])?;

        Ok(parse_blame_porcelain(&output))
    }
}

impl GitCliOps {
    /// Get ahead/behind counts relative to upstream.
    fn ahead_behind(&self) -> Result<(usize, usize), GitError> {
        let output = self.git(&["rev-list", "--left-right", "--count", "HEAD...@{upstream}"]);

        match output {
            Ok(s) => {
                let parts: Vec<&str> = s.trim().split('\t').collect();
                if parts.len() == 2 {
                    let ahead = parts[0].parse().unwrap_or(0);
                    let behind = parts[1].parse().unwrap_or(0);
                    Ok((ahead, behind))
                } else {
                    Ok((0, 0))
                }
            }
            Err(_) => Ok((0, 0)), // No upstream configured
        }
    }
}

// ── Parsing helpers ─────────────────────────────────────────────────────

/// Parse `git status --porcelain=v1` output.
fn parse_porcelain_status(output: &str) -> Vec<FileStatus> {
    let mut files = Vec::new();

    for line in output.lines() {
        if line.len() < 4 {
            continue;
        }

        let index_status = line.as_bytes()[0];
        let worktree_status = line.as_bytes()[1];
        let path = &line[3..];

        let status = match (index_status, worktree_status) {
            (b'?', b'?') => StatusKind::Untracked,
            (b'A', _) | (_, b'A') => StatusKind::Added,
            (b'D', _) | (_, b'D') => StatusKind::Deleted,
            (b'R', _) | (_, b'R') => StatusKind::Renamed,
            (b'M', _) | (_, b'M') => StatusKind::Modified,
            _ => StatusKind::Modified,
        };

        files.push(FileStatus {
            path: PathBuf::from(path),
            status,
        });
    }

    files
}

/// Parse `git blame --porcelain` output into BlameLine entries.
fn parse_blame_porcelain(output: &str) -> Vec<BlameLine> {
    let mut lines = Vec::new();
    let mut current_commit = String::new();
    let mut current_author = String::new();
    let mut current_email = String::new();
    let mut current_timestamp: i64 = 0;
    let mut current_summary = String::new();
    let mut line_number: usize = 0;

    for line in output.lines() {
        // Commit header: starts with hex hash (40 chars) followed by line numbers
        let first_word = line.split_whitespace().next().unwrap_or("");
        if first_word.len() >= 40 && first_word.chars().all(|c| c.is_ascii_hexdigit()) {
            // Commit header line: <hash> <orig-line> <final-line> [<num-lines>]
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                current_commit = parts[0].to_string();
                line_number = parts[2].parse().unwrap_or(0);
            }
        } else if let Some(val) = line.strip_prefix("author ") {
            current_author = val.to_string();
        } else if let Some(val) = line.strip_prefix("author-mail ") {
            current_email = val.trim_matches(|c| c == '<' || c == '>').to_string();
        } else if let Some(val) = line.strip_prefix("author-time ") {
            current_timestamp = val.parse().unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("summary ") {
            current_summary = val.to_string();
        } else if line.starts_with('\t') {
            // Content line — marks end of this blame entry
            lines.push(BlameLine {
                line: line_number,
                author: current_author.clone(),
                email: current_email.clone(),
                commit_id: if current_commit.len() > 8 {
                    current_commit[..8].to_string()
                } else {
                    current_commit.clone()
                },
                timestamp: current_timestamp,
                summary: current_summary.clone(),
            });
        }
    }

    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_porcelain_status() {
        let output = " M src/main.rs\n?? new_file.txt\nA  staged.rs\n D deleted.rs\n";
        let files = parse_porcelain_status(output);

        assert_eq!(files.len(), 4);
        assert_eq!(files[0].status, StatusKind::Modified);
        assert_eq!(files[0].path, PathBuf::from("src/main.rs"));
        assert_eq!(files[1].status, StatusKind::Untracked);
        assert_eq!(files[2].status, StatusKind::Added);
        assert_eq!(files[3].status, StatusKind::Deleted);
    }

    #[test]
    fn test_parse_porcelain_empty() {
        let files = parse_porcelain_status("");
        assert!(files.is_empty());
    }

    #[test]
    fn test_discover_current_repo() {
        let result = GitCliOps::discover(Path::new("."));
        assert!(result.is_ok(), "should discover git repo in workspace");

        let ops = result.unwrap();
        let branch = ops.current_branch();
        assert!(branch.is_ok(), "should get current branch");
        assert!(!branch.unwrap().is_empty());
    }

    #[test]
    fn test_discover_nonexistent() {
        let result = GitCliOps::discover(Path::new("/tmp/definitely-not-a-git-repo-abc123"));
        assert!(result.is_err());
    }

    #[test]
    fn test_status_in_current_repo() {
        let ops = GitCliOps::discover(Path::new(".")).unwrap();
        let status = ops.status();
        assert!(status.is_ok());
        assert!(!status.unwrap().branch.is_empty());
    }

    #[test]
    fn test_branches_in_current_repo() {
        let ops = GitCliOps::discover(Path::new(".")).unwrap();
        let branches = ops.branches();
        assert!(branches.is_ok());
        let branches = branches.unwrap();
        assert!(!branches.is_empty());
        assert!(branches.iter().any(|b| b.is_current));
    }

    #[test]
    fn test_log_in_current_repo() {
        let ops = GitCliOps::discover(Path::new(".")).unwrap();
        let log = ops.log(5);
        assert!(log.is_ok());
        let commits = log.unwrap();
        assert!(!commits.is_empty());
        assert!(!commits[0].id.is_empty());
        assert!(!commits[0].subject.is_empty());
    }

    #[test]
    fn test_parse_blame_porcelain() {
        let output = "abc123def4567890123456789012345678901234 1 1 1\nauthor John Doe\nauthor-mail <john@example.com>\nauthor-time 1700000000\nsummary Initial commit\n\tline content here\n";
        let lines = parse_blame_porcelain(output);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].author, "John Doe");
        assert_eq!(lines[0].line, 1);
        assert_eq!(lines[0].commit_id, "abc123de");
    }
}
