//! Core types for git operations.
//!
//! These types are the shared interface between the gix backend,
//! CLI fallback, and Kiln UI.

use std::path::PathBuf;

/// Git file status relative to HEAD.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStatus {
    /// Path relative to repo root.
    pub path: PathBuf,
    /// Status kind.
    pub status: StatusKind,
}

/// File status categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StatusKind {
    Added,
    Modified,
    Deleted,
    Renamed,
    Untracked,
}

impl StatusKind {
    /// Single-letter indicator for tab badges.
    pub fn letter(self) -> &'static str {
        match self {
            Self::Added => "A",
            Self::Modified => "M",
            Self::Deleted => "D",
            Self::Renamed => "R",
            Self::Untracked => "U",
        }
    }
}

/// Branch information.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchInfo {
    /// Branch name (e.g., "main", "feat/normalize").
    pub name: String,
    /// Whether this is the currently checked-out branch.
    pub is_current: bool,
    /// Commits ahead of upstream (0 if no upstream).
    pub ahead: usize,
    /// Commits behind upstream (0 if no upstream).
    pub behind: usize,
}

/// A single line of blame output.
#[derive(Clone, Debug)]
pub struct BlameLine {
    /// 1-based line number.
    pub line: usize,
    /// Author name.
    pub author: String,
    /// Author email.
    pub email: String,
    /// Commit hash (short).
    pub commit_id: String,
    /// Unix timestamp.
    pub timestamp: i64,
    /// Commit subject line.
    pub summary: String,
}

/// Commit metadata for log display.
#[derive(Clone, Debug)]
pub struct CommitInfo {
    /// Full commit hash.
    pub id: String,
    /// Author name.
    pub author: String,
    /// Author email.
    pub email: String,
    /// Unix timestamp.
    pub timestamp: i64,
    /// First line of commit message.
    pub subject: String,
    /// Remaining commit message body.
    pub body: Option<String>,
}

/// Summary of repository status for UI display.
#[derive(Clone, Debug, Default)]
pub struct RepoStatus {
    /// Current branch name.
    pub branch: String,
    /// Commits ahead of upstream.
    pub ahead: usize,
    /// Commits behind upstream.
    pub behind: usize,
    /// All file statuses.
    pub files: Vec<FileStatus>,
    /// Count of added files.
    pub added: usize,
    /// Count of modified files.
    pub modified: usize,
    /// Count of deleted files.
    pub deleted: usize,
    /// Count of untracked files.
    pub untracked: usize,
}

impl RepoStatus {
    /// Build summary counts from file list.
    pub fn from_files(branch: String, ahead: usize, behind: usize, files: Vec<FileStatus>) -> Self {
        let added = files.iter().filter(|f| f.status == StatusKind::Added).count();
        let modified = files.iter().filter(|f| f.status == StatusKind::Modified).count();
        let deleted = files.iter().filter(|f| f.status == StatusKind::Deleted).count();
        let untracked = files.iter().filter(|f| f.status == StatusKind::Untracked).count();

        Self {
            branch,
            ahead,
            behind,
            files,
            added,
            modified,
            deleted,
            untracked,
        }
    }

    /// Whether there are any changes (staged or unstaged).
    pub fn has_changes(&self) -> bool {
        !self.files.is_empty()
    }
}
