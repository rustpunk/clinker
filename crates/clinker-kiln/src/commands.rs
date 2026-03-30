//! Command registry for the command palette.
//!
//! Every action available in Kiln is registered as a `Command` with an id,
//! label, description, optional keyboard shortcut, and group.
//! The command palette fuzzy-searches against label + description.
//! Spec: clinker-kiln-git-addendum.md §G4.4.

/// A registered command.
#[derive(Clone, Debug)]
pub struct Command {
    /// Unique identifier (e.g., "git.commit", "file.save").
    pub id: &'static str,
    /// Display label (e.g., "git: commit").
    pub label: &'static str,
    /// Short description.
    pub description: &'static str,
    /// Keyboard shortcut display string (e.g., "Ctrl+K").
    pub shortcut: Option<&'static str>,
    /// Group for categorization.
    pub group: CommandGroup,
    /// Whether this command requires a git repo.
    pub requires_git: bool,
}

/// Command groups for palette sections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandGroup {
    File,
    Layout,
    Search,
    Template,
    Git,
}

impl CommandGroup {
    pub fn label(self) -> &'static str {
        match self {
            Self::File => "File",
            Self::Layout => "Layout",
            Self::Search => "Search",
            Self::Template => "Template",
            Self::Git => "Git",
        }
    }
}

/// All registered commands.
pub fn all_commands() -> Vec<Command> {
    vec![
        // ── File ────────────────────────────────────────────────
        Command {
            id: "file.new",
            label: "File: New",
            description: "Create a new untitled pipeline",
            shortcut: Some("Ctrl+N"),
            group: CommandGroup::File,
            requires_git: false,
        },
        Command {
            id: "file.open",
            label: "File: Open",
            description: "Open a pipeline file",
            shortcut: Some("Ctrl+O"),
            group: CommandGroup::File,
            requires_git: false,
        },
        Command {
            id: "file.save",
            label: "File: Save",
            description: "Save the current pipeline",
            shortcut: Some("Ctrl+S"),
            group: CommandGroup::File,
            requires_git: false,
        },
        Command {
            id: "file.save_as",
            label: "File: Save As",
            description: "Save the current pipeline to a new file",
            shortcut: Some("Ctrl+Shift+S"),
            group: CommandGroup::File,
            requires_git: false,
        },
        Command {
            id: "file.close",
            label: "File: Close Tab",
            description: "Close the active tab",
            shortcut: Some("Ctrl+W"),
            group: CommandGroup::File,
            requires_git: false,
        },
        // ── Layout ──────────────────────────────────────────────
        Command {
            id: "layout.canvas",
            label: "Layout: Canvas Focus",
            description: "Switch to canvas-only layout",
            shortcut: None,
            group: CommandGroup::Layout,
            requires_git: false,
        },
        Command {
            id: "layout.hybrid",
            label: "Layout: Hybrid",
            description: "Switch to canvas + YAML sidebar layout",
            shortcut: None,
            group: CommandGroup::Layout,
            requires_git: false,
        },
        Command {
            id: "layout.editor",
            label: "Layout: Editor Focus",
            description: "Switch to YAML editor-only layout",
            shortcut: None,
            group: CommandGroup::Layout,
            requires_git: false,
        },
        Command {
            id: "layout.schematics",
            label: "Layout: Schematics",
            description: "Switch to documentation/schematics view",
            shortcut: None,
            group: CommandGroup::Layout,
            requires_git: false,
        },
        Command {
            id: "layout.version",
            label: "Layout: Version",
            description: "Switch to git version control mode",
            shortcut: Some("Ctrl+Shift+G"),
            group: CommandGroup::Layout,
            requires_git: true,
        },
        // ── Search ──────────────────────────────────────────────
        Command {
            id: "search.text",
            label: "Search: Text",
            description: "Search across pipeline files",
            shortcut: Some("Ctrl+Shift+F"),
            group: CommandGroup::Search,
            requires_git: false,
        },
        Command {
            id: "search.schemas",
            label: "Search: Browse Schemas",
            description: "Open the schema browser panel",
            shortcut: Some("Ctrl+Shift+E"),
            group: CommandGroup::Search,
            requires_git: false,
        },
        // ── Template ────────────────────────────────────────────
        Command {
            id: "template.new",
            label: "Template: New from Template",
            description: "Create a pipeline from a template",
            shortcut: Some("Ctrl+Shift+N"),
            group: CommandGroup::Template,
            requires_git: false,
        },
        // ── Git ─────────────────────────────────────────────────
        Command {
            id: "git.commit",
            label: "git: commit",
            description: "Commit staged changes",
            shortcut: Some("Ctrl+K"),
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.commit_all",
            label: "git: commit all",
            description: "Stage all changes and commit",
            shortcut: Some("Ctrl+Shift+K"),
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.stage_file",
            label: "git: stage file",
            description: "Stage the current file",
            shortcut: None,
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.push",
            label: "git: push",
            description: "Push commits to remote",
            shortcut: None,
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.pull",
            label: "git: pull",
            description: "Pull from remote",
            shortcut: None,
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.fetch",
            label: "git: fetch",
            description: "Fetch from remote (no merge)",
            shortcut: None,
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.switch_branch",
            label: "git: switch branch",
            description: "Switch to a different branch",
            shortcut: None,
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.create_branch",
            label: "git: create branch",
            description: "Create a new branch from HEAD",
            shortcut: None,
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.view_log",
            label: "git: view log",
            description: "Show commit history",
            shortcut: None,
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.view_diff",
            label: "git: view diff",
            description: "Show diff for current file",
            shortcut: Some("Ctrl+D"),
            group: CommandGroup::Git,
            requires_git: true,
        },
        Command {
            id: "git.version_mode",
            label: "git: open version mode",
            description: "Switch to Version layout",
            shortcut: Some("Ctrl+Shift+G"),
            group: CommandGroup::Git,
            requires_git: true,
        },
    ]
}
