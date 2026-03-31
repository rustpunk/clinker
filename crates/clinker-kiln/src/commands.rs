//! Command registry for the command palette.
//!
//! Every action available in Kiln is registered as a `Command` with an id,
//! label, description, optional keyboard shortcut, and group.
//! The command palette fuzzy-searches against label + description.
//! Spec: clinker-kiln-git-addendum.md §G4.4, navigation addendum §N8.

/// A registered command.
#[derive(Clone, Debug)]
pub struct Command {
    /// Unique identifier (e.g., "git.commit", "nav.pipeline").
    pub id: &'static str,
    /// Display label (e.g., "nav: pipeline").
    pub label: &'static str,
    /// Short description.
    pub description: &'static str,
    /// Keyboard shortcut display string (e.g., "Ctrl+Shift+E").
    pub shortcut: Option<&'static str>,
    /// Group for categorization.
    pub group: CommandGroup,
    /// Whether this command requires a git repo.
    pub requires_git: bool,
}

/// Command groups for palette sections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandGroup {
    Navigation,
    File,
    Layout,
    Search,
    Composition,
    Template,
    Settings,
    Git,
}

impl CommandGroup {
    pub fn label(self) -> &'static str {
        match self {
            Self::Navigation => "Navigation",
            Self::File => "File",
            Self::Layout => "Layout",
            Self::Search => "Search",
            Self::Composition => "Composition",
            Self::Template => "Template",
            Self::Settings => "Settings",
            Self::Git => "Git",
        }
    }
}

/// All registered commands.
pub fn all_commands() -> Vec<Command> {
    vec![
        // ── Navigation ─────────────────────────────────────────
        Command {
            id: "nav.pipeline",
            label: "nav: Pipeline",
            description: "Switch to Pipeline context",
            shortcut: Some("Ctrl+Shift+E"),
            group: CommandGroup::Navigation,
            requires_git: false,
        },
        Command {
            id: "nav.channels",
            label: "nav: Channels",
            description: "Switch to Channels context",
            shortcut: Some("Ctrl+Shift+C"),
            group: CommandGroup::Navigation,
            requires_git: false,
        },
        Command {
            id: "nav.git",
            label: "nav: Git",
            description: "Switch to Version Control context",
            shortcut: Some("Ctrl+Shift+G"),
            group: CommandGroup::Navigation,
            requires_git: false,
        },
        Command {
            id: "nav.docs",
            label: "nav: Docs",
            description: "Switch to Documentation context",
            shortcut: Some("Ctrl+Shift+D"),
            group: CommandGroup::Navigation,
            requires_git: false,
        },
        Command {
            id: "nav.runs",
            label: "nav: Runs",
            description: "Switch to Run History context",
            shortcut: Some("Ctrl+Shift+R"),
            group: CommandGroup::Navigation,
            requires_git: false,
        },
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
        // ── Layout (Pipeline context) ──────────────────────────
        Command {
            id: "layout.canvas",
            label: "layout: Canvas",
            description: "Switch to canvas-only layout mode",
            shortcut: Some("Ctrl+Shift+1"),
            group: CommandGroup::Layout,
            requires_git: false,
        },
        Command {
            id: "layout.hybrid",
            label: "layout: Hybrid",
            description: "Switch to canvas + YAML sidebar layout mode",
            shortcut: Some("Ctrl+Shift+2"),
            group: CommandGroup::Layout,
            requires_git: false,
        },
        Command {
            id: "layout.editor",
            label: "layout: Editor",
            description: "Switch to YAML editor-only layout mode",
            shortcut: Some("Ctrl+Shift+3"),
            group: CommandGroup::Layout,
            requires_git: false,
        },
        // ── Search ──────────────────────────────────────────────
        Command {
            id: "search.text",
            label: "Search: Text",
            description: "Search across pipeline files",
            shortcut: Some("Alt+F"),
            group: CommandGroup::Search,
            requires_git: false,
        },
        Command {
            id: "search.schemas",
            label: "Search: Browse Schemas",
            description: "Open the schema browser panel",
            shortcut: Some("Alt+E"),
            group: CommandGroup::Search,
            requires_git: false,
        },
        // ── Composition ─────────────────────────────────────────
        Command {
            id: "composition.browse",
            label: "Composition: Browse",
            description: "Open the composition browser panel",
            shortcut: Some("Alt+C"),
            group: CommandGroup::Composition,
            requires_git: false,
        },
        Command {
            id: "composition.new",
            label: "Composition: New",
            description: "Create a new composition file",
            shortcut: None,
            group: CommandGroup::Composition,
            requires_git: false,
        },
        Command {
            id: "composition.extract",
            label: "Composition: Extract from Pipeline",
            description: "Extract selected transforms into a composition",
            shortcut: None,
            group: CommandGroup::Composition,
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
        // ── Settings ────────────────────────────────────────────
        Command {
            id: "settings.open",
            label: "settings: Open",
            description: "Open workspace settings",
            shortcut: Some("Ctrl+,"),
            group: CommandGroup::Settings,
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
    ]
}
