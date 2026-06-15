# CLAUDE.md

Claude Code uses this file as its repository entry point.

@AGENTS.md

## Claude Code Notes

- Treat `AGENTS.md` as the shared contract for autonomous issue work, architecture rules, commands, and definition of done.
- Use Claude Code plan mode for issues labeled `agent-plan-first`, `agent-size:L`, `agent-mode:decision`, or for any Issue Readiness Review.
- Do not implement an issue labeled `agent-size:XL`; split it into atomic linked issues or convert it into a milestone/epic tracker first.
- Follow the agent close protocol in `AGENTS.md`: leave a final issue comment with changed behavior, files/crates touched, validation, skipped checks, and follow-up issues before closing.
- Use GitHub's native sub-issues API for umbrella/sub-issue relationships instead of markdown task-list checkboxes in parent issue bodies.
- Claude-specific skills, commands, and policies under `.claude/` may provide additional workflow help. Read the relevant skill or policy before relying on it.
