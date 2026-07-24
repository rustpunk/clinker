# GitHub Workflow: Decisions

Verified against origin/main cf6609b9 (2026-07-24).

Purpose: Resolve bounded decisions before implementation changes make those
decisions implicitly.

Cross-repo agent workflow and planning policy decisions belong in the
maintainer's central policy notes, outside individual project repositories,
unless the maintainer explicitly asks for repo-local guidance.

Use the Agent Decision Gate template only when implementation depends on an
unresolved choice that needs issue-level discussion or maintainer review before
work can proceed.

## When To Create A Decision Gate

Create or link a Decision Gate for:

- new dependency or cargo-deny exception
- public API behavior
- data model, schema, storage, or migration behavior
- auth, security, privacy, or credentials
- performance or bounded-memory tradeoff with unclear priority
- backward compatibility or breaking changes
- user-visible behavior with unclear product intent
- cross-crate or cross-service architecture boundaries
- any behavior the agent cannot validate from existing docs, tests, or source

## Decision Authority

Agent may decide:

- local implementation approach
- naming consistent with local patterns
- test structure consistent with existing tests
- minor internal refactors required by the issue
- documentation wording that reflects implemented behavior

Agent must create or link a Decision Gate for:

- new dependencies
- new public APIs
- schema or storage changes
- security-sensitive behavior
- cross-crate architecture changes
- incompatible behavior
- memory or performance tradeoffs with unclear priority

Maintainer review is required for decisions that affect public contribution
expectations or repository policy. Use `needs-maintainer-review` for:

- product behavior
- breaking changes
- security posture
- release scope tradeoffs
- legal/compliance-sensitive behavior
- anything irreversible or expensive to revert

If those choices are not flagged for maintainer review and the work is
agent-authorized, an agent may resolve the Decision Gate within the repository
architecture, dependency, safety, and documentation rules. Decision resolution
does not authorize merging the implementation PR.

## Closure

Close a Decision Gate with a final comment containing:

- chosen option
- rejected options
- source evidence
- consequences
- docs updated, if any
- follow-up issue links, if implementation remains

If the decision is durable project architecture or behavior, record it in the
repo's architecture docs, ADR location, or equivalent project-local decision
surface. Do not record agent workflow policy in project `AI_CHANGELOG.md` files.
If the question remains unresolved, capture it in the repo's open-question
surface when one exists.
