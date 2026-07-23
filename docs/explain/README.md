# `clinker explain` pages

Each `E*.md` / `W*.md` file here is the long-form explanation for one diagnostic
code, surfaced by `clinker explain <code>` and compiled into the binary via
`include_str!`. Every page must carry the five required sections
(`## What it means`, `## Example`, `## How to fix`, `## Technical context`,
`## See also`); a test enforces their presence.

## Compile-checking example blocks

A page's `## Example` section shows YAML. Some blocks are complete node lists;
others are deliberate mid-indentation fragments that show only the lines that
change. To catch a page that prescribes a configuration which does not actually
parse, compile, or fire the diagnostic it documents, mark the **complete,
compilable** blocks so the test suite compiles them.

Put an HTML comment on the line directly above the fence. It renders invisibly
in any generated output:

    <!-- explain-test: expect-code=E349 -->
    ```yaml
    # Rejected: ...
    nodes:
      - type: source
        ...
    ```

Two directives are recognized:

- `<!-- explain-test: expect-code=EXXX -->` — the block must compile far enough
  to surface diagnostic `EXXX` (whether that arrives as a parse-time validation
  error or a compile-time diagnostic).
- `<!-- explain-test: expect-clean -->` — the block must parse, compile, and
  produce no diagnostic at all.

The harness lives in
`crates/clinker-plan/src/plan/tests/explain_examples.rs`. Notes for authors:

- Example blocks omit the top-level `pipeline:` metadata for brevity. A marked
  block that has no `pipeline:` key is compiled under a synthetic
  `pipeline: { name }` header; nothing else is injected, so the rest of the
  block must be a self-contained node list.
- Leave fragments unmarked — they are skipped. A page with no marked block at
  all is *reported* (run the harness with `--nocapture` to see the list), not
  failed, so coverage can grow page by page without a single large sweep.
