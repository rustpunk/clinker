# CLINKER KILN — Inspector & Autodoc Addendum
// four-concern inspector, node run log, auto-documentation, notes, and schematics view

**Version**: 0.2.0-addendum · DRAFT
**Parent Spec**: clinker-kiln-spec.md v0.1.0
**Supersedes**: Parent spec §5 (Node Inspector) — this addendum replaces the original inspector panel design in its entirety.
**Aesthetic**: Rustpunk (Oxide default, Phosphor for run, Blueprint for docs)
**Date**: March 2026

---

## A1. OVERVIEW

This addendum restructures the node inspector from a single-pane config editor into a four-concern panel that separates **editing** (Config), **observing** (Run), **understanding** (Docs), and **annotating** (Notes). It also introduces the **Schematics** layout mode for full-pipeline documentation, a **markdown export** system, and a **PDF export** with Blueprint styling.

### A1.1 Design Principles

— **Four concerns, one panel**: The inspector serves four distinct purposes. Config is for editing pipeline structure. Run is for observing execution results. Docs is for understanding what a stage does. Notes is for authoring human context that can't be derived from configuration. Each concern has its own sub-aesthetic, its own data sources, and its own interaction model. They never mix.

— **Config is primary**: Config is always visible. Run, Docs, and Notes are secondary views accessed through a drawer that expands upward from the bottom of the inspector. This reflects the authoring workflow — you spend most of your time editing, and glance at run results, documentation, or annotations occasionally.

— **Structure, not execution**: The Docs drawer and Schematics layout document what a pipeline *is* — topology, configuration, column transformations, test fixtures. Row counts, deltas, timing, and pass statistics are runtime concerns that belong exclusively in the Run drawer and the main Run Log. Documentation surfaces never display or reference execution results.

— **Preview scales up**: The inspector's narrow width is insufficient for data preview tables. Rather than cramming wide tables into 340px, the inspector shows a compact preview and offers an expand action that slides a full-width preview panel over the canvas. The inspector remains visible alongside the expanded preview.

— **Sub-aesthetic separation**: Each concern maps to a rustpunk sub-aesthetic. Config uses Oxide (the default). Run uses Phosphor (terminal/CRT). Docs uses Blueprint (technical schematics). Notes uses Oxide with a distinct `iron` accent to signal user-authored content without introducing a fourth sub-aesthetic. The active drawer's styling colors only the drawer region — the Config section above always remains Oxide.

— **Derived, then annotated**: Auto-generated documentation is computed from the `Pipeline` model, `StageConfig` entries, and schema definitions. User-authored notes — per-node and per-field — are the sole manually-written content. The autodoc compiler weaves both layers together: auto-generated descriptions establish the structural baseline, and user notes add the business context, intent, and caveats that configuration alone cannot express. Notes are stored in the pipeline YAML as metadata, so they travel with the file.

— **Export as artifact**: Markdown export produces a standalone document suitable for README files, runbooks, or ops handoff — not a raw dump of internal state. The export is structured for human readers who may never open the IDE.

### A1.2 Concern Boundaries

| Concern | Sub-Aesthetic | Data Source | Mutates Model | Content |
|---------|--------------|-------------|---------------|---------|
| **Config** | Oxide | `Pipeline` model + JSON Schema | Yes | Form fields, node YAML, add/remove properties |
| **Run** | Phosphor | Run statistics, child process output | No (read-only) | Pass stats, data preview, node log |
| **Docs** | Blueprint | `Pipeline` model (derived) + notes (compiled) | No (read-only) | Auto-description, structural metadata, column impact, compiled notes |
| **Notes** | Oxide (iron accent) | `PipelineNotes` metadata in YAML | Yes | Stage-level markdown note, per-field annotations |

---

## A2. INSPECTOR PANEL REDESIGN

This section supersedes parent spec §5 (Node Inspector) in its entirety. The inspector header, resize behavior, push behavior, close behavior, and crossfade timing from §3.2 and §5.1 remain unchanged. What changes is everything below the header.

### A2.1 Panel Structure

```
┌─ border-top (3px, stage accent — always Oxide)
│
│  ◆ STAGE_LABEL   [type badge]                    [×]
│
├─ CONFIG (always visible, scrollable) ────────────────
│
│  CONFIGURATION
│  FIELD_KEY     ✎    [input value]
│  FIELD_KEY          [select ▾]
│  FIELD_KEY     ✎    [toggle ○──]  value
│  [+ Add Property]
│
│  NODE YAML
│  ▊ - step: filter
│  ▊   expr: 'status == "active"'
│
├─ DRAWER TOGGLE BAR ─────────────────────────────────
│  [▸ Run  9,231]   |   [◇ Docs  16col]   |   [✎ Notes  2]
│
├─ DRAWER (expandable, swappable) ─────────────────────
│  (Run, Docs, or Notes content depending on active toggle)
│
└──────────────────────────────────────────────────────
```

### A2.2 Config Section (Always Visible)

The Config section occupies the upper portion of the inspector. When the drawer is closed, it fills the full panel height. When the drawer is open, it compresses to approximately 42% of the panel height with vertical scroll.

Content is identical to the original inspector spec (parent §5.2–5.3): form fields generated from the JSON Schema, node YAML in a phosphor-highlighted code block, and add/remove property controls. The pass statistics row from the original spec is **removed** from Config — it moves to the Run drawer.

**Annotation indicators**: Config fields that have a corresponding field annotation (see A5A) display a small ✎ icon (8px, `iron`, 60% opacity) to the right of the field label. The icon is non-interactive in the Config section — clicking it opens the Notes drawer and scrolls to the relevant annotation. Fields without annotations show no icon. This provides at-a-glance visibility into which fields have human-authored context without cluttering the editing surface.

The Config section's max-height transitions over 300ms with easing when the drawer opens or closes.

### A2.3 Drawer Toggle Bar

A horizontal bar between the Config section and the drawer content. Three toggle buttons, visually separated by 1px vertical dividers (`border-medium`). Each button toggles its respective drawer open (expanding the drawer region) or closed (collapsing it).

```
┌──────────────────┬──────────────────┬──────────────────┐
│  ▸ Run   [9,231] │  ◇ Docs  [16col] │  ✎ Notes    [2] │
└──────────────────┴──────────────────┴──────────────────┘
```

**Run button**:
— Icon: ▸ (play triangle), 9px.
— Label: "Run" in Chakra Petch 9px, 500 weight, 0.1em tracking, uppercase.
— Badge: Row count from last run (JetBrains Mono 8px, `text-floor`, `char` background, `border-subtle` border, 2px border-radius). Shows "—" if never run.
— Active state: `phosphor` at 10% background, 2px `phosphor` bottom border, `phosphor` text.
— Inactive state: transparent background, transparent bottom border, `text-floor` text.

**Docs button**:
— Icon: ◇ (hollow diamond), 9px.
— Label: "Docs" in Chakra Petch 9px, 500 weight, 0.1em tracking, uppercase.
— Badge: Column count at output (JetBrains Mono 8px, `text-floor`, `char` background, `border-subtle` border, 2px border-radius). Format: `{n}col`.
— Active state: `bpAccent` at 10% background, 2px `bpAccent` bottom border, `bpAccent` text.
— Inactive state: transparent background, transparent bottom border, `text-floor` text.

**Notes button**:
— Icon: ✎ (pencil), 9px.
— Label: "Notes" in Chakra Petch 9px, 500 weight, 0.1em tracking, uppercase.
— Badge: Total annotation count — stage note (counts as 1 if non-empty) + number of field annotations (JetBrains Mono 8px, `text-floor`, `char` background, `border-subtle` border, 2px border-radius). Shows "0" when empty. Hidden entirely when 0 to reduce visual noise — badge only appears when ≥1 note exists.
— Active state: `iron` text, `iron` at 10% background, 2px `iron` bottom border.
— Inactive state: transparent background, transparent bottom border, `text-floor` text.

**Toggle behavior**: Clicking an inactive button opens the drawer with that content. Clicking the active button closes the drawer entirely. Only one drawer can be active at a time. The toggle bar border-top transitions color to match the active drawer's accent (phosphor, verdigris, or iron). Transition: 200ms ease on all properties.

Background: `char-raised`. Height: 32px. The toggle bar is always visible regardless of drawer state.

### A2.4 Drawer Region

The drawer expands upward from the toggle bar, compressing the Config section above. When open, the drawer occupies approximately 58% of the inspector height. When closed, the drawer height is 0. Height transition: 300ms ease.

The drawer content is scrollable independently of the Config section. The active drawer's sub-aesthetic applies only to the drawer region — the Config section above always remains Oxide.

### A2.5 Header Behavior

The inspector header (3px accent top border, diamond icon, stage label, type badge, close button) remains Oxide at all times. The top border always uses the stage accent color, regardless of which drawer is active. This anchors the inspector to the selected node's identity even when viewing Run or Docs content.

---

## A3. RUN DRAWER

The Run drawer displays execution results for the selected node. All content is derived from the most recent pipeline run. If the pipeline has never been run, the drawer shows a "no run data" state with a prompt to execute.

### A3.1 Run Drawer Content

```
├─ RUN DRAWER ─────────────────────────────────────────
│
│  PASS STATISTICS
│  IN 9,231   OUT 9,231   PASS 1
│  TIME 0.002s   MEM 1.2 MB
│
│  DATA PREVIEW                         [← Expand]
│  ┌──────────────────────────────────────────────┐
│  │  ID     FULL_NAME      EMAIL_LOWER    +13col │
│  │  1001   Jane Chen      jane.chen@...         │
│  │  1002   Kwame Asante   k.asante@...          │
│  │  1003   Mika Torvalds  mika.t@...            │
│  └──────────────────────────────────────────────┘
│  3 of 50 rows · expand for full table
│
│  NODE LOG
│  ┌──────────────────────────────────────────────┐
│  │  00:00.038  INF  [map] applying 2 fields...  │
│  │  00:00.039  DAT  [map] → 9,231 rows          │
│  │  00:00.039  INF  [map] pass 1 complete        │
│  └──────────────────────────────────────────────┘
│
└──────────────────────────────────────────────────────
```

### A3.2 Pass Statistics

A compact stats section showing runtime metrics for the selected stage:

— **Row metrics**: IN, OUT, PASS, and Δ (delta, only shown when rows are lost). JetBrains Mono 10px. Labels in `text-floor`. Values in `bone` (or `hazard` for OUT when delta is non-zero, `oxide-red` for delta).
— **Performance metrics**: TIME (duration) and MEM (peak memory). JetBrains Mono 10px. Labels in `text-floor`. Values in `phosphor`.
— Layout: Two rows of inline metrics. Border-bottom: `border-subtle`.
— Populated from the most recent run's `StageStats`. Shows "—" for all values if never run.

### A3.3 Compact Data Preview

A narrow preview table showing a subset of columns and rows, designed to fit within the inspector's 260–520px width. This is a sanity-check view, not a full data explorer.

— **Column selection**: Shows up to 3 key columns (prioritizing the stage's `columns_added` and the primary key column) plus a trailing "+{n} cols" indicator showing how many columns are hidden.
— **Row count**: First 3 rows of the stage's output sample. Footer text: "{n} of {total} rows · expand for full table".
— **Table styling**: `border-collapse: collapse`. Header: Chakra Petch 8px uppercase, `bone` text, oxide-red at 15% background. Body: JetBrains Mono 9px. Added columns highlighted in `verdigris`. Alternating row backgrounds: transparent / `char`.
— **Expand button**: Positioned in the section header, right-aligned. Chakra Petch 8px uppercase, stage accent text, accent at 10% background, accent at 30% border. Label: "← Expand". Clicking opens the Expanded Data Preview (see A4).

### A3.4 Node Log

A filtered view of the run log showing only log lines relevant to the selected stage. This is a subset of the main Run Log drawer (parent spec §7), scoped to the node.

— **Container**: `char` background, `border-medium` border, 2px border-radius.
— **Line format**: Identical to the main Run Log (parent spec §7.2): timestamp (60px, `text-floor`), level tag (24px, level-colored), message (flex, level-colored). JetBrains Mono 10px, 16px line-height.
— **Level colors**: Same as parent spec §7.3.
— **Filtering**: Only lines containing the stage's `[label]` tag (e.g., `[map]`) are shown. If no matching lines exist, shows "no log output for this stage" in `text-floor`.

### A3.5 No-Run State

When the pipeline has never been run, the entire Run drawer shows a centered placeholder:

— Text: "No run data — execute the pipeline to see results" in JetBrains Mono 10px, `text-floor`.
— The pass statistics, preview, and node log sections are hidden.
— The Run toggle badge shows "—" instead of a row count.

---

## A4. EXPANDED DATA PREVIEW

The expanded preview is a full-width panel that slides over the canvas, providing a complete data table for the selected stage. The inspector panel remains visible alongside it. This is a runtime concern — it is part of the Run drawer's feature set, not documentation.

### A4.1 Panel Behavior

— **Trigger**: Click the "← Expand" button in the Run drawer's compact data preview.
— **Position**: Absolutely positioned over the canvas area, anchored to the right edge (adjacent to the inspector). Width: `calc(100% - 60px)` of the canvas container, leaving a 60px sliver of the canvas visible on the left as a spatial anchor.
— **Animation**: Slides in from the right over 250ms with easing.
— **Z-index**: Above the canvas (z-index 30) but below the title bar (z-index 40).
— **Shadow**: Left-facing box-shadow (`-4px 0 20px rgba(0,0,0,0.5)`) to separate from the canvas.
— **Close**: Click the "→ Collapse" button in the panel header, or click a different node on the visible canvas sliver.
— **Canvas interaction**: The canvas behind the panel is dimmed (35% opacity) and pointer-events are disabled. Nodes on the visible 60px sliver remain clickable.

### A4.2 Panel Structure

```
┌─ border-top (3px, stage accent)
│
│  ◆ DATA PREVIEW | STAGE_LABEL [type badge]   rows×cols   FILTER [____]   [→ Collapse]
│
├─ PINNED  [id] [first] [last] [email] [full_name] [email_lower] ...  (clickable tags)
│
├─ TABLE ──────────────────────────────────────────────────────────────────
│                                                                         │
│  ◆ ID    ◆ FULL_NAME     ◆ EMAIL_LOWER    ║  FIRST   LAST    EMAIL     │
│  1001    Jane Chen       jane.chen@...    ║  Jane    Chen    jane...    │
│  1002    Kwame Asante    k.asante@...     ║  Kwame   Asante  K.As...   │
│  ...                                      ║                            │
│                                                                         │
│  (pinned columns stick left, unpinned scroll freely, separator between) │
│                                                                         │
├─ IN 9,231   OUT 9,231   PASS 1   |   TIME 0.002s   MEM 1.2 MB   |  n of m rows
└──────────────────────────────────────────────────────────────────────────
```

### A4.3 Header Bar

— **Stage identity**: Diamond icon (stage accent), "Data Preview" label, vertical divider, stage label + type badge. Chakra Petch 10px, 500 weight, 0.2em tracking.
— **Dimensions**: Row count × column count in JetBrains Mono 10px, `iron`.
— **Filter**: Text input for row filtering. Label: "FILTER" in Chakra Petch 8px, `text-floor`. Input: JetBrains Mono 10px, `bone`, `char` background, `border-medium` border. Width: 140px. Filters rows where any cell value contains the search string (case-insensitive).
— **Collapse button**: Chakra Petch 9px, "→ Collapse", `bone` text, `border-medium` border.

Background: `char-raised`. Border-bottom: `border-medium`. Border-top: 3px stage accent.

### A4.4 Column Pin Bar

A horizontal bar of clickable column tags showing all columns in the preview. Each tag represents one column and can be toggled between pinned and unpinned.

— **Layout**: Flex-wrap row with 4px gap. Label "PINNED" in Chakra Petch 8px, `text-floor`, 0.12em tracking, left of the tags.
— **Pinned tag**: Stage accent text, accent at 20% background, accent at 40% outline. JetBrains Mono 8px.
— **Unpinned tag**: `text-floor` text (or `verdigris` at 80% for added columns), `border-subtle` outline, transparent background.
— **Click**: Toggles pin state. Transition: 150ms ease.
— **Default pins**: The primary key column (e.g., `id`) and all `columns_added` for the selected stage.

Border-bottom: `border-subtle`. Padding: 6px 16px.

### A4.5 Table Rendering

The table uses sticky positioning to keep pinned columns visible during horizontal scroll.

**Pinned columns**:
— Fixed width: 140px per column (`width`, `min-width`, `max-width` all set, `box-sizing: border-box`).
— `position: sticky` with `left: index * 140px` where `index` is the column's position within the pinned set (0-based).
— Header cells: Opaque background (`#1C110C` — pre-composited oxide-red at 9% over `char-raised`). Stage accent text, Chakra Petch 9px uppercase. Diamond marker (◆, 7px, 60% opacity) before the column name.
— Body cells: Opaque background alternating between `char-surface` and `char-raised` (matching the row's parity). `bone` text for regular columns, `verdigris` for added columns. JetBrains Mono 10px, 500 weight.
— Right border: 1px `border-medium` on each pinned cell.
— Z-index: 4 for header cells (above sticky header row), 2 for body cells.

**Separator strip**:
— A 3px-wide sticky column between pinned and unpinned regions.
— `position: sticky` with `left: pinnedCount * 140px`.
— Opaque background: `#15100D`.
— Box-shadow: `-3px 0 0 0 #15100D` to seal sub-pixel gaps between the last pinned cell's border-right and the separator's left edge.
— Z-index: matches the row (4 in header, 2 in body).
— Both thead and tbody separator cells must include this box-shadow.

**Unpinned columns**:
— No sticky positioning. Natural horizontal scroll.
— Header: `char-raised` background, `bone` text (or `verdigris` with `+` prefix for added columns).
— Body: No explicit background (inherits from row). `iron` text (or `verdigris` for added columns).

**Row styling**: Alternating backgrounds via the `<tr>` element: transparent for even rows, `char-raised` at 80% for odd rows.

**Sticky header**: The `<thead>` has `position: sticky; top: 0; z-index: 3`. All pinned header cells use z-index 4 to layer above both the sticky header and the horizontal scroll.

**Scroll container**: `overflow-x: auto; overflow-y: auto` on the parent div. The `<table>` has `min-width: 100%` to ensure it fills the container width even when columns are narrow.

### A4.6 Footer Stats Bar

A compact stats bar at the bottom of the expanded preview:

— **Left group**: IN, OUT, PASS metrics (same as Run drawer §A3.2).
— **Right group**: TIME, MEM metrics in `phosphor`.
— **Far right**: "showing {filtered} of {total} sample rows" in Chakra Petch 8px, `text-floor`.
— Groups separated by 1px × 12px vertical dividers in `border-medium`.

Background: `char-raised`. Border-top: `border-medium`. Padding: 6px 16px. JetBrains Mono 10px.

### A4.7 Title Bar Integration

When the expanded preview is open, the title bar displays a stage indicator:

— A 4px accent-colored dot with box-shadow glow, followed by "PREVIEW: {stage_label}" in Chakra Petch 8px, stage accent color, 0.1em tracking.
— Positioned between the filename and the layout switcher.
— Disappears when the preview is collapsed.

---

## A5. DOCS DRAWER

The Docs drawer displays the compiled documentation for the selected node — auto-generated structural descriptions woven together with user-authored notes from the Notes drawer (A5A). It uses the Blueprint sub-aesthetic exclusively. All structural content is derived from the pipeline model — never from run results. User notes are rendered alongside auto-generated content with distinct visual treatment.

### A5.1 Sub-Aesthetic Application

When the Docs drawer is active:

— The drawer region's background gains a Blueprint gridline overlay (minor grid: `bpAccent` at 5%, 16px spacing).
— Body text in the drawer uses Share Tech Mono instead of JetBrains Mono.
— Label text remains Chakra Petch (shared across sub-aesthetics).
— The Config section above the toggle bar is unaffected — it remains Oxide.

### A5.2 Docs Drawer Content

```
├─ DOCS DRAWER (Blueprint sub-aesthetic) ──────────────
│
│  DESCRIPTION (auto-generated)
│  ┌─ border-left (2px, bpAccent at 30%)
│  │  Computes 2 derived fields: full_name from
│  │  first + last, email_lower from lower(email).
│  │  Row count is preserved.
│  └──────────────────────────────
│
│  NOTE (user-authored, when present)
│  ┌─ border-left (2px, iron)
│  │  full_name is used downstream by the email
│  │  personalization service. Format must be
│  │  "First Last" — no middle names.
│  └──────────────────────────────
│
│  STAGE METADATA
│  TYPE           map
│  PASS           1 (Scan)
│  EXPRESSIONS    2 field derivations
│    — field annotation inline               (when annotated)
│  PRESERVES ROWS yes
│
│  COLUMN IMPACT
│  ┌──────────────────────────────
│  │  ADDED
│  │  [+full_name] [+email_lower]
│  │
│  │  16 columns at output
│  └──────────────────────────────
│
│  OUTPUT COLUMNS
│  [id] [first] [last] ... [+full_name] [+email_lower]
│
│  ──── AUTODOC ────
│
└──────────────────────────────────────────────────────
```

### A5.3 Description Block

Auto-generated text (see A8.2) in a bordered block:

— `char` background, `bpAccent` at 15% border, 2px `bpAccent` left border at 30% opacity. 2px border-radius.
— Share Tech Mono 10px, `bpText`, 1.6 line-height.
— Content describes the stage's structural purpose and column behavior — never references row counts, deltas, or run timing.

**Compiled stage note** (when present): Rendered directly below the auto-description, visually distinct. "NOTE" label in Chakra Petch 8px, `iron`. Content in a bordered block: `char-raised` background, 2px `iron` left border, JetBrains Mono 10px, `bone` text, 1.5 line-height. Basic markdown rendering (bold, italic, inline code, links). When no stage note exists, this block is omitted — no empty placeholder.

### A5.4 Stage Metadata

A key-value grid of structural properties derived from the stage's step type and configuration:

— Keys: Chakra Petch 8px uppercase, `bpSecondary`, 80px width.
— Values: Share Tech Mono 10px, `bpText`.
— Row separators: `border-subtle`.

Contents vary by step type but always include Type and Pass. Additional entries are drawn from config:

| Step Type | Metadata Entries |
|----------|-----------------|
| source | Type, Pass, Format, Path, Has Header |
| filter | Type, Pass, Expression |
| map | Type, Pass, Expressions (count), Preserves Rows (yes) |
| lookup_join | Type, Pass, Lookup File, Join Key, Joined Fields (count) |
| distinct | Type, Pass, Dedup Keys |
| sort | Type, Pass, Sort Keys, Direction |
| sink | Type, Pass, Format, Path, Overwrite |

No runtime metrics (IN/OUT/Δ/TIME/MEM) appear here — those live exclusively in the Run drawer.

**Compiled field annotations**: Metadata entries whose field key has a corresponding annotation (from A5A.4) display the annotation text below the value. The annotation is prefixed with "—" in Share Tech Mono 9px, `iron`, indented below the value line. Entries without annotations show only the key and value. This provides inline context without disrupting the grid layout.

### A5.5 Column Impact

A bordered card (`char` background, `bpAccent` at 12% border) showing column changes at this stage:

— **When columns are added**: "ADDED" label (Chakra Petch 8px, `bpAccent`) followed by column tags (Share Tech Mono 9px, verdigris text, tinted background at 12%, accent border at 25%, `+` prefix).
— **When columns are removed**: "REMOVED" label (Chakra Petch 8px, `oxide-red`) followed by column tags (oxide-red text, tinted background, oxide-red border).
— **When no changes**: "No column changes at this stage" in Share Tech Mono 9px, `bpSecondary`.
— **Column count**: Always shown. Share Tech Mono 9px, `bpSecondary`. "{n} columns at output".

### A5.6 Output Columns

A wrapping tag list showing all columns present at this stage's output:

— Columns introduced at this stage: `bpAccent` text, tinted background at 12%, accent border at 25%.
— Pass-through columns: `bpSecondary` text, `border-subtle` border, transparent background.
— Font: Share Tech Mono 9px. Padding: 1px 5px. Border-radius: 2px. Gap: 2px.

### A5.7 Footer

Centered "AUTODOC" text flanked by verdigris gradient lines (same gradient as the Schematics mode indicator but at reduced width). Share Tech Mono 8px, `bpSecondary`.

---

## A5A. NOTES DRAWER

The Notes drawer provides two levels of user-authored annotation: a **stage-level note** (freeform markdown) and **field-level annotations** (short contextual notes attached to specific config fields, expressions, or rules). Notes are stored in the pipeline YAML as metadata and compiled into the Docs drawer and Schematics view alongside auto-generated content.

### A5A.1 Design Intent

Auto-generated descriptions explain *what* a stage does structurally. Notes explain *why* — the business context, design intent, edge case warnings, and domain knowledge that configuration alone cannot express. The two layers are complementary:

— Auto-generated: "Filters rows where `status == "active"` evaluates to true."
— Stage note: "Active-only filtering was added in Q3 2025 to exclude churned accounts from the marketing dataset. The sales team still needs churned records — see customer_etl_full.yaml for the unfiltered variant."
— Field annotation on `expr`: "The status field uses lowercase string values. Upstream systems occasionally send 'Active' with a capital A — a pre-filter normalization step may be needed if data quality degrades."

### A5A.2 Notes Drawer Content

```
├─ NOTES DRAWER ───────────────────────────────────────
│
│  STAGE NOTE                                    [✎ edit]
│  ┌──────────────────────────────────────────────┐
│  │  Active-only filtering was added in Q3 2025  │
│  │  to exclude churned accounts from the        │
│  │  marketing dataset. The sales team still     │
│  │  needs churned records — see                 │
│  │  customer_etl_full.yaml for the unfiltered   │
│  │  variant.                                    │
│  └──────────────────────────────────────────────┘
│
│  FIELD ANNOTATIONS ──────────────────── 2 annotations
│
│  ✎ expr                                       [×]
│  ┌──────────────────────────────────────────────┐
│  │  The status field uses lowercase string      │
│  │  values. Upstream systems occasionally       │
│  │  send 'Active' with a capital A.             │
│  └──────────────────────────────────────────────┘
│
│  ✎ fields[0].name                              [×]
│  ┌──────────────────────────────────────────────┐
│  │  full_name is used downstream by the email   │
│  │  personalization service. Format must be     │
│  │  "First Last" — no middle names.             │
│  └──────────────────────────────────────────────┘
│
│  [+ Add Field Annotation]
│
└──────────────────────────────────────────────────────
```

### A5A.3 Stage-Level Note

A single freeform markdown text area for the selected node:

— **Display mode**: Rendered markdown in JetBrains Mono 10px, `bone` text, 1.5 line-height. Contained in a bordered block: `char` background, `border-medium` border, 2px border-radius. Supports basic markdown rendering (bold, italic, inline code, links, line breaks). No heading rendering — headings within notes would conflict with the document hierarchy.
— **Edit mode**: Toggle via the `✎ edit` button in the section header. Replaces the rendered view with a `<textarea>` styled as a code editor: JetBrains Mono 11px, `bone` text, `char` background, `border-medium` border, full width. Auto-grows vertically with content (min 3 lines, max 50% of drawer height). An "✎ done" button replaces the edit button.
— **Empty state**: When no note exists, shows "No stage note — click ✎ to add context" in `text-floor`, italic. Clicking the text or the edit button enters edit mode with an empty textarea.
— **Section label**: "STAGE NOTE" in Chakra Petch 9px, 500 weight, 0.15em tracking, `text-tertiary`.

### A5A.4 Field Annotations

A list of short notes attached to specific config field keys:

— **Annotation card**: Each annotation is a bordered card (`char` background, `border-subtle` border, 2px border-radius) containing:
  — Field key label: ✎ icon + field key path (e.g., `expr`, `fields[0].name`, `key`) in Chakra Petch 9px, `iron`, 0.1em tracking, uppercase. Right-aligned × remove button.
  — Note text: JetBrains Mono 10px, `bone`, 1.4 line-height. Inline editing (click to focus, blur to save). Supports plain text only — no markdown rendering in field annotations to keep them concise.
— **Add button**: "[+ Add Field Annotation]" at the bottom. Clicking opens a dropdown of config field keys that don't yet have annotations (schema-aware, matching the Config tab's field list). Selecting a key adds a new empty annotation card with the textarea auto-focused.
— **Remove**: The × button on each card removes the annotation (with undo support via the model's undo stack).
— **Section label**: "FIELD ANNOTATIONS" in Chakra Petch 9px, 500 weight, 0.15em tracking, `text-tertiary`. Right-aligned count: "{n} annotations" in JetBrains Mono 9px, `text-floor`.
— **Empty state**: When no field annotations exist, the section shows "No field annotations" in `text-floor`. The add button remains visible.

### A5A.5 YAML Storage

Notes are stored in the pipeline YAML as a `_notes` metadata block within each stage config. The underscore prefix signals non-functional metadata — Clinker's engine ignores any key prefixed with `_`.

```yaml
stages:
  - step: filter
    expr: 'status == "active"'
    _notes:
      stage: |
        Active-only filtering was added in Q3 2025
        to exclude churned accounts from the marketing dataset.
      fields:
        expr: "The status field uses lowercase string values."
```

— **`_notes.stage`**: Stage-level note as a YAML multiline string (`|` block scalar).
— **`_notes.fields`**: Map of field key → annotation string. Keys match the config field paths used in the inspector (e.g., `expr`, `fields[0].name`, `key`).
— **Serialization**: Notes are serialized alongside the stage config when the YAML is written. The sync engine preserves `_notes` blocks during round-trip parsing — they are never stripped or rewritten unless the user edits them.
— **Pipeline-level notes**: A top-level `_notes.pipeline` key stores the pipeline-level note (used in the Schematics summary and markdown export).

```yaml
_notes:
  pipeline: |
    Customer ETL pipeline for the marketing analytics team.
    Runs nightly via cron. Output feeds the Looker dashboard.

source:
  format: csv
  path: "./data/customers_*.csv"
```

### A5A.6 Autodoc Compilation

The Docs drawer and Schematics view compile user notes alongside auto-generated content. The compilation strategy:

**Per-stage in Docs drawer (A5) and Schematics stage cards (A7.8)**:

1. Auto-generated description (A8.2) renders first as the structural baseline.
2. If a stage note exists, it renders below the auto-description in a visually distinct block: `iron` left border (2px), `char-raised` background, JetBrains Mono 10px, `bone` text. Label: "NOTE" in Chakra Petch 8px, `iron`, above the block.
3. If field annotations exist, they render below the config entries. Each annotated field key is followed by its annotation inline: field key in Chakra Petch 9px `bpSecondary`, then the annotation in Share Tech Mono 9px `iron`, prefixed with "—". Non-annotated fields show only the key and value with no annotation line.

**In Schematics stage cards**:

```
│  DESCRIPTION (auto-generated)
│  Filters rows where status == "active" evaluates to true.
│
│  NOTE
│  ┌─ border-left (2px, iron)
│  │  Active-only filtering was added in Q3 2025...
│  └──────────────────────────────
│
│  CONFIG_KEY    config_value
│    — field annotation text            (only for annotated fields)
│  CONFIG_KEY    config_value
```

**In markdown export (A9)**:

— Stage notes render as a blockquote below the auto-generated description.
— Field annotations render as sub-bullets under their respective config entries.
— Pipeline-level notes render as a blockquote below the pipeline description in the README, and in the Quick Reference section of the Runbook.

### A5A.7 Config Field Annotation Indicators

Fields with annotations show a ✎ icon in the Config section (see A2.2). Clicking this icon:

1. Opens the Notes drawer (if not already open).
2. Scrolls the Notes drawer to the relevant field annotation card.
3. Focuses the annotation text for inline editing.

This creates a bidirectional link: Config → Notes (click ✎ to see/edit the annotation) and Notes → Config (the field key label in the annotation card identifies which config field it annotates).

---

## A6. PIPELINE DOCS (NO NODE SELECTED)

When no node is selected but the user activates documentation mode, the inspector renders a pipeline-level summary.

### A6.1 Docs Button in Title Bar

— Position: Between the test override badge and the layout switcher.
— Appearance: Chakra Petch 9px, 500 weight, 0.12em tracking, uppercase. Hollow diamond icon (5px, verdigris border) + "Docs" label.
— Active state: `bpAccent` at 15% background, `bpAccent` at 40% border, verdigris text.
— Inactive state: `char-raised` background, `border-subtle` border, `text-floor` text.
— Click behavior: Opens the inspector panel in pipeline docs mode (deselects any selected node). Clicking again closes the inspector.

### A6.2 Pipeline Docs Content

The inspector renders in full Blueprint sub-aesthetic (gridline background across the entire panel, no drawer split):

— **Header**: 3px `bpAccent` top border, hollow diamond icon, "Pipeline Docs" in Share Tech Mono 10px, 0.2em tracking. Export button `↓ .md` in the header.
— **Summary**: Pipeline description, compact metadata grid (Stages, Pass 1, Pass 2, Final Columns).
— **Data Flow**: Compact stage list — one row per stage showing index, accent dot, label, pass badge, and column addition count. No runtime metrics.
— **Test Profiles**: Summary cards for each profile with name, scope badge, description, and per-override source/path/row-limit entries.
— **Footer**: Verdigris gradient lines flanking "AUTODOC".

### A6.3 Interaction with Selection

— Selecting a node while pipeline docs is open switches the inspector to the per-node view with the Docs drawer pre-opened. The pipeline docs mode is deactivated.
— Deselecting all nodes (click empty canvas, press Escape) returns to pipeline docs if the Docs button is still active in the title bar.
— The Docs button state is independent of the layout preset — it works in Canvas Focus, Hybrid, and Editor Focus modes (but is redundant in Schematics mode, where the full-panel view already shows everything).

---

## A7. SCHEMATICS LAYOUT

### A7.1 Layout Preset Integration

The layout switcher expands from three to four presets:

| Preset | Canvas | Inspector | YAML Sidebar | Schematics | Use Case |
|--------|--------|-----------|--------------|------------|----------|
| Canvas Focus | 100% | Slides in | Hidden | Hidden | Visual pipeline design |
| Hybrid | ~62% | Between canvas and YAML | ~38% (360px) | Hidden | Primary authoring mode |
| Editor Focus | Hidden | Hidden | 100% | Hidden | YAML-first editing |
| **Schematics** | Hidden | Hidden | Hidden | **100%** | Pipeline documentation, review, export |

Switching to Schematics follows the same 300ms animated transition as other presets. The inspector closes if open. The run log drawer remains available.

The Schematics layout switcher button uses the Blueprint sub-aesthetic: `bpAccent` (`#43B3AE`) text when active, with a 2px verdigris bottom border instead of the standard `border-strong` background used by other presets.

### A7.2 Schematics Panel Structure

```
┌────────────────────────────────────────────────────────────────────┐
│  TITLE BAR  [clinker][kiln]  file.yaml  [Canvas][Hybrid][Editor][Schematics]  │
├─── 2px verdigris mode indicator ───────────────────────────────────┤
│                                                                    │
│  FLOW BAR — compact horizontal pipeline summary                   │
│  [source] → [filter] → [map] → [lookup] → [distinct] → [sink]    │
│  each node: label + type + pass                                    │
│                                                                    │
├────────┬───────────────────────────────────────────────────────────┤
│        │                                                           │
│  TOC   │  CONTENT                                                  │
│  180px │  (scrollable)                                             │
│        │                                                           │
│  ◇ Summary  │  ═══ PIPELINE SUMMARY ═══════════════════════       │
│  ▸ Stages   │  description, source/sink, stage count,              │
│  ≡ Lineage  │  pass breakdown, column count                        │
│  ⊞ Profiles │                                                      │
│        │  ═══ STAGE DETAILS ════════════════════════════            │
│  ──────│  stage cards with config, description, column changes     │
│        │  flow arrows between stages                               │
│  ↓ README  │                                                       │
│  ↓ Runbook │  ═══ COLUMN LINEAGE ══════════════════════════        │
│        │  full matrix: columns × stages                            │
│        │                                                           │
│        │  ═══ TEST PROFILES ═══════════════════════════            │
│        │  profile cards with scope, overrides, paths               │
│        │                                                           │
│        │  ──── CLINKER AUTODOC · BLUEPRINT · date ────             │
│        │                                                           │
└────────┴───────────────────────────────────────────────────────────┘
```

### A7.3 Mode Indicator

A 2px horizontal line spanning the full viewport width immediately below the title bar:

```
transparent → verdigris 40% → verdigris 60% → verdigris 40% → transparent
```

Visible only in Schematics mode.

### A7.4 Flow Bar

Compact horizontal pipeline representation between the mode indicator and content area:

— **Node pills**: Label + type + pass badge per stage. Stage accent tinted background (8%), accent border (20%), 2px accent top border (50%).
— **Connectors**: 24px SVG, dashed line + open chevron, verdigris at 40%.
— **Click**: Scrolls content to the stage's detail card with a verdigris border flash (600ms ease-out).
— **Overflow**: Horizontal scroll. No wrapping.
— **No runtime data**: Structural information only.

Height: 40px. Background: `char-surface`. Bottom border: `bpAccent` at 15%.

### A7.5 Table of Contents Sidebar

180px sidebar with section navigation and export actions.

**Navigation items**:

| Item | Icon | Section |
|------|------|---------|
| Pipeline Summary | ◇ | `#summary` |
| Stage Details | ▸ | `#stages` |
| Column Lineage | ≡ | `#lineage` |
| Test Profiles | ⊞ | `#profiles` |

Active: `bpAccent` at 10% background, 2px verdigris left border. Inactive: transparent, 2px transparent left border. Share Tech Mono 10px. Transition: 200ms ease.

**Export buttons** (below a verdigris divider):

— **↓ Export README.md**: verdigris text, `bpAccent` border at 30%. Generates documentation-oriented markdown.
— **↓ Export Runbook.md**: phosphor text, `phosphor` border at 30%. Generates operations-oriented markdown.
— **↓ Export PDF**: `oxide-red` text, `oxide-red` border at 30%. Generates a styled Blueprint PDF.

All three: Chakra Petch 8px, 500 weight, 0.1em tracking, uppercase. `char-raised` background. Full sidebar width minus padding. Stacked vertically with 4px gap.

### A7.6 Content Area

Scrollable column with four documentation sections. Background: Blueprint gridlines.

**Section headers**: Hollow diamond (7px, 1.5px verdigris border), Share Tech Mono 12px, 0.2em tracking, uppercase, verdigris text. Trailing rule at `bpAccent` 25%. Right-aligned annotation in Share Tech Mono 10px, `bpSecondary`.

**Content cards**: `char-surface` background, `bpAccent` at 15–20% border, 2px border-radius. Scanlines at reduced opacity.

### A7.7 Pipeline Summary Section

Single content card:

— **Description**: Auto-generated (A8.2). Share Tech Mono 11px, `bpSecondary`, 1.6 line-height.
— **Pipeline note** (when present): Rendered below the auto-description. "NOTE" label in Chakra Petch 8px, `iron`. Content in a bordered block: `char-raised` background, 2px `iron` left border, JetBrains Mono 10px, `bone` text. Supports basic markdown rendering.
— **Metadata grid**: Two-column layout — Source, Sink, Stages, Glob Matches, Pass 1, Pass 2, Final Columns.

### A7.8 Stage Detail Section

One card per stage with flow arrows between them:

```
┌─ border-left (3px, stage accent at 50%)
│
│  00  STAGE_LABEL   [type badge]  [PASS badge]
│
│  ── verdigris line (30%) ──
│
│  DESCRIPTION (auto-generated)
│
│  NOTE (if stage note exists)
│  ┌─ border-left (2px, iron)
│  │  User-authored stage note rendered as markdown.
│  └──────────────────────────────
│
│  CONFIG_KEY    config_value
│    — field annotation text          (only if annotated)
│  CONFIG_KEY    config_value
│
│  COLUMN CHANGES
│  [+full_name] [+email_lower]
│
│  18 columns at output
│
└──────────────────────────────────────────────────
```

— Index: Two-digit zero-padded, Share Tech Mono 10px, `bpSecondary`.
— Label: Chakra Petch 14px, 600 weight, `bone`.
— Type badge: Share Tech Mono 10px, stage accent, tinted background (12%), accent border (25%).
— Pass badge: Chakra Petch 8px, pass-colored border (P1=verdigris, P2=phosphor, I/O=oxide-red).
— Description: Auto-generated text (A8.2). Share Tech Mono 10px, `bpSecondary`, 1.5 line-height.
— Stage note (when present): Rendered below the description. "NOTE" label in Chakra Petch 8px, `iron`. Content in a bordered block: `char-raised` background, 2px `iron` left border, JetBrains Mono 10px, `bone` text. Basic markdown rendering (bold, italic, inline code, links).
— Config entries: Keys in Chakra Petch 9px uppercase `bpSecondary`. Values in Share Tech Mono 11px `bpText`. Annotated fields show their annotation below the value line: "—" prefix, Share Tech Mono 9px, `iron`. Non-annotated fields show only key and value.
— Column changes: Tags with `+` prefix. Added: verdigris. Removed: oxide-red. Only rendered if non-empty.
— Column count: Share Tech Mono 9px, `bpSecondary`.

**Flow arrows**: 20×24px SVG. Dashed vertical verdigris line (1.5px, 4-3 pattern, 50%) with downward chevron (70%).

### A7.9 Column Lineage Section

Full-width table in a content card.

— Header: Column name (left, sticky), then one column per stage (center, stage-accent colored). Chakra Petch 7–8px uppercase.
— Body: Presence markers per stage — `+` (introduced, verdigris bold), `·` (present, verdigris 40%), `—` (absent, `border-medium`).
— Legend: Three-item footer.
— Sticky first column: `position: sticky; left: 0`, opaque `char-surface` background.

### A7.10 Test Profiles Section

One card per profile:

— Left border: 3px, scope-colored at 50%.
— Header: Profile name (Chakra Petch 12px, 600, `bone`) + ScopeBadge.
— Description: Share Tech Mono 10px, `bpSecondary`.
— Override rows: Source name, path (`phosphor`), row limit (`iron`).

---

## A8. DATA MODEL ADDITIONS

### A8.1 Documentation Model

Derived structure, recomputed via `Memo<T>` whenever the `Pipeline` signal changes. Never stored independently.

```
PipelineDoc {
  summary: PipelineSummary,
  stages: Vec<StageDoc>,
  lineage: ColumnLineage,
  profiles: Vec<ProfileDoc>,
  generated_at: DateTime,
}

PipelineSummary {
  name: String,
  file: String,
  description: String,
  source_format: String,
  source_path: String,
  source_glob_matches: Option<usize>,
  sink_format: String,
  sink_path: String,
  stage_count: usize,
  pass_1_count: usize,
  pass_2_count: usize,
  final_column_count: usize,
  pipeline_note: Option<String>,        // from _notes.pipeline in YAML
}

StageDoc {
  index: usize,
  id: StageId,
  label: String,
  step_type: StepType,
  pass: PassAssignment,
  config_entries: Vec<(String, String)>,
  description: String,
  columns_at_output: Vec<String>,
  columns_added: Vec<String>,
  columns_removed: Vec<String>,
  note: Option<String>,                 // from _notes.stage in YAML
  field_annotations: Vec<FieldAnnotation>,  // from _notes.fields in YAML
}

FieldAnnotation {
  field_key: String,                    // matches config field path (e.g., "expr", "fields[0].name")
  text: String,                         // plain text annotation
}

ColumnLineage {
  columns: Vec<ColumnEntry>,
}

ColumnEntry {
  name: String,
  origin_stage: StageId,
  origin_index: usize,
  present_at: Vec<StageId>,
}

ProfileDoc {
  name: String,
  scope: ProfileScope,
  description: String,
  file_path: String,
  overrides: Vec<OverrideDoc>,
}

OverrideDoc {
  source_name: String,
  source_type: String,
  path: String,
  row_limit: Option<usize>,
  column_subset: Option<Vec<String>>,
}
```

### A8.2 Description Generation

Deterministic string formatting from step type and config values. Not LLM-generated.

| Step Type | Template Pattern |
|----------|-----------------|
| source | `Reads {format} data from {path}. {glob_count} files matched. Header row {present\|absent}.` |
| filter | `Filters rows where {expr} evaluates to true.` |
| map | `Computes {n} derived field(s): {field_list}. Row count is preserved — map never adds or removes rows.` |
| lookup_join | `Left joins against {file} on {key}. Adds {n} field(s): {field_list}. Non-matching rows retain null values for joined columns.` |
| distinct | `Deduplicates on the composite key ({key_list}). First occurrence is retained.` |
| sort | `Sorts by {key_list} in {direction} order. Row count is preserved.` |
| sink | `Writes {format} output to {path}. Overwrite {enabled\|disabled}.` |

Pipeline-level: `{verb} {source_format} data from {source_path}, applies {stage_count} transform(s), and writes {sink_format} output to {sink_path}.`

All templates reference structural configuration only. No template may reference row counts, deltas, percentages, timing, or any value that requires a pipeline run.

### A8.3 Column Lineage Computation

Walk the stages array in order, tracking the column set at each output:

1. Initialize from the source stage's output columns.
2. Per stage, apply mutations: `map` adds fields, `lookup_join` adds fields, `rename` swaps names, `select` replaces the set, `drop` removes columns. `filter`/`distinct`/`sort` are pass-through.
3. Record `origin_stage` (first introduction) and `present_at` (all stages where present) per column.

Lineage matrix symbols: `+` = introduced (verdigris bold), `·` = present (verdigris 40%), `—` = absent (`border-medium`).

---

## A9. MARKDOWN EXPORT

### A9.1 README Export

Documentation-oriented markdown for repository root or `docs/`. Structural content only — no runtime metrics.

Sections: Pipeline title + blockquote description → Pipeline Overview table (Source, Sink, Stages, Architecture, Output Columns) → Per-stage headings with description and column changes → Column Lineage matrix → Test Profiles with override tables → Generator footer.

Stage headings format: `### 00 · csv_reader (source) — Pass 1`

### A9.2 Runbook Export

Operations-oriented markdown for handoff to operators. Includes CLI execution examples and deterministic troubleshooting section.

Sections: Quick Reference table (Pipeline File, Source, Output, Output Columns, External Dependencies) → Execution code block (production + test profile commands) → Pipeline Stages table (# / Stage / Type / Pass / Notes / Column Changes) → Troubleshooting → Test Profiles summary.

### A9.3 Troubleshooting Generation (Runbook Only)

Deterministically generated from stage types:

— **filter**: "Row count significantly lower than expected" → references the filter expression.
— **distinct**: "High duplicate count" → references dedup keys, suggests upstream join verification.
— **map**: "Missing columns in output" → lists derived columns and source dependencies.
— **lookup_join**: "External dependencies" → lists lookup file path and failure modes.
— **sink**: "Output file issues" → notes overwrite behavior and path accessibility.

Only generated for stages where the pattern applies.

### A9.4 Export Mechanics

— Triggered from Schematics sidebar export buttons or pipeline docs header buttons.
— Markdown exports generate from the `PipelineDoc` model via template formatting. Native save dialog with suggested filename: `{pipeline_name}_README.md` or `{pipeline_name}_RUNBOOK.md`.
— PDF export generates from the `PipelineDoc` model via the document generation pipeline (see A9.5).
— No intermediate preview — the Schematics view already shows what will be exported.
— **Notes in exports**: Stage notes render as blockquotes below auto-generated descriptions. Field annotations render as sub-items under their respective config entries. Pipeline-level notes render below the pipeline description. Empty notes are omitted silently.

### A9.5 PDF Export

The PDF export produces a styled document using the Rustpunk document palette from RUSTPUNK_AESTHETICS.md. Unlike the markdown exports (which are plain text for maximum compatibility), the PDF preserves the Blueprint visual identity.

**Schematics sidebar button**: `↓ Export PDF` with verdigris text, `bpAccent` border at 30%. Positioned below the markdown export buttons.

**Document structure**:

— **Cover page**: `doc-surface-dark` (#1C1610) background. Pipeline name in Saira Stencil One 36pt, `bone` text. Pipeline description in Chakra Petch 14pt, 300 weight. Generation date. "CLINKER KILN · PIPELINE SCHEMATIC" footer. Verdigris accent rule.
— **Pipeline Overview**: Same content as the Schematics summary section (A7.7). Table with `oxide-red` header row background, `doc-border` cell borders. Chakra Petch headings, JetBrains Mono values.
— **Stage Details**: One section per stage. Stage name as H2 with accent-colored left border. Auto-generated description + stage note (if present, as an indented block with `iron` left rule). Config entries as a compact table. Field annotations inline below annotated entries. Column changes as tagged inline lists.
— **Column Lineage**: Full matrix table. `+` and `·` markers. `verdigris-dark` (#2D7A76) for introduced columns on white background (WCAG-safe per RUSTPUNK_AESTHETICS.md §Document Palette).
— **Test Profiles**: One sub-section per profile with scope badge, description, and override table.
— **Footer**: Page numbers in Chakra Petch 8pt. Pipeline name + "CLINKER KILN AUTODOC" in `doc-text-tertiary`.

**Palette mapping** (web → document):

| Web Token | Document Token | Notes |
|-----------|---------------|-------|
| `bpAccent` (#43B3AE) | `verdigris-dark` (#2D7A76) | WCAG AA safe on white |
| `bone` (#C4A882) | `doc-text` (#1C1610) | Dark text on light background |
| `char-surface` (#0A0804) | `doc-surface` (#F5F0E8) | Card/callout backgrounds |
| `border-subtle` (#1A1510) | `doc-border` (#D4C9B8) | Table and divider borders |
| `oxide-red` (#B7410E) | `oxide-red` (#B7410E) | Passes AA at 5.5:1 on white |
| `phosphor` (#D4A017) | `hazard-dark` (#9A6B00) | WCAG AA safe on white |

**Implementation**: Generated using a Rust PDF library (e.g., `printpdf` or `genpdf` crate). Fonts embedded: Chakra Petch (OFL), JetBrains Mono (OFL), Saira Stencil One (OFL, cover page only). Suggested filename: `{pipeline_name}_SCHEMATIC.pdf`.

**Page layout**: Letter size (8.5" × 11"). Margins per RUSTPUNK_AESTHETICS.md document spec: top/bottom 1.0", left 1.25", right 1.0". Content width: 6.25".

---

## A10. AESTHETIC SPECIFICATION

### A10.1 Sub-Aesthetic Mapping

| Region | Active When | Accent | Text Font | Background |
|--------|------------|--------|-----------|------------|
| Inspector header | Always | Stage accent (Oxide) | Chakra Petch | `char-surface` + scanlines |
| Config section | Always | Stage accent (Oxide) | JetBrains Mono | `char-surface` + scanlines |
| Drawer toggle bar | Always | Matches active drawer | Chakra Petch | `char-raised` |
| Run drawer | Run active | `phosphor` | JetBrains Mono | `char-surface` + scanlines |
| Docs drawer | Docs active | `bpAccent` / `verdigris` | Share Tech Mono | Blueprint gridlines |
| Notes drawer | Notes active | `iron` | JetBrains Mono | `char-surface` + scanlines |
| Expanded preview | Preview open | Stage accent | JetBrains Mono | `char` + scanlines |
| Schematics layout | Schematics mode | `bpAccent` / `verdigris` | Share Tech Mono | Blueprint gridlines |

### A10.2 Blueprint Gridline Pattern

— Major grid: `bpAccent` at 6–8%, 80×80px, 1px lines.
— Minor grid: `bpAccent` at 3–5%, 16×16px, 1px lines.
— CSS: Four `linear-gradient` layers. `pointer-events: none`.

### A10.3 Pinned Column Backgrounds

All sticky/pinned cells in the expanded preview use **pre-composited opaque colors** to prevent content bleed-through during horizontal scroll:

| Element | Hex | Derivation |
|---------|-----|------------|
| Pinned header cell | `#1C110C` | oxide-red 9% over `char-raised` |
| Separator strip | `#15100D` | accent 8% over `char-raised` |
| Pinned body cell (even row) | `char-surface` (`#0A0804`) | Direct token — already opaque |
| Pinned body cell (odd row) | `char-raised` (`#14110C`) | Direct token — already opaque |

Semi-transparent backgrounds (`accent + "18"`, `accent + "15"`) are never used on sticky-positioned cells. The separator strip includes `box-shadow: -3px 0 0 0 #15100D` to seal sub-pixel gaps at the boundary.

### A10.4 Scrollbar Styling

All autodoc and preview panels:

— Track: `char`.
— Thumb: `border-medium`, 3px border-radius.
— Thumb hover: `bpAccent` at 40% (docs/schematics) or `border-strong` (run/preview).
— Width: 5–6px.

---

## A11. PHASED BUILD PLAN ADDITIONS

### Phase 5.5 — Inspector Redesign & Run Drawer (Weeks 14–15)

**Goal**: Four-concern inspector with Config + Run/Docs/Notes drawer.

Tasks:
— Inspector panel restructure: Config always-visible section + drawer toggle bar + expandable drawer region
— Drawer toggle bar with Run/Docs/Notes buttons, active state styling, badge indicators
— Run drawer: pass statistics, compact data preview (3 rows × 3 cols), node log (filtered from main run log)
— Expanded data preview: overlay panel with column pin bar, sticky pinned columns with opaque backgrounds, row filter, footer stats, collapse interaction
— Column pinning: cumulative left offsets, separator strip with box-shadow gap sealing, pin/unpin toggle
— Title bar preview indicator
— Canvas dimming when preview is expanded

**Deliverable**: The inspector separates editing, observing, understanding, and annotating into distinct regions. Data preview scales from compact in-panel to full-width expanded overlay.

### Phase 5.75 — Docs Drawer, Notes & Schematics (Weeks 16–18)

**Goal**: Blueprint documentation, notes system, and full Schematics layout.

Tasks:
— Docs drawer content: description block, stage metadata, column impact, output columns
— Blueprint sub-aesthetic switching in drawer region (gridlines, verdigris, Share Tech Mono)
— Notes drawer: stage-level markdown note with edit/display toggle, field-level annotations with add/remove, annotation indicator (✎) on Config fields
— `_notes` YAML storage: `_notes.pipeline`, `_notes.stage`, `_notes.fields` serialization and round-trip preservation
— Autodoc compilation: weave user notes alongside auto-generated descriptions in Docs drawer and Schematics stage cards
— `PipelineDoc` derived model with `Memo<PipelineDoc>` reactive computation, including note and annotation fields
— Column lineage walker computing `ColumnLineage` from stage array
— Auto-description generator with per-step-type templates
— Pipeline docs mode (no node selected): title bar Docs button, full-panel Blueprint summary with pipeline note
— Fourth layout preset (Schematics) with flow bar, TOC sidebar, content area
— README markdown export with notes as blockquotes and field annotations as sub-items
— Runbook markdown export with troubleshooting generation

**Deliverable**: Full documentation and annotation system — per-node docs and notes in the inspector drawer, pipeline-level docs via title bar button, Schematics layout for full documentation review, and two-format markdown export with compiled notes.

### Phase 6.25 — PDF Export & Polish (Week 19)

**Goal**: Styled PDF export using Rustpunk document palette.

Tasks:
— PDF generation pipeline using `printpdf` or `genpdf` crate
— Rustpunk document palette mapping (web → print): verdigris-dark for Blueprint accent, doc-text for body, doc-surface for callout backgrounds
— Cover page: Saira Stencil One title, pipeline description, generation date, verdigris accent rule
— Interior pages: stage details with compiled notes, column lineage matrix, test profiles
— Font embedding: Chakra Petch, JetBrains Mono, Saira Stencil One (all OFL)
— Schematics sidebar `↓ Export PDF` button with oxide-red styling
— Suggested filename: `{pipeline_name}_SCHEMATIC.pdf`

**Deliverable**: Production-quality styled PDF export that preserves the Blueprint visual identity on paper.

---

## A12. OPEN QUESTIONS

— Should the column lineage track column *types* (string, int, float) in addition to presence? This would require the scan pass to report type inference results per column.
— Should the Runbook troubleshooting section include links to specific YAML line numbers for quick navigation back to the authoring view?
— Should the Schematics flow bar display warning/error badges per stage to surface validation issues while reviewing docs?
— Should markdown export include a YAML appendix with the full pipeline configuration?
— Should the expanded data preview support column reordering via drag in the pin bar?
— Should the Run drawer's node log support log level filtering (e.g., show only DAT/WRN/ERR)?
— Should the compact data preview auto-select which 3 columns to show based on the stage type, or should the user's last pin selection persist?
— Should stage notes support full markdown (headings, lists, code blocks) or only inline formatting (bold, italic, code, links)? Full markdown is more expressive but risks visual hierarchy conflicts within the Schematics stage cards.
— Should field annotations support linking to other stages (e.g., "this field is consumed by the distinct stage's dedup key")? Cross-references would make annotations more useful but add complexity to the editor and compilation.
— Should there be a pipeline-level notes editor accessible from the Schematics view directly (not just via YAML), or is the `_notes.pipeline` key in the YAML editor sufficient?
— Should the PDF export include a table of contents with page numbers? Useful for long pipelines, but adds generation complexity.
— Should notes be diffable in version control — i.e., should the `_notes` YAML block be formatted for minimal diff noise (one sentence per line, consistent key ordering)?

---

— Feed the kiln. Read the schematic. Watch it run. —
RUSTPUNK · CLINKER KILN · INSPECTOR & AUTODOC ADDENDUM · v0.2.0 · 2026
