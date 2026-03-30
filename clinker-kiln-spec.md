# CLINKER KILN — IDE Design Specification
// pipeline configuration IDE for clinker YAML pipelines

**Version**: 0.1.0 · DRAFT
**Target**: Dioxus 0.7 desktop application (Rust)
**Aesthetic**: Rustpunk (Oxide sub-aesthetic, Phosphor for terminal elements)
**Date**: March 2026

---

## 1. OVERVIEW AND PURPOSE

Clinker Kiln is a dedicated IDE for authoring, visualizing, and testing Clinker YAML pipeline configurations. It replaces the workflow of editing YAML in a text editor, running `clinker` from a terminal, and mentally reconstructing the pipeline's data flow — unifying all three into a single environment with bidirectional feedback.

### 1.1 Design Principles

— **Bidirectional truth**: The canvas, node inspector, and YAML editor are three views of one data model. Edits in any view propagate to all others in real time. There is no "primary" view.

— **Progressive disclosure**: The canvas shows pipeline topology. Clicking a node reveals its inspector with structured form fields. The scoped YAML editor inside the inspector surfaces the raw config. The full YAML sidebar shows everything. Each layer adds detail without hiding the others.

— **2-pass awareness**: Clinker's 2-pass architecture (scan → transform) is a first-class concept in the UI. Nodes display their pass assignment. The run log separates pass 1 and pass 2 output. The inspector shows scan statistics from pass 1 alongside transform results from pass 2.

— **Rustpunk identity**: Every surface follows the RUSTPUNK_AESTHETICS.md specification. The IDE should feel like a control panel bolted to factory infrastructure — oxidized, textured, angular, functional.

### 1.2 Core Features (v1)

— Node-graph pipeline canvas (draggable, pannable, zoomable)
— Slide-in node inspector with form fields, pass stats, and scoped YAML editor
— Full pipeline YAML sidebar with line numbers, syntax highlighting, and gutter
— Toggleable run log drawer with CLI execution and log streaming
— Bidirectional selection sync (canvas ↔ YAML ↔ inspector)
— Cross-highlighting of validation errors across canvas and editor
— Live schema validation against clinker-pipeline-schema-3.json
— Schema-powered YAML autocomplete
— Data preview per pipeline stage
— Pass 1 scan stats on node hover
— Multi-pipeline tabs
— Three layout presets: Canvas Focus, Hybrid, Editor Focus
— Source glob/regex pattern support with file match preview
— Test profiles with project (shared/committed) and user (personal/gitignored) scopes
— Per-source test data override with row limit and column subset controls

---

## 2. ARCHITECTURE OVERVIEW

### 2.1 Application Shell

The app is a Dioxus 0.7 desktop application using a custom window frame (no native title bar). The window is divided into five primary zones arranged in a flex layout:

```
┌────────────────────────────────────────────────────────────────────┐
│  TITLE BAR  [clinker][kiln]  file.yaml  [TEST: sample-50]  [Hybrid]  ● VALID │
├────────────────────────────────────────────────────────────────────┤
│              │              │           │                          │
│              │  INSPECTOR   │  TEST     │                          │
│   CANVAS     │  (slide-in)  │  PROFILES │   YAML SIDEBAR           │
│   (draggable)│              │  (toggle) │   (line numbers + gutter) │
│              │  form fields │           │                          │
│              │  stats       │  project  │                          │
│              │  node yaml   │  user     │                          │
│              │              │           │                          │
├────────────────────────────────────────────────────────────────────┤
│  ◆ RUN LOG  [TEST sample-50 ⊞ PROJECT]  [toggleable drawer]       │
└────────────────────────────────────────────────────────────────────┘
```

Panel order in Hybrid mode: **Canvas → Inspector → Test Profiles (toggleable) → YAML Sidebar**. The inspector slides in from behind the YAML sidebar, pushing the canvas narrower. The test profiles panel is toggled via the title bar badge.

### 2.2 Data Model

The central data structure is a `Pipeline` model parsed from YAML. All UI components subscribe to this reactive model. Edits from any source (canvas drag, inspector form field, YAML text edit) mutate the model, which triggers re-renders across all views.

```
Pipeline {
  source: SourceConfig,
  stages: Vec<StageConfig>,
  sink: SinkConfig,
  metadata: PipelineMeta,
}

StageConfig {
  id: StageId,              // stable UUID, not YAML position
  step_type: StepType,      // filter, map, lookup_join, distinct, ...
  config: serde_yaml::Value, // arbitrary config per step type
  position: CanvasPosition,  // x, y on canvas (not persisted to YAML)
  pass: PassAssignment,      // P1 (scan) or P2 (transform)
  validation: ValidationState,
}
```

### 2.3 Sync Engine

The bidirectional sync engine is the architectural core. Three data representations must stay consistent:

— **Structured model** (`Pipeline`): The canonical source of truth.
— **YAML text** (`String`): Serialized from the model, or parsed into it.
— **Canvas graph** (`Vec<CanvasNode>`): Visual positions and connections derived from the model.

Edit flows:

1. **Form field edit** → mutate `Pipeline` model → serialize to YAML text → update canvas node state
2. **YAML text edit** → parse to `Pipeline` model (with error recovery) → update canvas + inspector
3. **Canvas action** (add/remove/reorder node) → mutate `Pipeline` model → serialize to YAML → update inspector

**Conflict resolution**: The most recent edit source wins. Parse errors in YAML do not destroy the model — the last valid parse is retained and errors are displayed inline.

**Selection sync**: A single `SelectedStage: Signal<Option<StageId>>` signal drives all highlight behavior. Changing it from any source (canvas click, YAML cursor position, inspector navigation) updates all views simultaneously.

### 2.4 Schema Integration

The JSON Schema (`clinker-pipeline-schema-3.json`) is loaded at startup and used for:

— **Validation**: Every model mutation triggers async validation. Errors are mapped to YAML line ranges and canvas node IDs, enabling cross-highlighting.
— **Autocomplete**: The schema's `properties`, `enum`, and `$ref` definitions power context-aware YAML completions.
— **Form generation**: The inspector's form fields are generated from the schema's stage-type definitions. Each step type's schema properties become labeled input fields.

---

## 3. PANEL SYSTEM AND LAYOUT

### 3.1 Layout Presets

Three preset layouts, selectable from the title bar:

| Preset | Canvas | Inspector | YAML Sidebar | Use Case |
|--------|--------|-----------|--------------|----------|
| Canvas Focus | 100% (- inspector if open) | Slides in from right | Hidden | Visual pipeline design, drag-and-drop |
| Hybrid | ~62% (- inspector if open) | Between canvas and YAML | ~38% (360px fixed) | Primary authoring mode |
| Editor Focus | Hidden | Hidden | 100% | YAML-first editing, bulk config |

Switching presets animates panel widths over 300ms with easing.

### 3.2 Inspector Panel Behavior

— **Trigger**: Click a node on canvas, or click a YAML block in the editor.
— **Position**: Slides in between the canvas and YAML sidebar (Hybrid) or from the right edge (Canvas Focus).
— **Width**: Default 340px, user-resizable via drag handle (min 260px, max 520px).
— **Drag handle**: A thin vertical bar on the inspector's left edge with a gradient grip indicator. Cursor changes to `col-resize` on hover.
— **Content transition**: Selecting a different node while the inspector is open crossfades the content in place (250ms ease). No slide-out/slide-in.
— **Close**: Click the × button, click the selected node again, or press Escape.
— **Push behavior**: The inspector pushes the canvas narrower. The YAML sidebar width is fixed.

### 3.3 Run Log Drawer

— **Position**: Anchored to the bottom edge, spanning the full viewport width.
— **Toggle**: Click the "Terminal" tab bar to expand (220px) or collapse (28px).
— **Animation**: Height transition over 300ms with easing.
— **Auto-scroll**: When expanded, the log auto-scrolls to the bottom (latest output).
— **Z-index**: Overlaps the main content area (sits on top of canvas/editor bottom edge).

---

## 4. CANVAS SYSTEM

### 4.1 Viewport

The canvas is an infinite 2D viewport. Nodes are positioned in world-space coordinates. The viewport supports:

— **Pan**: Click-drag on empty canvas area (or middle-mouse-drag anywhere) to translate the viewport.
— **Zoom**: Scroll wheel to zoom in/out (range: 25%–400%). Zoom targets the cursor position.
— **Fit-to-view**: Double-click empty canvas to auto-fit all nodes with padding.
— **Grid**: Subtle dot grid (20px spacing, `border-subtle` color) that scales with zoom. Provides spatial reference without visual noise.

### 4.2 Node Rendering

Each stage is rendered as a uniform card following the rustpunk card anatomy:

```
┌─ border-top (3px, accent color, brighter on selection)
│
│  ● STATUS_TYPE_BADGE
│  
│  LABEL (Chakra Petch 600)
│
│  ── rust line ──
│
│  subtitle (JetBrains Mono, iron)
│
└─ eroded corner (asymmetric: top-right 16px, bottom-left 14px)
```

Surface: `char-surface` background, `border-subtle` border. Scanlines overlay. Noise canvas overlay at 30% opacity. Eroded corners disappear when selected.

Node dimensions: 160px wide (compact). Height varies by content (typically 80-100px).

### 4.3 Stage Accent Colors

Each stage type maps to exactly one accent color. No mixing within a node.

| Stage Type | Accent | Spec Token |
|-----------|--------|------------|
| source | `#43B3AE` | verdigris |
| filter | `#E8A524` | hazard |
| map | `#C75B2A` | ember |
| lookup_join | `#43B3AE` | verdigris |
| distinct / aggregate | `#D4A017` | phosphor |
| sink | `#B7410E` | oxide-red |

### 4.4 Connectors

Connectors between nodes use a three-layer SVG rendering for high contrast against the dark canvas:

1. **Glow layer**: 5px stroke, node accent color, 10% opacity (soft halo)
2. **Core cable**: 2px stroke, dashed (8px dash, 4px gap), 70% opacity
3. **Bright center**: 0.75px stroke, solid, 90% opacity (hot wire effect)
4. **Arrowhead**: Open chevron at the target end, 1.5px stroke, 80% opacity

Port connectors on each node: 8×8px squares with 2px border-radius, `char-raised` fill, `border-medium` border (accent border on selection).

### 4.5 Canvas Interactions

— **Select node**: Click. Highlights node, opens inspector, syncs YAML highlight.
— **Multi-select**: Ctrl+click or drag-select rectangle.
— **Add node**: Right-click canvas → context menu with stage types. Inserts at click position.
— **Remove node**: Select → Delete key or right-click → Remove.
— **Reorder**: Drag a node left/right to change pipeline position. Connectors update live.
— **No hover transforms**: Per rustpunk spec — border color shifts only, no scale/bounce.

---

## 5. NODE INSPECTOR

### 5.1 Panel Structure

The inspector is a vertical scrollable panel divided into three sections:

```
┌─ border-top (3px, node accent color)
│
│  ◆ HEADER — node label + type badge + × close
│
├─ CONFIGURATION ─────────────────────────
│
│  FIELD_KEY          label (Chakra Petch 9px)
│  [input value]      input (JetBrains Mono 11px)
│  
│  FIELD_KEY
│  [select ▾]
│
│  FIELD_KEY
│  [toggle ○──]  value
│
│  FIELDS (list)      section header
│  ┌──────────────────────────── [×]
│  │ name  [full_name     ]
│  │ expr  [first + ' ' + last]
│  └────────────────────────────
│  ┌──────────────────────────── [×]
│  │ name  [email_lower   ]
│  │ expr  [lower(email)  ]
│  └────────────────────────────
│  [+ Add Field]
│
│  [+ Add Property]  (schema-aware)
│
├─ PASS STATISTICS ───────────────────────
│
│  IN 12,847   OUT 9,231   PASS 1   Δ -3,616
│
├─ NODE YAML ─────────────────────────────
│
│  ▊ - step: filter
│  ▊   expr: 'status == "active"'
│
└─────────────────────────────────────────
```

### 5.2 Form Field Generation

Fields are generated from the JSON Schema's definition for the selected step type. Field types:

— **text**: Standard input for string/expression values. `char` background, `border-medium` border.
— **select**: Dropdown for enum values. Shows current value with a ▾ indicator.
— **toggle**: Boolean switch. 28×14px track, accent color when true.
— **label**: Non-editable display for the step type identifier. Accent-tinted background.
— **list**: For array fields (e.g., `fields`, `by`). Renders as a repeating group of sub-fields inside bordered cards. See below.

All field edits immediately mutate the Pipeline model, triggering YAML re-serialization and canvas updates.

### 5.2.1 List Field Add/Remove

Array-type config fields (like `map.fields`, `distinct.by`, `lookup.fields`) render as a list of removable cards:

— Each item is a bordered card (`border-subtle`, `char` background) containing its sub-fields (e.g., `name` + `expr` for a map field).
— Each card has a **× remove button** in the top-right corner. Click to remove the item from the array. Confirmation is not required — undo is available.
— Below the last card, an **[+ Add Field]** button appends a new empty item with default/placeholder values. The new item's first input is auto-focused.
— Items can be **reordered** by drag-handle (grip icon on the left edge of each card). Reordering updates the YAML array order.
— Removing the last item in a required array shows a warning badge but does not prevent the removal.

### 5.2.2 Add/Remove Properties

Beyond list items, the inspector supports adding and removing top-level properties on a node:

— An **[+ Add Property]** button at the bottom of the configuration section opens a dropdown of available properties from the schema that are not yet configured on this node. Selecting one adds the field with its default value.
— Each non-required property has a **× remove** icon next to its label. Clicking it removes the property from the node config and the YAML.
— Required properties (like `step`) cannot be removed — their remove icon is hidden.
— The dropdown is schema-aware: it only shows properties valid for the current step type, and omits properties already present.

### 5.2.3 Add/Remove Styling

— **[+ Add]** buttons: `border-medium` border, `char-raised` background, `iron` text, Chakra Petch 9px uppercase. On hover: border shifts to `border-strong`, text to `bone`.
— **[× Remove]** buttons: 16×16px, `char` background, `text-floor` text (× glyph). On hover: border shifts to `oxide-red` at 40%, × shifts to `oxide-red`.
— **Drag handles**: 6×16px grip area with three horizontal lines in `border-medium`. Cursor: `grab` (→ `grabbing` while dragging).
— No hover transforms — border/color shifts only, per rustpunk spec.

### 5.3 Scoped YAML Editor

The bottom section of the inspector contains an editable YAML block showing only the selected node's configuration. It has:

— A 3px accent-colored left border
— `char` background with `border-medium` border
— Phosphor-colored syntax highlighting (terminal sub-aesthetic)
— Edits here parse back into the model and update both the form fields above and the full YAML sidebar

### 5.4 Pass Statistics

A compact stats row showing:

— **IN**: Row count entering this stage
— **OUT**: Row count exiting this stage (hazard color if different from IN)
— **PASS**: Which pass this stage runs in (1 or 2)
— **Δ**: Difference (oxide-red color, only shown when rows are lost)

In live execution mode, these update in real time as the pipeline runs.

---

## 6. YAML EDITOR (SIDEBAR)

### 6.1 Structure

The YAML sidebar is a full-height code editor panel with:

— **Section header**: Diamond + "PIPELINE YAML" + trailing rule + filename
— **Gutter**: 36px wide, `char` background, `border-subtle` right border. Line numbers in JetBrains Mono 10px, `text-floor` color.
— **Code area**: JetBrains Mono 11px, 20px line height. Syntax-colored by token type.

### 6.2 Syntax Coloring

| Token Type | Color | Spec Token |
|-----------|-------|------------|
| Comment | `#5C4A30` | text-floor |
| Key | `#43B3AE` | verdigris |
| Value (string) | `#D4A017` | phosphor |
| Property | `#C4A882` | bone |
| Punctuation | `#8A7E6E` | iron |

### 6.3 Selection Highlighting

When a stage is selected (from any source), the corresponding YAML lines receive:

— Tinted background: node accent color at 12% opacity
— Left border: 2px solid in the node accent color
— Gutter highlight: accent at 15% opacity, line numbers shift to `iron` color
— Auto-scroll: If the highlighted block is offscreen, smooth-scroll to center it

### 6.4 Click-to-Select

Clicking a line in the YAML editor determines which stage it belongs to (by line range mapping) and selects that stage globally — triggering canvas node highlight and inspector panel open/swap.

### 6.5 Live Editing

In the real Dioxus implementation:

— Every keystroke triggers incremental YAML parsing with error recovery
— Valid parses update the Pipeline model immediately
— Invalid YAML shows inline diagnostics (red squiggle, error tooltip) without destroying the model
— Schema violations appear as warnings (yellow squiggle)
— Autocomplete suggestions appear on Ctrl+Space or after typing a colon

---

## 7. RUN LOG

### 7.1 Drawer Behavior

— Collapsed state: 28px tab bar at the bottom of the viewport. Shows "Run Log" label with status LED. When a test override is active, displays the profile name and scope badge (PROJECT/USER) next to the label.
— Expanded state: 220px height. Tab bar + scrollable log content.
— Toggle: Click tab bar. Animates height over 300ms.

### 7.2 Log Display

Each log line has four columns:

| Column | Width | Content | Style |
|--------|-------|---------|-------|
| Timestamp | 72px | `00:00.000` format | text-floor |
| Level tag | 32px | `$`, `INF`, `DAT`, `WRN`, `OK`, `ERR` | Level-colored |
| Message | flex | Log message content | Level-colored |

### 7.3 Level Colors

| Level | Tag | Color | Background Tint |
|-------|-----|-------|----------------|
| cmd | `  $` | bone (bold) | none |
| info | `INF` | iron | none |
| stat | `DAT` | verdigris | none |
| warn | `WRN` | hazard | hazard at 8% |
| ok | ` OK` | #7ab648 (bold) | none |
| err | `ERR` | oxide-red | oxide-red at 8% |

### 7.4 Content Structure

The run log shows the full lifecycle of a `clinker run` invocation:

1. CLI command prompt (`$ clinker run file.yaml --verbose --profile sample-50`)
2. Engine startup and schema validation
3. Test override warning (if active): profile name, scope, overridden source paths
4. Pass 1 separator (`─── PASS 1: SCAN ───`)
5. Per-stage scan output with row counts, column types, precomputed stats
6. Pass 2 separator (`─── PASS 2: TRANSFORM ───`)
7. Per-stage transform output with row counts, deltas, warnings
8. Completion summary with total rows and elapsed time

### 7.5 Future: Live Streaming

In the production build, the run log will stream output from a spawned `clinker` child process in real time, with ANSI escape code parsing for color.

---

## 8. TITLE BAR

### 8.1 Elements

From left to right:

— **Brand badge**: Two-segment `[clinker][kiln]` badge. Label segment: `border-medium` bg, `bone` text. Value segment: `border-strong` bg, `oxide-red` text.
— **Vertical divider**: 1px × 14px, `border-medium` color.
— **Filename**: `customer_etl.yaml` in JetBrains Mono 10px, `text-floor` color.
— **Flex spacer**
— **Test override badge** (when active): Hazard-tinted pill showing "TEST: profile-name" with hazard LED. Clicking opens/closes the test profiles panel. When override is off, shows a neutral "PROFILES" button instead.
— **Layout switcher**: Three-segment button group (Canvas | Hybrid | Editor). Active segment: `border-strong` bg, `bone` text. Inactive: `char-raised` bg, `text-floor` text.
— **Validation status**: Green LED (5px circle, #7ab648, box-shadow glow) + "VALID" label in Chakra Petch 9px.

---

## 9. SOURCE PATTERNS AND TEST PROFILES

### 9.1 Glob and Regex Source Patterns

Clinker sources use glob patterns and regex for file matching rather than hardcoded file paths. The source `path` field accepts patterns like `./data/customers_*.csv` or `./data/invoices_202[3-5]*.csv`.

**Canvas node display**: Source and lookup nodes that use glob/regex patterns show a match count badge (e.g., "47 files") on the node itself, visible at a glance without opening the inspector.

**Inspector display**: The source inspector shows the pattern in the `path` field, plus a collapsible "MATCHED FILES" section listing resolved file paths with a count. Files are listed in a scrollable panel (max 100px height) with accent-colored ▸ bullets and an overflow indicator ("... and 42 more").

### 9.2 Test Override System

When testing a pipeline (run or dry-run), users can override source paths to point to test data without modifying the pipeline YAML itself. Overrides support three capabilities:

— **Path override**: Replace the source glob/path with a specific test file path.
— **Row limit**: Optionally cap the number of rows read (e.g., first 100 rows). Useful for fast iteration.
— **Column subset**: Optionally restrict which columns are loaded. Useful for isolating specific fields during debugging.

Overrides are managed through **test profiles** — named configurations saved to disk alongside the pipeline YAML.

### 9.3 Test Profile Scopes

Profiles exist in two scopes, visually differentiated by accent color and icon:

| Scope | Directory | Version Control | Accent | Icon | Use Case |
|-------|-----------|----------------|--------|------|----------|
| **Project** | `.clinker-test/` | Committed to repo | verdigris | ⊞ | CI, regression suites, shared fixtures, staging mirrors |
| **User** | `.clinker-test.local/` | Gitignored | ember | ▸ | Personal debug subsets, benchmarks, scratch data |

**Override priority**: When both a project and user profile configure the same source, the user profile's override takes precedence.

**Scope badges**: Each profile displays a `ScopeBadge` component — Chakra Petch 8px uppercase, scope-colored background tint and border, with the scope icon. The badge appears in profile cards, the title bar, and the run log drawer.

### 9.4 Test Profile Panel

The test profiles panel is a toggleable right-side panel (300px wide), accessed by clicking the test override badge in the title bar. Structure:

```
┌─ border-top (3px, hazard at 60%)
│
│  ◆ TEST PROFILES                    [toggle on/off]
│
├─ GLOBAL BASE DIRECTORY ─────────────
│  [./test/fixtures/                ]
│
├─ [All (5)] [Project (3)] [User (2)] ── scope tabs
│
├─ ⊞ PROJECT ── .clinker-test/ — committed ──
│
│  (●) sample-50        ⊞ PROJECT   2src  [edit] [×] [▸]
│      50 rows from each source
│      ┌─── csv_reader ─── source ──────── [×]
│      │ ./test/fixtures/customers_sample.csv
│      │ rows: 50   cols: all
│      └────────────────────────────────────
│      ┌─── regions ─── lookup ─────────── [×]
│      │ ./test/fixtures/regions_small.csv
│      │ rows: 10   cols: all
│      └────────────────────────────────────
│      [+ Add Source Override]
│
│  ( ) staging-mirror    ⊞ PROJECT   2src  [edit] [×] [▸]
│  ( ) edge-cases        ⊞ PROJECT   1src  [edit] [×] [▸]
│
├─ ▸ USER ── .clinker-test.local/ — gitignored ──
│
│  ( ) debug-subset      ▸ USER      1src  [edit] [×] [▸]
│  ( ) perf-1M           ▸ USER      2src  [edit] [×] [▸]
│
│  [+ Project Profile]  [+ User Profile]
│
├─ ● active: sample-50 ⊞ PROJECT  yaml unchanged
└─────────────────────────────────────
```

### 9.5 Profile Card Interaction

— **Click card body**: Expand or collapse the profile to show/hide source overrides.
— **Click radio dot**: Select/deselect this profile as the active override (independent of expand).
— **Click "edit"**: Enter edit mode — profile name becomes an inline input, description/row limit/column subset fields appear as editable inputs, source override paths become editable, remove buttons appear on each source, and an "Add Source Override" button is shown.
— **Click "done"**: Exit edit mode, lock in changes.
— **Click ×**: Delete the profile entirely.
— **Click ▸ chevron**: Expand/collapse (redundant with card body click, but provides a visible affordance).

### 9.6 Profile Editing

In edit mode, all fields become live-editable:

— **Profile name**: Inline input in the card header row.
— **Description**: Text input below the header.
— **Row limit**: Text input (applies globally to all sources in this profile).
— **Column subset**: Text input (comma-separated column names, or "all").
— **Per-source overrides**: Each source override card exposes editable fields for name, path, rows, and cols. Sources can be removed (× button) or added ("+ Add Source Override" button appends a blank card with defaults and enters edit mode).

All edits persist to the `.clinker-test/` or `.clinker-test.local/` YAML file for the profile.

### 9.7 Profile Lifecycle

— **Persist until manually cleared**: Overrides stay in the profile file until the user edits or deletes them.
— **Saved to disk**: Each profile is a `.yaml` file in the appropriate scope directory.
— **Toggle on/off without losing config**: The master toggle in the panel header enables/disables all overrides globally. Turning off does not delete profiles — it simply tells the engine to use production sources. Turning back on restores the last-active profile.

### 9.8 Integration Points

— **Canvas nodes**: Source and lookup nodes display a hazard-tinted "TEST OVERRIDE" badge when the active profile overrides their source path.
— **Title bar**: Shows the active profile name in a hazard-tinted pill. Clicking toggles the profiles panel.
— **Run log**: Tab bar shows "TEST" badge + profile name + scope badge. Log output includes a warning line identifying the active profile and the overridden source paths.
— **CLI invocation**: The run command includes `--profile profile-name` when an override is active.
— **Scope filter tabs**: The panel has All / Project / User tabs with colored underlines and profile counts, allowing quick filtering.

---

## 10. DIOXUS-SPECIFIC CONSIDERATIONS

### 10.1 Rendering Approach

The canvas requires either a custom renderer or an embedded WebView/Canvas element. Options:

— **Dioxus native rendering** with custom `div`-based nodes and CSS transforms for pan/zoom. Simplest but may have performance issues with many nodes.
— **wgpu canvas** embedded in the Dioxus layout. Hardware-accelerated, best for complex graphs, but requires a custom rendering pipeline.
— **SVG overlay** within a Dioxus `div`. Each node is an SVG group. Pan/zoom via `viewBox` manipulation. Good middle ground.

Recommended: Start with the CSS transform approach (Phase 1), migrate to wgpu if performance is insufficient (Phase 4+).

### 10.2 State Management

— Use Dioxus `Signal<T>` for all reactive state (selected node, layout mode, run log open/closed, inspector width, active test profile).
— The `Pipeline` model lives in a `Signal<Pipeline>` at the app root.
— Derived computations (YAML text, validation errors, canvas node positions) use `Memo<T>`.
— The sync engine is implemented as a set of effects (`use_effect`) that watch for model changes and propagate updates.

### 10.3 YAML Editing

For the YAML editor, options include:

— **tree-sitter-yaml** for incremental parsing and syntax highlighting. Rust-native, fast, error-tolerant.
— **Custom Rope data structure** for efficient text editing (large files). Consider `ropey` crate.
— **Monaco-style editing** features (cursor, selection, undo/redo) will need a custom text editor component or integration with an existing Rust editor widget.

### 10.4 Schema Validation

— Load `clinker-pipeline-schema-3.json` at startup using `serde_json`.
— Validate using the `jsonschema` crate (or `valico`).
— Map validation errors back to YAML source positions using `serde_yaml`'s `Location` tracking.
— Debounce validation (100ms after last edit) to avoid blocking the UI thread.

### 10.5 CLI Execution

— Spawn `clinker` as a child process using `tokio::process::Command`.
— When a test profile is active, pass `--profile profile-name` and `--profile-dir .clinker-test/` (or `.clinker-test.local/`) to the CLI.
— Stream stdout/stderr line-by-line into the run log model.
— Parse ANSI escape codes for color mapping.
— Support cancellation (kill child process on user request or pipeline re-run).

---

## 11. PHASED BUILD PLAN

### Phase 1 — Foundation (Weeks 1–3)

**Goal**: App shell, panel system, static canvas, and YAML viewer.

Tasks:
— Dioxus 0.7 desktop app scaffold with custom window frame
— Rustpunk theme system: CSS variables, noise canvas, scanlines, rust lines
— Title bar with brand badge, filename, layout switcher, validation LED, test override badge
— Three-preset layout system (Canvas Focus / Hybrid / Editor Focus) with animated transitions
— Static canvas with node rendering (no drag/pan/zoom yet)
— Connector rendering between nodes (three-layer SVG)
— YAML sidebar with gutter, line numbers, and static syntax highlighting
— Run log drawer (collapsed/expanded toggle, static demo content)

**Deliverable**: An app that displays a hardcoded pipeline visually across all three views, with working layout presets and run log drawer.

### Phase 2 — Interactivity and Sync (Weeks 4–6)

**Goal**: Bidirectional selection sync, inspector panel, and live YAML editing.

Tasks:
— `Pipeline` data model with `Signal<Pipeline>` reactive state
— YAML parser integration (serde_yaml with error recovery)
— Bidirectional sync engine: model → YAML serialization, YAML → model parsing
— Selection sync signal (`SelectedStage`) driving canvas, YAML, and inspector highlights
— Click-to-select on canvas nodes and YAML lines
— Inspector slide-in panel with push layout behavior
— Inspector form field generation (text, select, toggle, label)
— Inspector scoped YAML editor (phosphor sub-aesthetic)
— Inspector pass statistics row
— Drag-to-resize inspector width (260–520px, default 340px)
— Crossfade content swap when selecting different nodes
— YAML editor selection highlighting (tinted lines, accent border, auto-scroll)

**Deliverable**: Full interactive authoring loop — edit in any view, see updates everywhere.

### Phase 3 — Canvas Interactions (Weeks 7–9)

**Goal**: Draggable/pannable canvas, node manipulation, and context menus.

Tasks:
— Canvas pan (click-drag empty space / middle-mouse)
— Canvas zoom (scroll wheel, cursor-centered)
— Fit-to-view (double-click empty space)
— Node drag-to-reorder (changes pipeline stage order)
— Right-click context menu: add node (by stage type), remove node
— Node add via canvas: scaffold YAML + open inspector with form fields
— Multi-select (Ctrl+click, drag-select rectangle)
— Delete selected nodes (Delete key)
— Canvas minimap (optional — small overview in corner)

**Deliverable**: The canvas is a fully interactive graph editor.

### Phase 4 — Validation and Autocomplete (Weeks 10–12)

**Goal**: Schema-driven validation, autocomplete, and error cross-highlighting.

Tasks:
— Load and parse `clinker-pipeline-schema-3.json` at startup
— Async validation on model mutation (100ms debounce)
— Map validation errors to YAML line ranges and canvas node IDs
— YAML editor: inline error diagnostics (red underline, tooltip on hover)
— YAML editor: warning diagnostics for schema violations (yellow underline)
— Canvas: error badge on nodes with validation failures (red dot + count)
— Cross-highlight: clicking an error in the YAML editor highlights the corresponding node, and vice versa
— Title bar validation LED: green when valid, red with error count when invalid
— Schema-powered autocomplete (Ctrl+Space): property names, enum values, type suggestions
— Autocomplete popup styled to rustpunk spec (char-surface bg, bone text, accent highlights)

**Deliverable**: The IDE validates pipelines in real time and helps authors fix errors.

### Phase 5 — CLI Execution, Data Preview, and Test Profiles (Weeks 13–16)

**Goal**: Run pipelines, stream output, preview data, and manage test profiles.

Tasks:
— `clinker` child process spawning with streaming stdout/stderr
— ANSI escape code parsing for run log colors
— Run log: live-updating with auto-scroll
— Run/stop controls in the run log tab bar
— Pass 1 scan stats populated on node hover (row count, column types, min/max/nulls)
— Pass 2 transform results: row counts, deltas, timing per stage
— Data preview per stage: click "Preview" in inspector to see sample rows (first 50)
— Data preview table styled to rustpunk spec (oxide-red header, alternating rows)
— Pipeline step debugger: step-through mode, pause between stages
— Re-run on save: auto-execute pipeline when YAML changes (with debounce)
— Source glob/regex pattern matching with file count display on canvas nodes
— Glob match preview panel in source inspector (scrollable file list with count)
— Test profiles panel: toggleable right-side panel with profile list
— Project profiles (.clinker-test/) and user profiles (.clinker-test.local/) with scope badges
— Profile editing: inline name editing, description, per-source override CRUD, row limit, column subset
— Profile scope filter tabs (All / Project / User)
— Profile creation (+ Project / + User) and deletion
— Master override toggle (on/off without losing config)
— Canvas "TEST OVERRIDE" badges on overridden source/lookup nodes
— Run log test profile display: profile name + scope badge in tab bar
— CLI `--profile` flag integration when running with active override

**Deliverable**: The IDE is a complete pipeline authoring, testing, and profiling environment.

### Phase 6 — Multi-Pipeline and Polish (Weeks 17–20)

**Goal**: Tab system, persistence, and production polish.

Tasks:
— Multi-pipeline tabs: open/close/reorder pipeline files
— Tab bar with rustpunk styling (active tab: border-strong bg, accent top border)
— File open/save dialogs
— Recent files list
— Undo/redo stack (model-level, not text-level)
— Keyboard shortcuts (Ctrl+S save, Ctrl+Z undo, Ctrl+Shift+Z redo, Escape close inspector)
— Drag-to-resize YAML sidebar width
— Window state persistence (layout preset, panel widths, last opened files)
— Performance optimization: virtualized YAML rendering for large files, canvas node culling
— Error boundary: graceful recovery from parse failures, child process crashes
— Installer and distribution (platform-specific packaging)

**Deliverable**: Production-ready v1.0.

---

## 12. OPEN QUESTIONS

— Should the canvas support vertical (top-to-bottom) layout in addition to horizontal (left-to-right)?
— Should the data preview show diff between input and output rows for transform stages?
— Should there be a "pipeline gallery" for browsing example pipelines?
— Should the inspector support batch-editing multiple selected nodes?
— Should test profiles support inheriting from a base profile (e.g., user profile extends a project profile)?
— Should the glob match preview in the inspector support filtering/searching matched files?
— Should there be a "dry-run" mode that validates overrides without executing the pipeline?

---

— Feed the kiln. —
RUSTPUNK · CLINKER KILN · v0.1.0 · 2026
