# Clinker Kiln Phase 1 → Phase 2b Codebase Snapshot

**Date:** 2026-03-29  
**Scope:** Complete source code inventory for Phase 2b planning  
**Directory:** `/home/glitch/code/rustpunk/clinker/.claude/worktrees/clinker-kiln-init/crates/clinker-kiln/`

---

## I. Workspace & Dependencies

### Workspace Root Cargo.toml
**Path:** `/home/glitch/code/rustpunk/clinker/Cargo.toml`

```toml
[workspace]
resolver = "2"
members = [
    "crates/clinker-record",
    "crates/cxl",
    "crates/cxl-cli",
    "crates/clinker-format",
    "crates/clinker-core",
    "crates/clinker",
    "crates/clinker-kiln",
]

[workspace.package]
version = "0.1.0"
edition = "2024"

[workspace.dependencies]
dioxus = { version = "=0.7.4", features = ["desktop"] }
cxl = { path = "crates/cxl" }
clinker-record = { path = "crates/clinker-record" }
clinker-format = { path = "crates/clinker-format" }
clinker-core = { path = "crates/clinker-core" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde-saphyr = "0.0.22" ← YAML parsing
chrono = { version = "0.4", features = ["serde"] }
miette = { version = "7", features = ["fancy"] }
clap = { version = "4", features = ["derive"] }
regex = "1"
ahash = "0.8"
indexmap = "2"
tracing = "0.1"
static_assertions = "1"
```

### Clinker Kiln Cargo.toml
**Path:** `crates/clinker-kiln/Cargo.toml`

```toml
[package]
name = "clinker-kiln"
version.workspace = true
edition.workspace = true
description = "Clinker Kiln — IDE for authoring Clinker YAML pipeline configurations"

[dependencies]
dioxus.workspace = true
cxl.workspace = true
```

**Key:** Minimal deps — only Dioxus 0.7.4 and CXL crate. All parsing/validation currently static.

---

## II. AppState: Signal Architecture

**File:** `src/state.rs`

```rust
#[derive(Clone, Copy)]
pub struct AppState {
    /// Current layout preset driving panel widths.
    pub layout: Signal<LayoutPreset>,
    
    /// Whether the run-log drawer is expanded (220 px) or collapsed (28 px).
    pub run_log_expanded: Signal<bool>,
    
    /// ID of the currently selected pipeline stage, or `None` if nothing is
    /// selected. Drives canvas highlight, YAML sidebar line tinting, and
    /// inspector panel visibility.
    pub selected_stage: Signal<Option<&'static str>>,
    
    /// Inspector panel width in pixels; range 260–520, default 340.
    #[allow(dead_code)]
    pub inspector_width: Signal<f32>,
}

pub enum LayoutPreset {
    CanvasFocus,
    Hybrid,
    EditorFocus,
}
```

**Signal Pattern (AP-4 Compliant):**
- Each field is an **independent Signal handle**, not a `Signal<AppState>` 
- `Signal<T>` is `Copy` → `AppState` is cheap to pass around
- Provided via `use_context_provider()` in App root
- Consumed via `use_context::<AppState>()` in descendants
- **Benefit:** Writing to `layout` doesn't re-render components subscribed only to `run_log_expanded`

**Phase 2b Considerations:**
- `selected_stage` may change from `&'static str` to `String` or `u32` (stage ID type)
- Need to consider if `inspector_width` should be reactive (currently unused `#[allow(dead_code)]`)
- May add signals for YAML file dirty state, validation status, etc.

---

## III. Demo Data Structure

**File:** `src/demo.rs`

### DemoStage Struct
The **core representation** of a pipeline stage — replaced with real Pipeline/Stage types in Phase 2b:

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct DemoStage {
    /// Stable string ID used as the RSX iterator key (never index-based).
    pub id: &'static str,
    
    /// Human-readable node label (Chakra Petch heading).
    pub label: &'static str,
    
    pub step_type: StepType,  // Source, Filter, Map, Sink
    pub pass: Pass,           // Scan or Transform
    
    /// Secondary subtitle line (JetBrains Mono, iron colour).
    pub subtitle: &'static str,
    
    /// Canvas world-space X position (top-left of the node card).
    pub canvas_x: f32,
    /// Canvas world-space Y position.
    pub canvas_y: f32,
    
    /// CXL expression fields for this stage (empty for Source/Sink).
    pub expr_fields: Vec<DemoExprField>,
    
    /// First line of this stage's block in DEMO_YAML (1-indexed, inclusive).
    pub yaml_line_start: usize,
    /// Last line of this stage's block in DEMO_YAML (1-indexed, inclusive).
    pub yaml_line_end: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DemoExprField {
    pub label: &'static str,       // e.g., "expr", "full_name"
    pub expr: &'static str,        // Initial CXL expression text
}

pub enum StepType {
    Source,    // #43B3AE verdigris
    Filter,    // #E8A524 hazard
    Map,       // #C75B2A ember
    Sink,      // #B7410E oxide-red
}

pub enum Pass {
    Scan,      // P1 label
    Transform, // P2 label
}
```

### Port Geometry
```rust
pub const NODE_HEIGHT: f32 = 92.0;
pub const NODE_WIDTH: f32 = 160.0;

impl DemoStage {
    pub fn port_out(&self) -> (f32, f32) {
        (self.canvas_x + NODE_WIDTH, self.canvas_y + NODE_HEIGHT / 2.0)
    }
    pub fn port_in(&self) -> (f32, f32) {
        (self.canvas_x, self.canvas_y + NODE_HEIGHT / 2.0)
    }
}
```

### Demo Pipeline
```rust
pub fn demo_pipeline() -> Vec<DemoStage> {
    vec![
        DemoStage {
            id: "demo-source",
            label: "csv_reader",
            step_type: StepType::Source,
            pass: Pass::Scan,
            subtitle: "customers_*.csv",
            canvas_x: 60.0,
            canvas_y: 100.0,
            expr_fields: vec![],
            yaml_line_start: 5,
            yaml_line_end: 11,
        },
        // ... 3 more stages (filter, map, sink)
    ]
}
```

**Phase 2b Replacement:**
- Replace hardcoded `Vec<DemoStage>` with parsed `Pipeline` from YAML via `serde-saphyr`
- Canvas coordinates will need a **layout engine** (spring physics or tree layout)
- `expr_fields` becomes the actual field list from the parsed YAML

---

## IV. CXL Bridge: Validation Interface

**File:** `src/cxl_bridge.rs`

### Validation API
```rust
pub fn validate_expr(source: &str) -> CxlValidation {
    if source.trim().is_empty() {
        return CxlValidation { is_valid: true, errors: vec![] };
    }
    let result = Parser::parse(source);
    let errors: Vec<CxlDiagnostic> = result.errors.iter()
        .map(|e| CxlDiagnostic {
            start: e.span.start as usize,
            end: e.span.end as usize,
            message: e.message.clone(),
            severity: DiagSeverity::Error,
            how_to_fix: e.how_to_fix.clone(),
        })
        .collect();
    CxlValidation {
        is_valid: errors.is_empty(),
        errors,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CxlValidation {
    pub is_valid: bool,
    pub errors: Vec<CxlDiagnostic>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CxlDiagnostic {
    pub start: usize,               // Byte offset in expression
    pub end: usize,
    pub message: String,
    pub severity: DiagSeverity,
    pub how_to_fix: String,         // Actionable fix suggestion
}

pub enum DiagSeverity {
    Error,
    #[allow(dead_code)]
    Warning,
}
```

**Current Scope (Phase 1):**
- Parser only — `cxl::parser::Parser::parse()` is synchronous
- Fast enough for on-keystroke validation (< 1ms for < 200 char expressions)
- No debounce needed

**Phase 2b Scope:**
- Phase 2a: Parser only (no resolver/typechecker — need real schema)
- Phase 2b: Add `validate_with_schema()` using resolver + type checker
  - Requires access to schema context (current stage inputs, previous stage outputs)
  - May need a different error model (warnings, auto-fix suggestions)

---

## V. Component Hierarchy & RSX Layout

### App Root
**File:** `src/app.rs`

```
<div class="kiln-app">
  ├─ <TitleBar />
  ├─ <div class="kiln-main" data-layout="canvas-focus|hybrid|editor-focus">
  │  ├─ <CanvasPanel />
  │  ├─ <InspectorPanel stage_id="..."> (conditional on selected_stage)
  │  └─ <YamlSidebar />
  └─ <RunLogDrawer />
```

**Key Points:**
- `App` owns **all top-level signals** (unconditional, at top level — AP-3 compliant)
- Signals bundled into `AppState` struct via `use_context_provider()`
- `data-layout` attribute on `.kiln-main` drives CSS grid widths for three presets
- `InspectorPanel` **keyed on `stage_id`** → full remount on selection change
  - Prevents stale-state bugs when switching stages

---

## VI. Canvas Panel

**File:** `src/components/canvas/panel.rs`, `node.rs`, `connector.rs`

### Transform State
```rust
pub fn CanvasPanel() -> Element {
    let state = use_context::<AppState>();
    let stages = demo_pipeline();
    let connections: Vec<_> = stages.windows(2)
        .map(|w| (w[0].clone(), w[1].clone()))
        .collect();

    let mut pan_x = use_signal(|| 0.0_f32);
    let mut pan_y = use_signal(|| 0.0_f32);
    let mut zoom = use_signal(|| 1.0_f32);
    
    // Non-reactive drag state (hot path, no re-renders during drag)
    let drag = use_hook(|| Rc::new(RefCell::new(DragState::default())));
}
```

### Zoom Behavior
- **Range:** 25% (0.25) to 400% (4.0)
- **Line/Page delta:** Zoom factor 1.10 per tick (±10%)
- **Pixel delta:** 0.001 per pixel
- **Anchor:** Zoom locked to cursor position

### Pan Behavior
- **Primary button** or **auxiliary (middle) button** initiates drag
- Right-click reserved for future context menu (Phase 3)
- Non-reactive `DragState` in `Rc<RefCell<>>` to avoid signal writes during drag

### SVG Connector (Three-Layer Design)
```rust
// Layer 1: Glow (wide stroke, 10% opacity)
path { stroke_width: "5", stroke_opacity: "0.1" }

// Layer 2: Dashed core cable (8px dash, 4px gap, 70% opacity)
path { stroke_width: "2", stroke_dasharray: "8 4", stroke_opacity: "0.7" }

// Layer 3: Bright centre hairline (90% opacity, "hot-wire" effect)
path { stroke_width: "0.75", stroke_opacity: "0.9" }

// Plus open chevron arrowhead at target port
```

### Node Card Selection
- Click-to-select toggles `AppState.selected_stage`
- `onmousedown.stop_propagation()` prevents canvas pan
- Inline `left`/`top` positioning via `DemoStage.canvas_x/y`
- Accent color from `stage.step_type.accent_color()`
- Pass label + type badge rendered in header

---

## VII. Inspector Panel

**File:** `src/components/inspector/mod.rs`, `panel.rs`, `stage_header.rs`, `cxl_input.rs`, `cxl_diagnostics.rs`, `scoped_yaml.rs`

### Inspector Panel Layout
```
<div class="kiln-inspector">
  ├─ <StageHeader />            (accent border, badge, label, close button)
  ├─ <Configuration Section>     (if expr_fields not empty)
  │  ├─ Section header
  │  └─ <CxlInput /> × N         (one per expr_field)
  └─ <ScopedYaml />             (selected stage's YAML block with line numbers)
```

### Key Points
- **Keyed on `stage_id`** in parent → full remount on selection change
- **Mousedown propagation stopped** to prevent canvas pan interactions

### CxlInput Component
```rust
#[component]
pub fn CxlInput(label: &'static str, initial_value: &'static str) -> Element {
    let mut text = use_signal(|| initial_value.to_string());
    let mut validation = use_signal(|| validate_expr(initial_value));

    let on_input = move |e: FormEvent| {
        let new_text = e.value();
        let result = validate_expr(&new_text);  // Sync validation on keystroke
        text.set(new_text);
        validation.set(result);
    };

    // Renders input field + <CxlDiagnostics /> if validation errors exist
}
```

**Per-Field Signals:**
- Each `CxlInput` is keyed by `"{stage_id}-{label}"` 
- Safe because each lives in its own hook scope
- Validation runs synchronously on every keystroke (no debounce)

### CxlDiagnostics Component
- Renders error/warning list with message + optional "how-to-fix" hint
- Index-based keys (diagnostics are ephemeral, recalculated each keystroke)

### StageHeader Component
- Type badge with accent color/border
- Stage label
- Close button (sets `selected_stage` to `None`)

### ScopedYaml Component
```rust
pub fn ScopedYaml(stage: DemoStage) -> Element {
    let all_lines = tokenize(DEMO_YAML);
    let start = stage.yaml_line_start.saturating_sub(1);  // 0-indexed
    let end = stage.yaml_line_end.min(all_lines.len());
    let scoped_lines = &all_lines[start..end];

    // Renders gutter (absolute line numbers) + code with tokenized colors
}
```

**Line Number Sync:**
- Line numbers are **absolute** (e.g., lines 14–16), not relative
- User can cross-reference with full sidebar
- Uses same `tokenizer::tokenize()` as full sidebar

---

## VIII. YAML Sidebar

**File:** `src/components/yaml_sidebar/mod.rs`, `panel.rs`, `tokenizer.rs`

### YamlSidebar Component
```rust
pub fn YamlSidebar() -> Element {
    let state = use_context::<AppState>();
    let lines = tokenize(DEMO_YAML);

    // Compute selected line range from selected stage
    let stages = demo_pipeline();
    let selected_range = (state.selected_stage)()
        .and_then(|id| stages.iter()
            .find(|s| s.id == id)
            .map(|s| s.yaml_line_start..=s.yaml_line_end)
        );

    rsx! {
        <div class="kiln-yaml-sidebar">
          ├─ Section header with "PIPELINE YAML" title + filename
          └─ Scrollable code area:
             ├─ Gutter (line numbers, `data-selected` attribute)
             └─ Code column (tokenized lines with `data-token` attribute)
    }
}
```

**Selection Sync:**
- When a stage is selected, corresponding YAML lines get `data-selected="true"`
- Line range from `stage.yaml_line_start..=stage.yaml_line_end`
- CSS applies tinted background + accent left border to selected lines

### Tokenizer

**File:** `src/components/yaml_sidebar/tokenizer.rs`

**No external deps** — line-by-line pass covering common demo YAML patterns.

```rust
pub fn tokenize(yaml: &str) -> Vec<Vec<Token>> {
    yaml.lines().map(tokenize_line).collect()
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TokenKind {
    Comment,  // `#` lines
    Key,      // Mapping key before colon (verdigris)
    Value,    // Scalar value / sequence item (phosphor)
    Punct,    // `:`, `- `, quotes (iron)
    Indent,   // Whitespace-only indent
}

pub struct Token {
    pub kind: TokenKind,
    pub text: String,
}
```

**Smart Colon Detection:**
```rust
fn find_key_colon(s: &str) -> Option<usize> {
    // Ignores colons inside quoted strings (single/double)
    // Colon must be followed by whitespace or EOF to be a key colon
}
```

**Phase 2b Replacement:**
- Replace with incremental parsing driven by `serde-saphyr` + AST
- Enable edit support (not just static display)

---

## IX. Run Log Drawer

**File:** `src/components/run_log/mod.rs`, `drawer.rs`

### RunLogDrawer Component
```rust
pub fn RunLogDrawer() -> Element {
    let state = use_context::<AppState>();

    rsx! {
        <div class="kiln-run-log" data-expanded={if state.run_log_expanded() { "true" }}>
          ├─ Tab bar (height 28px)
          │  ├─ Status LED (green in demo, red for errors)
          │  ├─ "RUN LOG" label
          │  └─ Expand/collapse chevron (▲ / ▼)
          └─ Log content (height 220px when expanded)
             └─ Scrollable list of demo log lines
    }
}
```

**Log Line Format:**
```rust
struct LogLine {
    timestamp: &'static str,  // "MM:SS.mmm" format
    level: Level,             // Cmd, Info, Stat, Ok, Warn, Err
    message: &'static str,
}

enum Level {
    Cmd  // "  $"
    Info // "INF"
    Stat // "DAT"
    Ok   // " OK"
    Warn // "WRN"
    Err  // "ERR"
}
```

**Transition:**
- CSS `height` animation (300ms ease-out)
- Driven by `data-expanded` attribute

**Phase 1 vs Phase 5:**
- Phase 1: Static hardcoded demo log
- Phase 5: Real streaming output via `tokio::process::Command`

---

## X. Title Bar

**File:** `src/components/title_bar.rs`

```
[clinker|kiln] ── customer_etl.yaml ······· [Canvas|Hybrid|Editor] ● VALID
```

### Features
- **Native drag** via `window.drag()` on empty bar areas
- **Layout switcher** (three-button toggle group)
- **Validation LED** (green dot + "VALID" label)
- **Interactive elements** use `onmousedown.stop_propagation()` to prevent window drag

---

## XI. File Structure Summary

```
src/
├── main.rs                          (entry point, window config)
├── app.rs                           (root component, signal ownership)
├── state.rs                         (AppState struct, LayoutPreset enum)
├── demo.rs                          (DemoStage, DemoExprField, demo data)
├── cxl_bridge.rs                    (validation API wrapper)
└── components/
    ├── mod.rs
    ├── title_bar.rs                 (frameless title bar)
    ├── canvas/
    │   ├── mod.rs
    │   ├── panel.rs                 (infinite canvas, pan/zoom, connectors)
    │   ├── node.rs                  (stage card, selection)
    │   └── connector.rs             (SVG three-layer dashed cables)
    ├── inspector/
    │   ├── mod.rs
    │   ├── panel.rs                 (slide-in stage inspector)
    │   ├── stage_header.rs          (badge, label, close button)
    │   ├── cxl_input.rs             (expression field with real-time validation)
    │   ├── cxl_diagnostics.rs       (error/warning list)
    │   └── scoped_yaml.rs           (selected stage's YAML block)
    ├── yaml_sidebar/
    │   ├── mod.rs
    │   ├── panel.rs                 (full YAML with line-by-line sync)
    │   └── tokenizer.rs             (static syntax tokenizer, no deps)
    └── run_log/
        ├── mod.rs
        └── drawer.rs                (collapsible log output)
```

---

## XII. Phase 2b Key Transition Points

### 1. **Signal: `selected_stage`**
- **Current:** `Signal<Option<&'static str>>`
- **Phase 2b:** Likely `Signal<Option<StageId>>` or `Signal<Option<String>>`
- **Propagation:** Canvas node, inspector, YAML sidebar all key off this
- **Remount:** InspectorPanel keyed on stage ID → safe cross-component sync

### 2. **Data: Replace Demo with Parsed Pipeline**
- **Current:** `demo_pipeline() → Vec<DemoStage>`
- **Phase 2b:** Parse YAML via `serde-saphyr` → real `Pipeline` struct
- **Components affected:** Canvas (needs stage list), Inspector (stage fields), YAML sidebar (entire doc)
- **Layout:** Canvas positions hardcoded in demo; Phase 2b needs **layout engine** (spring physics or tree layout)

### 3. **CXL Validation: Add Schema Context**
- **Current:** Parser only, no schema awareness
- **Phase 2b:** Resolver + type checker with schema context
- **Impact:** `validate_expr()` signature may change to accept stage context/input schema
- **File:** `cxl_bridge.rs` will grow significantly

### 4. **YAML Sidebar: From Display to Editor**
- **Current:** Static tokenizer, read-only display with line tinting
- **Phase 2b:** Edit support via `serde-saphyr` incremental parsing
- **Components affected:** Tokenizer (needs AST), panel (needs edit handlers)

### 5. **Inspector: Dynamic Field Generation**
- **Current:** `stage.expr_fields: Vec<DemoExprField>` hardcoded per stage
- **Phase 2b:** Expression fields parsed from stage YAML + type info from schema
- **Impact:** `CxlInput` component reuse for dynamic field lists

### 6. **Run Log: Live Streaming**
- **Current:** Static hardcoded demo log
- **Phase 5:** Real output via `tokio::process::Command`
- **For Phase 2b:** Placeholder complete, no changes needed yet

---

## XIII. Known Dead Code & Phase 3+ Placeholders

### Dead Code
- `AppState.inspector_width` — marked `#[allow(dead_code)]`
- `DiagSeverity::Warning` — marked `#[allow(dead_code)]`
- `Level::Warn`, `Level::Err` in run log — Phase 5 feature

### Phase 3+ Stubs
- Double-click canvas to "fit to view" — currently resets to origin
- Right-click context menu — reserved, not implemented
- Keyboard shortcuts — not yet implemented
- Undo/redo — not yet implemented

---

## XIV. CSS Architecture Notes

- **File:** `/assets/kiln.css` (loaded via `asset!()` macro)
- **Attribute-driven styling:** `data-layout`, `data-token`, `data-selected`, `data-expanded`, `data-level`
- **No Tailwind/utility classes** — semantic selectors (`.kiln-*`)
- **Three layout presets:** canvas-focus / hybrid / editor-focus (via CSS Grid/Flexbox width rules)
- **Syntax highlighting:** `data-token` attribute values (comment, key, value, punct, indent)

---

## XV. Dioxus Patterns & Anti-Patterns Observed

### ✅ Correct Patterns
1. **AP-3 Compliance:** All hooks unconditional at component top level
2. **AP-4 Compliance:** Struct of signals (AppState), not Signal of struct
3. **Keying Strategy:** 
   - Static `{stage.id}` keys for node list (stable identity)
   - InspectorPanel keyed on `stage_id` → full remount on change
   - Inline index keys for ephemeral diagnostics (acceptable)
4. **Signal Subscription:** Direct reads in RSX attributes (`.read()`, `()` invocation)
5. **Event Handler Closures:** Proper capture of signals via `move || { ... }`
6. **Propagation Control:** `stop_propagation()` used correctly for interactive elements

### Gotchas to Watch in Phase 2b
1. **Stale state on selection change:** InspectorPanel keying prevents this
2. **Signal writes during render:** Rc<RefCell<>> used for non-reactive drag state (correct)
3. **Derived state:** Might need memos for "selected stage object" vs "selected stage ID"

---

## XVI. Edge Cases & Behavior Notes

### Canvas
- **Zoom limits:** Enforced client-side, no server validation
- **Pan boundary:** No boundaries; canvas extends infinitely
- **Cursor sync:** Zoom anchored to cursor position (preserved across zoom)
- **Drag escape:** If pointer leaves panel, drag is cancelled

### Inspector
- **Empty expr_fields:** Configuration section hidden (e.g., Source/Sink stages)
- **Input validation:** On-keystroke, no debounce, fast enough (<1ms)
- **Scoped YAML:** Line numbers absolute, cross-referenceable with full sidebar

### YAML Sidebar
- **Selection sync:** Computed on-render from `selected_stage` signal
- **Line tinting:** CSS `:data-selected` attribute drives styling
- **Colon detection:** Smart (ignores colons in quotes), handles `key: value` and `key:\n` forms
- **Empty line handling:** NBSPs (`\u{00A0}`) rendered for empty lines to maintain spacing

### Run Log
- **Collapsed height:** 28px (tab bar only)
- **Expanded height:** 220px (tab + scrollable content)
- **Transition:** CSS height animation, `data-expanded` attribute driven
- **Toggle:** Click tab bar to expand/collapse

---

## Summary for Phase 2b

**Stability Points:**
- AppState signal architecture is solid and well-keyed
- Component hierarchy and remounting strategy is safe
- CXL validation bridge is clean and ready for schema extension

**Transition Points:**
1. Demo data → Parsed YAML (serde-saphyr)
2. Canvas layout (hardcoded → spring physics/tree layout)
3. Inspector fields (hardcoded → dynamic from parsed stage)
4. CXL validation (parser → parser + resolver + type checker)
5. YAML sidebar (static tokenizer → AST-based editor)

**Zero Changes Needed:**
- Title bar, run log, signal architecture
- Component keying and remounting strategy
- Dioxus patterns (all correct for 0.7)
