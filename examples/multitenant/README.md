# Multi-tenant channel/group overlay example

A runnable workspace showing how one base pipeline is customized per tenant
through the channel/group overlay system — without copying or editing the base
YAML.

## Layout

```
multitenant/
  clinker.toml                 # declares the channel/ and group/ roots
  pipeline/
    order_fulfillment.yaml     # base pipeline (the pipeline-default layer)
    orders.csv                 # sample input, colocated with the pipeline
  composition/
    risk_score.comp.yaml       # used by the base pipeline as node `risk`
    fraud_check.comp.yaml      # injected by the enterprise group
  group/
    enterprise.group.yaml      # selector-derived group; injects fraud_check
  channel/
    globex/                    # an enterprise tenant
      channel.cfg.yaml         # labels + a channel-wide `fixed:` lock
      order_fulfillment.channel.yaml   # per-target overlay (+ patch_schema)
    initech/                   # a standard-tier tenant
      channel.cfg.yaml
      order_fulfillment.channel.yaml   # per-target overlay (+ bypass)
  output/                      # run output lands here (git-ignored)
```

## Layers

Overrides resolve in a fixed precedence order, clobbering (replacing, never
deep-merging) between layers:

```
pipeline-default  <  group(s) by priority  <  channel-wide  <  channel-per-target
```

Each layer carries two surfaces: a value clobber (`config:` / `vars:` /
`fixed:`) and an ordered structural op list (`overrides:`).

## What this example demonstrates

- **A group that injects a composition.** `group/enterprise.group.yaml` splices
  the `fraud_check` composition into the plan via `overrides: [{op: add,
  composition: ...}]`, and raises `risk.threshold` to `0.8`. Its `match:`
  selector (`tier == "enterprise"`) auto-applies it to any channel labelled
  `tier: enterprise`. `channels resolve` lists the injected node; `channels
  lint` compiles it.
- **Config knobs that change output, not just provenance.** Both compositions
  read their knobs in CXL (`$config.threshold`, `$config.sensitivity`), and the
  planner constant-folds the resolved per-tenant value into the compiled body.
  With `risk.threshold` at 0.5 the base pipeline marks orders `high_value` from
  1000 upward (`threshold * 2000`); each overlay layer moves that cut, so every
  variant below writes a different `high_value` column.
- **A `fixed:` lock that beats a higher layer.**
  `channel/globex/channel.cfg.yaml` locks `risk.threshold` at `0.9`
  channel-wide, so the per-target overlay's `0.95` — normally the winning
  layer — is ignored. `channels resolve --channel globex` marks the value
  `(fixed)`, and `explain --field risk.threshold --channel globex` shows the
  `0.95` as shadowed. The lock shows up in the data too: order `ord-1006`
  (1850 + 25 fee = 1875) clears the 0.9 cut of 1800 but would miss the 0.95
  cut of 1900.
- **A `patch_schema` op.** globex's per-target overlay re-shapes the `amount`
  source column with a `modify` leaf (`amount: { scale: 2 }`). Trace which
  layer set the attribute with
  `explain --field orders.amount.scale --channel globex`.
- **A `bypass` op.** `channel/initech/order_fulfillment.channel.yaml` drops the
  `normalize` stage: `bypass` removes a 1-in/1-out node and auto-rewires its
  sole consumer (`handling_fee`) onto its sole upstream (`orders`). A
  fan-in/fan-out node needs the explicit `remove` op with a `rewire:` map, and
  swapping a node's definition wholesale is `replace`. The bypass is visible in
  the effective DAG of `channels resolve --channel initech` and in the output:
  initech keeps the raw lowercase order ids that `normalize` would uppercase.
- **Selector-derived membership.** Because `globex` is labelled `tier:
  enterprise`, the `enterprise` group applies to it automatically — `run
  --channel globex` splices in `fraud_check`, which zeroes the suspiciously
  large order `ord-1004`. `initech` (`tier: standard`) runs without it.

## Commands

Run these from this directory (the workspace root), passing `--base-dir .` so
the channel/group roots and relative paths resolve here.

```sh
# Preview the effective plan for a tenant: which group was derived from the
# channel's labels, which node it injected, and the winning value per field —
# globex shows `risk.threshold = 0.9 [ChannelWide] (fixed)`.
clinker channels resolve pipeline/order_fulfillment.yaml --channel globex --base-dir .

# The same for initech: the effective DAG shows `handling_fee` reading
# `orders` directly — the bypassed `normalize` node is gone.
clinker channels resolve pipeline/order_fulfillment.yaml --channel initech --base-dir .

# Preview a group standalone (no channel).
clinker channels resolve pipeline/order_fulfillment.yaml --group enterprise --base-dir .

# Run the base pipeline, then each variant; diff output/fulfilled.csv between
# runs — all four write different rows.
clinker run pipeline/order_fulfillment.yaml --base-dir .
clinker run pipeline/order_fulfillment.yaml --channel globex --base-dir .
clinker run pipeline/order_fulfillment.yaml --channel initech --base-dir .
clinker run pipeline/order_fulfillment.yaml --group enterprise --base-dir .

# Trace one field's provenance across the overlay layers. For globex the
# per-target 0.95 is listed as shadowed by the channel-wide `fixed:` 0.9.
clinker explain pipeline/order_fulfillment.yaml --field risk.threshold --channel globex --base-dir .

# Trace a patched schema attribute to the layer that set it.
clinker explain pipeline/order_fulfillment.yaml --field orders.amount.scale --channel globex --base-dir .

# List the channels a group's selector matches.
clinker channels group members enterprise --base-dir .

# Compile every channel/group overlay in the workspace and report failures.
clinker channels lint --base-dir .
```
