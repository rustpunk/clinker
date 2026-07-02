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
      channel.cfg.yaml         # labels + channel-wide overrides
      order_fulfillment.channel.yaml   # per-target overlay
    initech/                   # a standard-tier tenant
      channel.cfg.yaml
      order_fulfillment.channel.yaml
  output/                      # run output lands here (git-ignored)
```

## Layers

Overrides resolve in a fixed precedence order, clobbering (replacing, never
deep-merging) between layers:

```
pipeline-default  <  group(s) by priority  <  channel-wide  <  channel-per-target
```

Each layer carries two surfaces: a value clobber (`config:` / `vars:`) and an
ordered structural op list (`overrides:`).

## What this example demonstrates

- **A group that injects a composition.** `group/enterprise.group.yaml` splices
  the `fraud_check` composition into the plan via `overrides: [{op: add,
  composition: ...}]`, and raises `risk.threshold` to `0.8`. Its `match:`
  selector (`tier == "enterprise"`) auto-applies it to any channel labelled
  `tier: enterprise`. `channels resolve` lists the injected node; `channels
  lint` compiles it.
- **A channel that overrides a field and patches a schema.**
  `channel/globex/order_fulfillment.channel.yaml` raises `risk.threshold` to
  `0.95` (the highest layer wins) and declares an extra `channel_note` source
  column via a `patch_schema` op.

## Commands

Run these from this directory (the workspace root), passing `--base-dir .` so
the channel/group roots and relative paths resolve here.

```sh
# Preview the effective plan for a tenant: which group was derived from the
# channel's labels, which node it injected, and the winning value per field.
clinker channels resolve pipeline/order_fulfillment.yaml --channel globex --base-dir .

# Preview a group standalone (no channel).
clinker channels resolve pipeline/order_fulfillment.yaml --group enterprise --base-dir .

# Run the base pipeline.
clinker run pipeline/order_fulfillment.yaml --base-dir .

# Run with the enterprise group applied (injects fraud_check, raises threshold).
clinker run pipeline/order_fulfillment.yaml --group enterprise --base-dir .

# Trace one field's provenance across the overlay layers.
clinker explain pipeline/order_fulfillment.yaml --field risk.threshold --group enterprise --base-dir .

# List the channels a group's selector matches.
clinker channels group members enterprise --base-dir .

# Compile every channel/group overlay in the workspace and report failures.
clinker channels lint --base-dir .
```
