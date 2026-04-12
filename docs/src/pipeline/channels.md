# Channels

Channels enable multi-tenant pipeline customization. A single pipeline definition can be run with different configurations per client, environment, or business unit -- without duplicating or modifying the base YAML.

## Channel structure

Each channel lives in its own directory under a `channels/` folder, with a `channel.yaml` manifest:

```
project/
  pipeline.yaml
  channels/
    acme-corp/
      channel.yaml
    globex/
      channel.yaml
```

## Channel manifest

```yaml
# channels/acme-corp/channel.yaml
_channel:
  id: acme-corp
  name: "Acme Corporation"
  description: "Custom config for Acme Corp"
  contact: "ops@acme.example.com"
  active: true
  tags: [enterprise, priority]
  tier: gold

variables:
  EXPRESS_SURCHARGE: "8.99"
  TAX_RATE: "0.095"
  OUTPUT_DIR: "/data/acme/output"
```

### Channel metadata fields

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique channel identifier (used in CLI and logs) |
| `name` | Yes | Human-readable display name |
| `description` | No | Channel purpose and notes |
| `contact` | No | Responsible team or person |
| `active` | No | Whether the channel is enabled (default: `true`) |
| `tags` | No | Arbitrary tags for filtering and grouping |
| `tier` | No | Service tier classification |

## Running with a channel

Use the `--channel` flag to run a pipeline with a specific channel's configuration:

```
clinker run pipeline.yaml --channel acme-corp
```

Clinker looks for the channel manifest in the workspace's `channels/` directory, loads the variable overrides, and applies them before pipeline execution begins.

## Variable overrides

The `variables:` section in a channel manifest provides values that override pipeline-level `vars:` defaults. This lets each tenant customize thresholds, paths, labels, and other parameters:

```yaml
# Pipeline defaults
pipeline:
  name: invoice_processing
  vars:
    late_fee: 25.00
    output_path: "./output/"

# Channel override
variables:
  LATE_FEE: "50.00"
  OUTPUT_PATH: "/data/acme/invoices/"
```

Channel variables support `${ENV_VAR}` syntax for referencing system environment variables -- these are resolved at channel load time.

## Workspace discovery

Channels are part of the broader workspace system. Clinker discovers workspaces via `clinker.toml` files, which can define the channel directory layout and other workspace-level settings.

## Current status

> **Note:** The channel system is being rebuilt in Phase 16c. The current implementation supports variable overrides. Full channel documentation -- including path overrides, schema overrides, and channel inheritance -- will be expanded when the rebuild is complete.

## Complete example

```yaml
# channels/globex/channel.yaml
_channel:
  id: globex
  name: "Globex Industries"
  active: true
  tier: standard

variables:
  COMMISSION_RATE: "0.15"
  MIN_ORDER: "250"
  REPORT_LABEL: "Globex Monthly"
```

```
# Run the pipeline with Globex's configuration
clinker run sales_pipeline.yaml --channel globex
```
