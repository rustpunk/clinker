# Fixed-Width Format

Fixed-width files carry no delimiters — each field occupies a fixed column
range on every line, the layout common to mainframe extracts and legacy
COBOL exports. Because the byte layout is not self-describing, a
fixed-width source needs **two** schemas: the `schema:` on the source body
declares CXL types for compile-time checking, and a separate format schema
(`.schema.yaml`) defines the physical layout — each field's start, width,
and padding. See [Source Nodes](../nodes/source.md) for the shared
transport rules.

```yaml
- type: source
  name: legacy_data
  config:
    name: legacy_data
    type: fixed_width
    path: "./data/mainframe.dat"
    schema:
      - { name: account_id, type: string }
      - { name: balance, type: float }
      - { name: status_code, type: string }
    options:
      line_separator: crlf    # line-ending style
```

## The format schema

The `.schema.yaml` file is the physical layout: it pins each field to a
column range and declares its padding so the reader can slice every line
identically. The source-body `schema:` and the format schema must name the
same fields — the former gives them CXL types, the latter gives them
positions.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `line_separator` | platform | Line-ending style (`lf` / `crlf`) used to split the file into records. |

## Schema drift

Fixed-width is **inert** with respect to
[auto-widen](../formats/auto-widen.md): because every byte is accounted
for by the format schema, there are no "unmapped" trailing columns to
absorb. The `on_unmapped` policy has no effect on a fixed-width source.
