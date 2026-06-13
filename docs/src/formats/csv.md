# CSV Format

CSV is the default file format and the most common Clinker input. The
reader decodes each line into a record whose fields are matched
positionally (or by header name) against the source's declared
`schema:`; the writer reverses the process. CSV pairs with the `file`
transport — see [Source Nodes](../nodes/source.md) for the transport /
format split and the schema rules every source shares.

```yaml
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: "./data/orders.csv"
    schema:
      - { name: order_id, type: int }
      - { name: customer_id, type: int }
      - { name: amount, type: float }
      - { name: order_date, type: date }
    options:
      delimiter: ","         # default ","
      quote_char: "\""       # default "\""
      has_header: true        # default true
      encoding: "utf-8"      # default "utf-8"
```

## Options

All CSV options are optional. With no `options:` block, Clinker uses
standard [RFC 4180](https://www.rfc-editor.org/rfc/rfc4180) defaults.

| Option | Default | Description |
|--------|---------|-------------|
| `delimiter` | `,` | Field separator. Set to `\t` for TSV, `;` for semicolon-delimited exports. |
| `quote_char` | `"` | Quote character that escapes delimiters and newlines inside a field. |
| `has_header` | `true` | When `true`, the first line names the columns and is consumed, not emitted. When `false`, fields bind to `schema:` positionally. |
| `encoding` | `utf-8` | Text encoding used to decode the bytes before field splitting. |

## Header handling

With `has_header: true`, the header row's names bind input columns to the
`schema:` entries — column order in the file may differ from the schema.
With `has_header: false`, binding is strictly positional, so the schema
order **must** match the file's column order.

Input columns the schema does not name are governed by the source's
[`on_unmapped`](../formats/auto-widen.md) policy, the same as every other
format.
