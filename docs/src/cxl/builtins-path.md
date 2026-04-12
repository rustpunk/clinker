# Path Methods

CXL provides 5 built-in methods for extracting components from file path strings. All path methods take a string receiver and return a string. They return `null` when the receiver is `null` or when the requested component does not exist.

## file_name() -> String

Returns the full filename (with extension) from the path.

```bash
$ cxl eval -e 'emit result = "/data/reports/sales.csv".file_name()'
```

```json
{
  "result": "sales.csv"
}
```

## file_stem() -> String

Returns the filename without the extension.

```bash
$ cxl eval -e 'emit result = "/data/reports/sales.csv".file_stem()'
```

```json
{
  "result": "sales"
}
```

## extension() -> String

Returns the file extension (without the leading dot).

```bash
$ cxl eval -e 'emit result = "/data/reports/sales.csv".extension()'
```

```json
{
  "result": "csv"
}
```

Returns `null` when no extension is present:

```bash
$ cxl eval -e 'emit result = "/data/reports/README".extension()'
```

```json
{
  "result": null
}
```

## parent() -> String

Returns the parent directory path.

```bash
$ cxl eval -e 'emit result = "/data/reports/sales.csv".parent()'
```

```json
{
  "result": "/data/reports"
}
```

## parent_name() -> String

Returns just the name of the parent directory (not the full path).

```bash
$ cxl eval -e 'emit result = "/data/reports/sales.csv".parent_name()'
```

```json
{
  "result": "reports"
}
```

## Practical examples

**Organize output by source directory:**

```
emit source_dir = $pipeline.source_file.parent_name()
emit source_type = $pipeline.source_file.extension()
```

**Extract file identifiers:**

```
emit file_id = $pipeline.source_file.file_stem()
emit is_csv = $pipeline.source_file.extension() == "csv"
```

**Route by file type:**

```
let ext = input_path.extension()
emit format = match ext {
  "csv"  => "delimited",
  "json" => "structured",
  "xml"  => "markup",
  _      => "unknown"
}
```
