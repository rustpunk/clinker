# Field Paths

A column name is not opaque. An unescaped `.` inside it separates **path
segments**, so the column `Address.City` addresses the path `Address` → `City`.
Readers produce such names when they flatten nested input, writers expand them
back into nested output, and the rule for reading them is the same everywhere in
Clinker — one grammar, not one per format.

This page is the reference for that grammar. The places it applies:

- Column names declared in a source or output `schema:`.
- Column names a self-describing reader infers (the JSON reader flattens
  `{"a":{"b":1}}` to the column `a.b`; the XML reader flattens
  `<a><b>1</b></a>` the same way).
- Column names a writer expands back into nesting — see
  [Writing JSON](../formats/json.md#writing-json) and
  [Writing XML](../formats/xml.md#writing-xml).

It does **not** cover CXL expressions. Reaching into a *value* — a map or array
held in one column — uses the expression forms on
[Nested Paths](nested-paths.md) (`profile.city`, `profile["a.b"]`, `items[0]`).
The two are different surfaces over the same idea: a path is an ordered list of
segments either way, but a column name spells it with `.` and a backslash
escape, while an expression spells it with dots and brackets.

## The rule

Reading a name left to right:

| Input | Meaning |
|-------|---------|
| `.` | Ends the current segment, starts the next. |
| `\.` | A literal `.` inside the current segment. |
| `\\` | A literal `\`. |
| `\[` | A literal `[`. |
| Anything else | A literal character of the current segment. |

So `Address.City` is two segments, and `a\.b` is one segment named `a.b`.

Three consequences worth stating outright:

- **A lone `\` is an error, never a literal.** `C:\temp` is rejected, because
  silently treating `\t` as the two characters `\` and `t` would make the
  encoding ambiguous, and silently *dropping* the `\` would rename the column
  without saying so. Write `C:\\temp`.
- **Empty segments are real.** `a..b` is three segments — `a`, the empty name,
  and `b`. An empty key is a value a document can genuinely carry, so the
  grammar does not reject it.
- **A name may nest at most 64 levels deep.** This matches the depth at which
  the JSON reader stops flattening, so any name that reader produces is a name a
  writer can expand. The XML reader flattens with no depth bound, so an
  extraordinarily deep XML document can produce a name past the cap; writing it
  back out fails with a clear error rather than recursing without limit.

### Writing a literal bracket

`[` is currently a literal character, so a column named `a[0]` works. It is
nonetheless **reserved**: bracket indexing may later be given meaning inside a
flat name, matching the `[n]` form CXL expressions already use. Writing `\[`
means "a literal `[`" today and will keep meaning exactly that. A bare `[` is
not guaranteed to.

If a column name of yours contains `[`, escaping it now costs nothing and makes
it future-proof.

## Expansion on write

A writer that can express nesting rebuilds it from the decoded paths.

**Grouping.** Columns sharing a prefix collect into one container, positioned
where that prefix first appeared — even when the schema interleaves them.
Columns `Address.City`, `name`, `Address.State` write as:

```json
{"Address":{"City":"Boston","State":"MA"},"name":"Ada"}
```

**Absent children.** A column omitted for a record (a null under
`preserve_nulls: false`) contributes nothing, and a container whose every
descendant is omitted emits no key at all rather than an empty object. That
keeps the round trip honest: reading `{"a":{}}` produces no column, so writing
no column must produce no `"a"`.

**Values are untouched.** Expansion adds structure *above* a column's value; a
`Value::Map` or array held *in* that column still serializes as itself. A column
`Items.Item` holding `[1,2]` writes as `{"Items":{"Item":[1,2]}}`.

**No carve-outs.** The rule reads the column-name string and nothing else, so it
applies identically to engine-stamped columns. Under
`include_correlation_keys: true` the column `$ck.customer_id` writes as
`{"$ck":{"customer_id":…}}`.

## When two names clash

Two columns can describe places that cannot both exist. The writer refuses the
whole column set before writing a single byte, naming both columns — it never
silently keeps one.

| Columns | Why |
|---------|-----|
| `a` and `a.b` | `a` holds a value and is also the container `a.b` sits inside. |
| `a.b` and `a.b.c` | The same clash, one level down. |
| `a[b` and `a\[b` | Two spellings of the identical path. |

`a.b` and `a\.b` do **not** clash: they address `a` → `b` and the single
segment `a.b`. That is what the escape is for.

When a clash comes from a `.` you meant literally, the diagnostic offers the
escaped spelling:

```
XML writer cannot expand this output's column names into nested output: field
names `a.b` and `a.b.c` cannot both be written: `a.b` holds a value and is also
the container `a.b.c` nests inside. Rename one of them, or — if the `.` in `a.b`
is part of the name rather than a nesting separator — declare it as `a\.b`.
```

## Where the grammar does not reach yet

Two surfaces read column names without this grammar. Both are tracked, and both
are stated here so the current behavior is a known position rather than a
surprise:

- **Flat writers emit the raw name.** CSV and fixed-width have no nesting to
  expand into, so they write the column name verbatim — a column declared
  `a\.b` appears as the literal CSV header `a\.b`, backslash included. Whether
  flat writers should emit the *decoded* name instead is a separate decision.
- **Readers join without escaping.** The JSON and XML readers join flattened
  path segments with a plain `.`, so a source key that literally contains a `.`
  (`{"a.b": 1}`) arrives as the column `a.b` — indistinguishable from a nested
  `{"a":{"b":1}}`, and it writes back nested. Closing that means escaping each
  key as the reader joins it, tracked by
  [issue 920](https://github.com/rustpunk/clinker/issues/920).

`set` and `unset` in CXL also use a path grammar of their own that does not yet
support escaping — see the known limitation on
[Map Methods](builtins-map.md#setkey-string-value-any---map).

## See also

- [Nested Paths](nested-paths.md) — the expression-side forms for reaching into
  a value.
- [Writing JSON](../formats/json.md#writing-json) — expansion on the JSON write
  side.
- [Writing XML](../formats/xml.md#writing-xml) — expansion on the XML write
  side, plus the attribute convention layered over it.
