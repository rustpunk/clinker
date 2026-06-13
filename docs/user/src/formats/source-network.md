# Network Sources (REST)

A Source reads from the filesystem by default. To pull records from a
network endpoint instead, declare a `transport:` block on the Source. The
transport selects **where** records come from; it sits above the on-disk
`type:` (the format), which for a REST source still selects how the
response bodies decode.

A network transport is a **finite-pull** source: it runs on its own
thread, drives a synchronous client to cursor exhaustion, then exits.
There is no daemon, no event loop, and no async runtime — the same single-
process, run-to-drain model as a file pipeline. Finiteness is a hard
property of the reader: a REST source caps its pull with an explicit
page/record limit, so an unbounded endpoint cannot keep it running
forever.

A network source still **requires** a `schema:` block. That authored
schema is the row-to-record target: the reader maps each decoded object
onto it, coercing values leniently. A per-row value that cannot coerce is
left unchanged at the reader and routed to the dead-letter queue at the
Transform stage — identical to file-source semantics. A network source
declares **no** file matcher (`path` / `glob` / `regex` / `paths`);
declaring one is a configuration error (`E219`).

Because a network source has no file path, its `$source.file` provenance
column and the `{source_file}` output template both resolve to a stable
synthetic identifier, `<source:NAME>`, where `NAME` is the Source node's
name.

## REST sources

A `rest` source issues paginated HTTP `GET`s against a base URL, decoding
each response body through the declared `json` or `xml` format. (Other
formats are rejected with `E220` — a REST body is a multi-record
document, not a flat CSV/fixed-width stream.)

```yaml
nodes:
  - type: source
    name: orders_api
    config:
      name: orders_api
      type: json
      options:
        format: array        # each page body is a JSON array of objects
      transport:
        kind: rest
        url: https://api.example.com/v1/orders
        max_pages: 50         # HARD page cap — required
        pagination:
          strategy: link_header
        auth:
          scheme: bearer
          token: "${ORDERS_TOKEN}"
      schema:
        - { name: order_id, type: int }
        - { name: total,    type: float }
        - { name: placed_at, type: date_time }
```

### Pagination strategies

The `pagination.strategy` selects how the reader advances pages and
detects the last one. Whatever the strategy, the pull always stops at the
`max_pages` / `max_records` cap, even when the server keeps offering more.

- **`none`** (default) — a single `GET`; the body is the whole result.
- **`offset`** — `?offset=N&limit=L`, advancing the offset by the page
  size each request. The last page is the one that returns fewer rows
  than `limit`.

  ```yaml
  pagination:
    strategy: offset
    limit: 200
    offset_param: offset     # optional, defaults shown
    limit_param: limit
  ```

- **`cursor_token`** — the reader reads a continuation token from a JSON
  pointer in each response and sends it back on the next request. Paging
  stops when the token field is absent or null.

  ```yaml
  pagination:
    strategy: cursor_token
    cursor_param: page_token
    next_token_pointer: /meta/next_page   # RFC 6901 JSON pointer
  ```

- **`link_header`** — the reader follows the URL in the response's RFC
  5988 `Link: <…>; rel="next"` header until no such link is present.

  ```yaml
  pagination:
    strategy: link_header
  ```

### Authentication

`auth.scheme` selects the credential sent on every request:

- **`none`** (default) — no auth header.
- **`bearer`** — sends `Authorization: Bearer <token>`.
- **`header`** — sends an arbitrary static header, e.g. an API key.

  ```yaml
  auth:
    scheme: header
    name: X-API-Key
    value: "${API_KEY}"
  ```

### Reliability and finiteness knobs

| Key            | Default | Meaning                                                              |
|----------------|---------|----------------------------------------------------------------------|
| `max_pages`    | —       | **Required.** Hard ceiling on pages fetched, regardless of the server. |
| `max_records`  | none    | Optional hard ceiling on records emitted.                            |
| `retries`      | `3`     | Bounded retries on a transient failure (5xx, connect/timeout error). A 4xx is fatal — retrying cannot help. |
| `timeout_secs` | `30`    | Per-request timeout. Bounds in-flight time so an interrupt lands within the shutdown window. |

A partial-page decode failure routes that page's offending rows to the
DLQ per-row, exactly like a file source; it does not abort the pull.

### Shutdown

On `SIGINT`/`SIGTERM` the reader polls its cancellation handle at each
page boundary and stops cleanly with a normal end-of-input — the same
graceful drain a file source performs. The `timeout_secs` per-request
bound caps how long a single in-flight request can delay that stop.
