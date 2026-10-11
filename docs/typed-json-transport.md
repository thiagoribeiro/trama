# Typed JSON workflow transport

JSON bodies always use typed replacement, without a mode header or a legacy text
rendering path. Task requests, async requests, compensations, completion hooks and
CLI dry-run simulation share this behavior. URLs and headers remain text templates;
XML, HTML, form and plain-text bodies retain their appropriate escaping.

A JSON `Content-Type`, including vendor JSON media types, selects JSON rendering.
Without a content type, bodies beginning with `{` or `[` are also treated as JSON.
Scalar JSON bodies must declare a JSON content type. The source body must be valid
JSON before replacement; invalid JSON fails rather than falling back to text.

## Typed references

Given payload `{"count":3,"state":{"minimum":7}}`, this body:

```json
{"count":"{{payload.count}}","state":"{{payload.state}}"}
```

is sent as:

```json
{"count":3,"state":{"minimum":7}}
```

Entire `{{path}}` string values preserve numbers, booleans, null, arrays and objects.
Dot and bracket list indexes are supported, including
`nodes.join.response.body.branches.0.result.output.price`. Partial text
interpolations accept scalars; collections must occupy an entire JSON value.
An explicitly present null becomes JSON null as a whole value and empty text
inside a longer string. An absent reference is an error, not an implicit null.

Missing keys, out-of-range indexes and traversal through null or scalars produce a
diagnostic containing the JSON field and full reference. Invalid task and
compensation bodies fail without retry or HTTP dispatch; task diagnostics also
identify the node. Invalid completion-hook bodies are not sent and record a
`callbackWarning`, preserving the workflow's existing terminal status.

Only placeholders present in the original body are evaluated. Referenced strings
containing template syntax remain literal. Sections, lambdas and triple braces are
rejected. An explicit `{"__trama_literal_json__": VALUE}` envelope preserves VALUE
without interpolation. This reserved form protects cached plans and future
placeholders; the receiving service sees VALUE, never the envelope.

## Compatibility and migration

This changes JSON template behavior: numeric and collection references retain their
JSON types rather than becoming strings or Java collection text. Initialize
counters and state explicitly in the first-run payload; do not reference a node
that has not executed yet to manufacture a null initial value. Carry the initialized
state into continuation payloads. Explicit null leaves remain valid; traversal
through them does not.

Rewrite JSON bodies relying on Mustache sections or raw substitution as valid JSON
with structured references. The former `X-Trama-Template-Mode` header is not
consulted and cannot select legacy rendering. Validate persisted workflows and
consumer generators before upgrading. Typed object/array workflows may not work
on an older engine; disabling generation alone does not rewrite persisted flows.

This change has no database migration and includes no production deployment.
Verification uses local unit, integration and end-to-end tests with controlled HTTP
servers and temporary PostgreSQL/Redis containers. It makes no LLM calls and does
not establish generation accuracy or latency.
