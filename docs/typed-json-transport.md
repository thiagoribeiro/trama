# Opt-in typed JSON workflow transport

JSON task, async callback and compensation bodies can opt in with the HTTP header
`X-Trama-Template-Mode: typed-json-v1`. Unflagged requests preserve Mustache behavior.
The JSON body is parsed before replacement. Entire `{{path}}` strings are replaced
with the context's JSON value, preserving numbers, booleans, null, arrays and objects.
Missing whole references become null. Partial text interpolations accept scalars;
collections must occupy an entire JSON value. Dot and bracket list indexes are
supported, including `nodes.join.response.body.branches.0.result.output.price`.

Only placeholders present in the original body are evaluated. Referenced strings
containing template syntax remain literal. Sections, lambdas, triple braces,
non-JSON bodies and unknown transport versions are rejected in this mode. Rendering
errors return a nonretryable task failure before making an HTTP request. URLs and
headers retain their existing escaping and rendering behavior.

An explicit `{"__trama_literal_json__": VALUE}` transport envelope preserves VALUE
without interpolation. This reserved form protects cached semantic plans, original
intent and constraints inside continuation requests. The receiving service sees
VALUE, never the transport envelope. Runtime continuation state stays dynamic.

This is a coordinated RiteSmith/Trama candidate, not a production release. The
RiteSmith generation switch is disabled by default. The legacy deployed Trama
does not support the header: enabling RiteSmith alone would be incorrect. Review
and deploy the engine support separately before generating flagged artifacts.
If flagged workflows are active, stop them before rolling the engine back;
disabling generation alone does not rewrite already persisted workflows.

Offline verification includes typed values, escaping, injection resistance, join
indexing, HTTP dispatch and compensation, JSONLogic conditions and existing
workflow behavior tests. There are no provider calls or external effects in these
tests. No production database migration is required.
