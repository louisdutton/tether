# ACP application interface

Tether speaks stable **ACP v1** using the official Rust SDK, on both its
application-facing server and its shared client for external agent executables.
Draft v2 and unstable features are disabled.

## Transports and lifecycle

- **Stdio:** launch `tether --stdio`; each line is one ACP JSON-RPC message.
  Logs go to stderr. Closing stdin stops the execution server and owned agents.
- **WebSocket:** launch `tether`, then connect to `ws://127.0.0.1:8787/acp`.
  Each text frame is one JSON-RPC message. This is a documented custom ACP
  transport, not the proposed ACP HTTP transport. Binary messages are rejected.
- **Health:** `GET /healthz` returns `200` while any configured agent is ready,
  otherwise `503`.

Incoming application messages are limited to 64 KiB. Use one process per data
directory; independent stdio processes need separate directories. Multiple
clients of an existing server should use WebSocket.

Initialize first, then create or load a session on each connection before
prompting it. Authentication methods are empty. Deployment access control is
external; provider login happens through the installed agent/native runtime.

| Standard method | Behavior |
| --- | --- |
| `initialize` | Negotiate v1 and advertised capabilities |
| `session/new` | Create a managed conversation with the default available agent |
| `session/list` | Authoritative catalog across all configured agents, with optional cwd/cursor filtering |
| `session/load` | Attach and replay shared history before responding |
| `session/resume` | Attach without replaying conversation history |
| `session/delete` | Permanently delete all bound native conversations and histories, then Tether records; failures remain visible |
| `session/close` | Cancel work and release native sessions when all adapters advertise close and load |
| `session/set_mode` | Select an adapter-advertised mode; modes are returned on new/load/resume |
| `session/set_config_option` | Select the agent or change an advertised native setting |
| `session/prompt` | Forward ACP prompt blocks, stream updates, respond at completion |
| `session/cancel` | Notify the agent to cancel the session's active turn |
| `session/update` | Standard message, reasoning, plan, tool, command, config and metadata updates |

The default is Codex when configured and ready, otherwise the first available
agent by configured ID. New/load responses include the current `configOptions`.
Prompt and MCP capabilities are the intersection of configured agents' supported
capabilities. Unsupported content and transports fail explicitly. Stdio MCP
servers require absolute commands and unique, nonempty names; standard MCP
objects pass through to the ACP agent unchanged.

Additional workspace roots, system-prompt overrides and client-provided
filesystem/terminal services are not exposed. Interactive permission requests
receive ACP's cancelled outcome; there is no approval UI.

## Authoritative session catalog

`session/list` reads Tether's known catalog immediately, without querying native
agents or waiting for discovery. Sessions created through Tether are included
immediately, alongside previously discovered external conversations. Normal list
refreshes and reconnects never discover additional external sessions.

When `agentCapabilities._meta.tether.sessionDiscovery` is true, clients can request
explicit discovery with `session/list` and `_meta: {"tether":{"discover":true}}`.
This is a fresh-list operation: do not include a cursor. It queries each configured
agent, drains native pagination and reconciles known identities and metadata before
returning the first page of the unified catalog. Later pages use ordinary
`session/list` with the returned cursor. Known catalog reads on other connections
remain available while discovery runs. Discovery neither loads conversations nor
executes prompts. The Codex adapter uses native scan-and-repair for this explicit
operation, retaining discovery of conversations missing from its database index.

The catalog preserves valid provider `updatedAt` timestamps and sorts by newest
known activity, then stable public ID. Undated sessions follow dated sessions;
refresh, load and transcript replay do not create activity. Prompts and live turn
events also record activity. Changes made externally become visible when explicitly
discovered or reported through a loaded provider session. Public IDs persist across
refreshes and process restarts. Use them for load, resume, close, import and delete.

Deduplication uses actual `(configured agent ID, native session ID)` bindings.
All bindings of a managed cross-agent conversation resolve to its existing public
ID, including inactive bindings. Similar titles or directories are not evidence
of identity. Adoption reuses an existing catalog ID instead of duplicating it.

`_meta.tether.discoveryPerformed` distinguishes explicit discovery from known-catalog
reads. During discovery, `catalogComplete` indicates whether every agent refreshed successfully;
`catalogErrors` identifies unavailable/unsupported catalogs. Cached conversations
remain visible during failures. Opaque cursors refer to a persisted snapshot of
ordered public IDs, scoped to the cwd filter. Continued pages retain that ordering
across concurrent activity and restarts; newly created sessions appear on a fresh
list. Deleted and internally classified sessions are skipped. Snapshots expire
after 30 minutes and at most 128 are retained; invalid or expired cursors return
an explicit refresh-required error. Older ID-based cursors must also be refreshed.
The supplied Codex adapter includes active and archived conversations and every
model provider. It classifies Guardian approval-review subagents by native source
metadata (`subAgent.other == "guardian"`), allowing Tether to omit them from public
lists and reclassify existing cached records without deleting original data.
A user conversation titled “Guardian review” remains visible. Other subagent
sources are retained. This classification is not inferred from chat titles. Other
adapters must expose their complete native catalog to provide the same coverage. The former `_tether/providers/sessions` discovery route is
removed; clients use standard `session/list`.

## Standard configuration

`configOptions` contains a standard select option with ID `tether.agent`. Its
values are configured agent IDs. Managed conversations can switch to available
agents that support native loading. Direct conversations expose their current
agent until imported.

Agent-supplied selectors retain their names, categories, available values,
ordering and current values. Their opaque IDs receive an `agent/` prefix to
avoid collisions. For example, an upstream model selector with ID `model` becomes
`agent/model`. Applications must use the advertised IDs rather than assume
these example names are universal.

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"my-app","version":"0.1.0"}}}
```

```json
{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/absolute/project","mcpServers":[]}}
```

Using the returned session ID and advertised option values:

```json
{"jsonrpc":"2.0","id":3,"method":"session/set_config_option","params":{"sessionId":"TETHER_SESSION_ID","configId":"tether.agent","value":"claude"}}
```

```json
{"jsonrpc":"2.0","id":4,"method":"session/set_config_option","params":{"sessionId":"TETHER_SESSION_ID","configId":"agent/model","value":"ADVERTISED_MODEL_ID"}}
```

Each configuration response returns the full updated catalog. Selecting another
agent can replace that catalog. Explicit native settings are persisted and
restored in catalog order on native load. A model change can reset or remove
dependent options; Tether retains only selections the returned catalog still
confirms. Native setting changes during a turn follow the agent's behavior;
changing execution agents requires idle source/target conversations.

## Prompting, cancellation and history

```json
{"jsonrpc":"2.0","id":5,"method":"session/prompt","params":{"sessionId":"TETHER_SESSION_ID","prompt":[{"type":"text","text":"Review this repository"}]}}
```

Prompt blocks reach the agent as ACP blocks, including supported images,
embedded resources and resource links. Tether does not flatten links into text
or fetch them itself. Structured updates preserve reasoning, plans, tool inputs,
tool diffs and standard metadata. Message/tool IDs are scoped to the Tether turn
so native IDs cannot collide across agent bindings. Native user echoes are
replaced by the authored input blocks, keeping injected handover out of history.

The prompt response follows the terminal agent response and preserves its stop
reason and metadata. Errors return JSON-RPC errors. Concurrent prompts to one
session fail with `-32001`; there is no prompt queue or automatic retry.

```json
{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"TETHER_SESSION_ID"}}
```

Cancellation is a notification without an ID. The prompt remains pending until
the agent settles it. Observed cancellation takes precedence over a racing
successful completion.

Use `session/list` to browse known conversations, opt into discovery as described
above to find external conversations, and use `session/load` to replay their
history. There is no separate custom history/get endpoint. Load validates the
stored cwd and includes current config options. Managed history remains readable
when the agent is unavailable. Running work can be attached with unchanged MCP
configuration; changes require an idle session.

Persisted updates are deduplicated during replay and live attachment. History
and live output do not form an atomic snapshot. After connection failure or
buffer loss, reload to reconcile. WebSocket disconnects leave agent execution
running; another client can initialize, load and cancel it.

## Cross-agent continuation

Changing `tether.agent` preserves the Tether session ID and transcript. A new
agent gets bounded historical user/assistant text; a returning agent loads its
native conversation and receives text since it last left. Original attachments,
hidden reasoning, tool runtime state and background jobs do not migrate.

ACP has no receipt acknowledgement. Tether retires pending handover after a
successful non-cancelled prompt response. Errors/cancellation retain it as
ambiguous. A switch can remain committed when its first prompt fails. There is
no automatic retry, durable transport replay or idempotency key. Unsettled turns
from a previous execution-server lifetime are marked failed with unknown delivery.

## Tether extensions

Ordinary conversation control, history replay and agent/model selection require
only standard ACP. Remaining extensions manage native ownership or provide
operational diagnostics:

| Extension method | Parameters | Behavior |
| --- | --- | --- |
| `_tether/providers` | `{}` | Agent IDs, readiness and inferred adapter capabilities |
| `_tether/session/adopt` | `{provider,providerSessionId,kind?}` | Bind an existing native conversation; load it before prompting |
| `_tether/session/import` | `{sessionId}` | Convert direct ownership to managed history, retaining identity |

Direct ownership can be requested on new through `_meta.tether.kind: "direct"`.
Other initial provider/model metadata and prompt-level provider overrides are
removed; use standard config options instead.

Initialize advertises these extensions under `agentCapabilities._meta.tether`.
New/load/list responses include provider, ownership kind, machine ID and runtime
status, native bindings, deletionPending and deletionError under `_meta.tether`. Agent changes also use `session_info_update`.
Clients advertising `clientCapabilities._meta.tether` receive `_tether/event`
notifications for operational details such as provider unavailability and
`resync.required`. Standard session metadata/config updates require no opt-in.
Extension responses retain internal snake_case fields; they are not standard ACP
session objects.

Unknown methods return `-32601`, invalid parameters `-32602`, missing
sessions/turns `-32002`, busy sessions `-32001` permanent deletion failures `-32003`, and storage/agent failures
`-32603`. Native ACP errors retain their code, message and data, including prompt failures and authentication errors.

References: [ACP v1](https://agentclientprotocol.com/protocol/v1/overview),
[configuration](https://agentclientprotocol.com/protocol/v1/session-config-options),
[session loading](https://agentclientprotocol.com/protocol/v1/session-setup),
[extensions](https://agentclientprotocol.com/protocol/v1/extensibility).

## Session cleanup and capability scope

Delete requires an explicitly configured permanent-deletion operation for every
native binding. It removes the original provider conversation and history, verifies
its absence from the fresh native catalog, and only then removes Tether's bindings,
authored inputs and transcript. Archiving, hiding and deleting only local records
never count as successful deletion. An active conversation returns `-32001`;
cancel it first. Unknown/already-deleted public IDs succeed idempotently.

Unsupported deletion, offline providers, native failures and failed verification
return an error (`-32003` for deletion failures). The session remains in
`session/list` with `_meta.tether.deletionError`. Once destructive work starts,
`deletionPending` remains set until completion; load/resume/prompt cannot recreate
a partially deleted conversation. Its retained Tether history remains readable.
Successfully deleted bindings are durably marked, so retry after restart only
deletes the remaining originals. Tether's transcript is removed only when every
binding succeeds. Successful deletion invalidates all client attachments.

The packaged Codex adapter exposes `_tether/session/delete_permanently`, calling
the supported native app-server `thread/delete` operation on its own connection.
Stock codex-acp 1.13.1 maps `session/delete` to `thread/archive` and is insufficient.
Codex also provides `codex delete --force`, but that CLI can fail while an adapter
still owns the loaded thread; the packaged integration uses the same-server API.
No filesystem deletion or SQLite modification of Codex storage is used.

Close retains the conversation and history. It forwards `session/close` to every
native binding, waits for active prompts to settle, and invalidates attachments.
Load or resume to continue. Unknown/already-closed IDs succeed. Close is only
advertised when every configured adapter advertises native close and supports
load; Tether does not pretend to release native resources through a no-op.

Resume checks the stored cwd and MCP configuration just like load, but sends no
historical messages. It fails if execution is unavailable. Load can still replay
managed history offline. File paths in tool locations and diffs are absolute,
including older stored updates: relative adapter paths use the session cwd.

See [ACP v1 coverage](acp-conformance.md) for the audited method surface,
capability boundaries, tests and remaining optional features.

Native API reference: [Codex app-server](https://learn.chatgpt.com/docs/app-server).
