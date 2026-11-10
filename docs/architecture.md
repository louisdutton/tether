# Architecture

```text
application
    | ACP v1 (stdio or custom WebSocket)
ACP Rust SDK server
    |
Tether session manager ----- SQLite identities + managed history
    |
shared ACP Rust SDK client
    | ACP v1 stdio
configured agent executables
    | upstream adapters own native integration
Codex / Claude Code / other agents
```

## Standard protocol boundary

Use the official `agent-client-protocol` Rust SDK for both connections. The SDK
owns JSON-RPC framing, request IDs, schemas and connection lifetime. Stable v1
is enabled; draft v2 and unstable schema features are disabled.

Applications create or load a session, change advertised config options and
send prompts. Tether forwards prompts using ACP content blocks and records ACP
session updates without translating native provider events. Native message and
tool IDs are scoped to the Tether turn when forwarded. Standard responses retain
the agent's stop reason and metadata.

The `tether.agent` config option selects the executing agent. Native config IDs
are exposed as `agent/<native-id>` to avoid collisions. Categories, available
values, ordering and current values come from the agent. Explicit selections
are persisted and restored in catalog order after native load. Changed defaults
are not pinned; dependent selections invalidated by a model change are dropped.

Prompt and MCP capabilities advertised to applications are the intersection of
configured agents' capabilities. MCP objects pass through unchanged after
validation. Additional workspace roots, system-prompt overrides, client file and
terminal services and interactive approvals are not exposed in this build.
Permission requests receive ACP's cancelled outcome; unsupported agent requests
receive method-not-found.

Per-connection initialization and attachment state enforce initialize → new/load
→ prompt. Prompt handlers use connection-owned SDK tasks, leaving the dispatcher
available for cancellation. Disconnecting an application drops its delivery task;
the shared provider connection and execution continue.

## Durable conversations

A Tether session has an opaque UUID, working directory, current agent, ownership
kind and native session bindings. Native IDs never serve as public IDs. SQLite
stores ordered turns and their ACP updates, authored prompt blocks, outcomes and
agent-change boundaries. A binding records the native session ID and the shared
history boundary already represented in that conversation.

ACP does not define durable storage or cross-agent context migration. Those
remain Tether responsibilities. ACP also has no native turn IDs: Tether assigns
execution IDs when publishing `session/prompt`. Native load replays are not
re-imported into managed history, avoiding duplicate conversations after restart.
An unsettled turn from an earlier execution-server lifetime is marked failed
with unknown delivery; it is never retried automatically.

A private per-agent JSON record stores native session directories, MCP settings
and explicit config selections needed to load native conversations. Managed
transcripts live in SQLite. Direct sessions use ACP's native load replay as their
history; adoption/import retain the Tether identity. Standard session/list reads
only the known catalog and does not query native agents. Explicit discovery via
`_meta.tether.discover` refreshes configured agents, records unseen bindings without
loading them, and reuses existing binding owners. Discovery depends on
the agent advertising `session/list`; returning to a native conversation requires
`loadSession` support.

The manager subscribes independently of application connections. Command and
persistence operations are serialized so output cannot race authored input
storage. Updates are persisted before broadcast. Replay deduplicates by stored
update identity, including around attachment to running work. There is no atomic
history/live snapshot or durable network replay; buffer loss requires reloading.

## Cross-agent continuation

Changing `tether.agent` requires a managed session and idle source/target
bindings. The manager saves source history, creates or loads the target native
session and commits the active binding. An idle source can remain loaded inside
its ACP adapter; Tether sends subsequent prompts only to the active binding.

A new agent receives recent user/assistant text. A returning agent receives text
since it last left. The seed is bounded to 32,000 characters plus framing and is
sent as a separate text block before the current authored prompt. The original
prompt blocks are retained separately for shared history. Attachments, hidden
reasoning, tool runtime state and background jobs are not migrated.

ACP has no receipt acknowledgement. A successful non-cancelled prompt response
retires pending handover context; errors and cancellation retain it as ambiguous.
A later explicitly requested prompt can therefore repeat that context. A switch
can remain committed when its first prompt fails. Tether never automatically
retries ambiguous prompts or creates a replacement for a missing native session.

## Runtime and access

Each configured agent uses one long-lived ACP stdio connection. Non-prompt
requests have 30-second timeouts; prompt completion follows the agent's response.
Cancellation uses `session/cancel`. Process exit marks only that agent unavailable.
Shutdown stops owned agent process groups and closes the managed store. There is
no distributed ownership lock against unrelated clients of the native runtimes.

Tether binds to loopback by default and has no application authentication.
Access control belongs to the deployment's trusted network, tunnel or reverse
proxy. Provider authentication belongs to the configured agent and its native
runtime. The data directory is private; SQLite uses an exclusive connection lock.

## Permanent deletion

Deletion preflights every binding, persists a pending marker, and deletes native
originals before removing SQLite history. Each completed binding is saved so a
partial failure remains visible and retryable after restart. Pending conversations
cannot execute or import, preventing resurrection. A successful native operation
must also pass a fresh catalog absence check. Native record caches are removed
only after verification; the shared transcript is removed in one final transaction.

Providers opt into an explicit permanent ACP extension or a native command;
standard ACP session/delete is not trusted to guarantee erasure. The packaged
Codex integration patches its upstream adapter to use supported thread/delete,
and includes archived threads in discovery. Upstream patch anchors are checked
at build time. Unsupported providers return errors and retain their records.
