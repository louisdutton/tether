# ACP v1 coverage

Tether targets stable ACP v1, using `agent-client-protocol` 5.1 and its 1.16.0
schema on both boundaries. This audit covers the stable methods in that SDK and
the [current v1 documentation](https://agentclientprotocol.com/protocol/v1/overview).
Optional capabilities are negotiated; supporting v1 does not imply supporting
all optional methods or v2/unstable extensions.

| Surface | Tether behavior |
| --- | --- |
| initialize | Negotiates v1, reports implementation, capabilities and empty auth methods; required before session operations |
| authenticate | No methods advertised: provider login happens on the execution host; unsupported method IDs return invalid params |
| new / prompt / cancel | Implemented using SDK types, rich content, structured updates and standard stop reasons; cancellation settles the prompt |
| list / load | Implemented for Tether-owned IDs; cwd filtering, pagination, metadata and managed history replay; offline history remains readable |
| delete | Advertised; transactional Tether history removal, idempotent missing IDs, busy rejection, available offline; native histories remain separately owned |
| resume | Advertised; same cwd/MCP checks as load, attaches without historical replay, unavailable execution returns an error |
| close | Advertised only when all adapters support native close and load; cancels work, preserves history and requires reattachment |
| set_mode | Available when the active adapter returns modes; new/load/resume include its mode state and changes emit current_mode_update |
| set_config_option | Agent selector plus namespaced native catalogs; model/reasoning choices preserve adapter categories and values |
| message / thought / plan / tools / commands | Standard updates streamed and replayed; identifiers scoped across agent bindings; file locations/diff paths made absolute |
| permission requests | Standard cancelled outcome; no approval UI or silent approval |
| content / MCP transports | Capability intersection across configured adapters; standard MCP objects forwarded unchanged to the adapter |
| errors / extensions / transport | SDK JSON-RPC framing and errors; native error code/message/data preserved; standard stdio plus documented custom WebSocket framing |

## Optional features not advertised

Tether does not advertise additional workspace directories, system-prompt
injection, remote authentication/logout, client filesystem/terminal services,
elicitation, boolean configuration support, notices or compactions to adapters.
Adapters must not invoke unadvertised client services. This keeps capability
negotiation truthful rather than accepting unsupported parameters or granting
new execution policy. Configuration selectors, session modes and provider login
remain adapter-owned.

Unstable fork, archive/unarchive, title setters and status methods are not enabled.
Draft v2 and the proposed ACP HTTP transport are not implemented. Session deletion
is stable v1; it does not require an unstable feature flag.

## Validation

Run `devenv shell -- tether-check` for formatting, Clippy and Rust tests. Tests use
the official ACP Rust client and real deterministic adapter subprocesses, including
stdio and WebSocket connections. Lifecycle regression tests cover deletion across
restart, unknown IDs, active-session handling, resume without history replay,
close/cancel/reattach, capability gating, mode selection, native errors, and
absolute live/replayed tool paths. The existing suite covers rich content,
configuration persistence, agent switching and offline history.

The official [ACP Test Compatibility Kit](https://github.com/agentclientprotocol/acp-tck)
is an independent additional check, not a full or certified conformance guarantee.
Use its v1 suite with `--cancel-prompt slow` and `tests/fixtures/tck-agent` after
`cargo build`. Run it in the devenv shell so Bash and jq are on PATH, and pass a
Nix-provided interpreter to uv on NixOS. For example, with the TCK checked out at
`/tmp/acp-tck`:

```sh
mkdir -p /tmp/acp-tck-workspace
devenv shell -- uv run --python /absolute/path/to/python3 --project /tmp/acp-tck acp-tck \
  --agent-cwd /tmp/acp-tck-workspace --cancel-prompt slow \
  --report-json /tmp/tether-acp-report.json -- ./tests/fixtures/tck-agent
```

The fixture adapters exercise protocol behavior without credentials or model
calls. They record forwarded MCP configuration but do not launch MCP servers;
therefore the TCK's advisory MCP-connectivity check needs a real adapter to pass.
Actual provider authentication and execution remain separate live smoke tests.

The 2026-11-10 fixture run used TCK commit
`b7f40fb3c003c45926547be5a1f7e2b374035815` (0.2.0): 30 mandatory and 16
capability requirements passed, with zero failures in those tiers and a
`CONFORMANT` verdict. One mandatory probe was skipped because optional
`$/cancel_request` cancellation was not exercised. The MCP-connectivity advisory
was the only failed advisory; the fixture limitation above explains its scope.
These results do not assert support for unadvertised optional features.
