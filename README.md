# Tether

A Rust execution service that speaks **Agent Client Protocol (ACP) v1** to both
applications and agents. One shared ACP client launches Codex, Claude Code or
another ACP agent; upstream adapters own the native protocol integration.

Tether adds persistent conversations that can continue across agents while
retaining one session identity and transcript.

## What it does

- Standard ACP session creation, listing, loading, resume, deletion, prompts and cancellation; native close when supported.
- Standard configuration selectors for the active agent, models, reasoning and
  other settings advertised by the agent.
- ACP message, reasoning, plan, tool, diff and command updates, with rich prompt
  content passed through using the standard types.
- Standard stdio and a documented custom WebSocket transport.
- Persistent managed history, including original prompt content, readable when
  an agent is unavailable.
- Separate native session bindings and bounded text handover between agents.
- One binding-deduplicated session catalog across configured agents, with stable IDs, provider activity timestamps, newest-first snapshot pagination, and source-based exclusion of internal Codex Guardian reviewers.
- Verified permanent native deletion; unsupported or failed deletion returns an error and keeps the session visible.
- Direct session adoption and managed import where supported.

This build serves one user on one execution machine. It has no frontend, relay,
pairing, multi-machine routing, delegation engine or worktree manager. Local
history is plaintext in a private data directory.

## Run

Requires Linux, Nix + devenv and installed, authenticated ACP agent adapters.
Install [codex-acp](https://github.com/agentclientprotocol/codex-acp) and optionally
[claude-agent-acp](https://github.com/agentclientprotocol/claude-agent-acp)
following their instructions. Authenticate with the underlying provider before
starting Tether. Tether does not install adapters automatically.

```sh
cd ~/projects/tether
devenv shell
export TETHER_WORKSPACE_ROOT="$HOME/projects"
export TETHER_CODEX_ACP_BIN=/absolute/path/to/codex-acp
# Optional second agent:
export TETHER_CLAUDE_ACP_BIN=/absolute/path/to/claude-agent-acp

# ACP WebSocket at ws://127.0.0.1:8787/acp
tether-serve
```

Any ACP executable can be configured without adding Rust provider code:

```sh
export TETHER_AGENTS='[
  {"id":"codex","command":"/absolute/path/to/codex-acp","args":[]},
  {"id":"claude","command":"/absolute/path/to/claude-agent-acp","args":[]}
]'
```

`command` and `args` are passed directly to the process, without a shell. The
agent inherits the execution server's environment and provider login. Keep IDs
stable across restarts because stored conversation bindings refer to them.

For standard stdio, build with `cargo build`, then configure the application to
launch the absolute path to `target/debug/tether` with argument `--stdio` and
the same environment. Stdout contains ACP JSON-RPC; logs go to stderr.

One Tether process owns each data directory. Multiple clients of a running server
should use WebSocket; independent stdio processes need separate data directories.
Closing Tether's stdin or stopping its process stops its owned agent processes.
Disconnecting a WebSocket client leaves execution running.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `TETHER_WORKSPACE_ROOT` | Required | Directory containing permitted projects |
| `TETHER_BIND` | `127.0.0.1:8787` | WebSocket/health address; unused with `--stdio` |
| `TETHER_AGENTS` | Unset | JSON array of ACP commands; overrides the two convenience variables |
| `TETHER_CODEX_ACP_BIN` | `codex-acp` | Codex ACP adapter executable |
| `TETHER_CLAUDE_ACP_BIN` | Unset | Enable a Claude ACP adapter |
| `TETHER_DATA_DIR` | `$XDG_DATA_HOME/tether` or `~/.local/share/tether` | SQLite history and ACP session/settings records |
| `RUST_LOG` | `tether=info` | Logging to stderr |

`GET /healthz` returns readiness while any agent is connected. Access control
belongs to the trusted network, tunnel or authenticated reverse proxy. Tether
has no application credentials. Anyone reaching it can control its sessions.
Workspace checks constrain session directories; agents own their sandbox and
execution policies.

Interactive permission requests and ACP form/URL elicitations wait for a client
response. Pending requests belong to the agent turn and survive phone disconnects;
agent cancellation, interrupted/failed turns, session close or server shutdown
ends them. Non-blocking questions survive normal turn completion. They are not
replayed after an agent/server restart. Tether does not advertise client filesystem
or terminal services, and never automatically approves a request or retries a prompt.

## NixOS

The flake exports `packages.<system>.default`, `nixosModules.default` (also
`nixosModules.tether`) and `overlays.default` for x86-64 and ARM64 Linux.

Add `github:louisdutton/tether` to your flake inputs and import
`inputs.tether.nixosModules.default`. Then configure the service:

```nix
{ lib, pkgs, ... }: {
  services.tether = {
    enable = true;
    user = "louis";
    workspaceRoot = "/home/louis/projects";
    agents.codex.command = lib.getExe pkgs.codex-acp;
  };
}
```

The service uses that user's home and existing provider login, keeps state in
`/var/lib/tether`, and listens on `127.0.0.1:8787`. See the
[NixOS setup guide](docs/nixos.md) for a complete flake example and options.

## Develop

```sh
devenv shell -- tether-check
devenv test
nix flake check
```

Checks run formatting, Clippy and tests. Tests use the official ACP SDK on the
application side and deterministic ACP agent subprocesses on the other side,
including real WebSocket and executable stdio connections. Live Codex/Claude
adapter execution still needs a credentialed smoke test.

See [ACP v1 coverage](docs/acp-conformance.md), [API](docs/api.md), [architecture](docs/architecture.md) and
[interop references](docs/interop.md).

### Messages during a running turn

Tether passes through the capability-advertised `_session/steering` extension.
`initialize._meta.steering.supported` is true when all configured agents support
it. A steering request takes `sessionId` and `prompt` (ACP content blocks). Its
result is `injected`, or `promptRequired` when the active turn has finished. The
host then sends a normal `session/prompt`; the bundled Codex adapter honours
`_meta.steering.idleBehavior = "promptRequired"` so it never creates a detached
fallback turn behind Tether's back.

`agentCapabilities._meta.tether.messageQueue` advertises a persistent queue:

| Method | Parameters | Result |
| --- | --- | --- |
| `_tether/session/queue` | `sessionId`, stable `messageId`, `prompt` | Saved message receipt |
| `_tether/session/queue/list` | `sessionId` | `messages` |
| `_tether/session/queue/delete` | `sessionId`, `messageId` | Updated `messages` |
| `_tether/session/queue/resume` | `sessionId`, paused `messageId` | Updated `messages` |

These operations require attachment, just like prompting. The queue is stored
before acknowledgement and executes FIFO, independently of client connections.
Repeating an enqueue with the same ID and content returns its existing receipt;
reusing an ID for different content fails. Clients subscribing to Tether events
receive `queue.updated` with `data.messages` and `turn.started`/terminal events.
Stopping or closing a session pauses its backlog. Failed/interrupted turns also
pause it. A dispatch interrupted by a server crash is retained as `uncertain`
and is never automatically resent; pending messages behind it remain blocked
until the uncertain entry is reviewed and removed. Restarting during an active
turn preserves subsequent queued messages as paused. Deleting a session deletes
its backlog. Removing a queued message preserves its cancelled receipt so a late
retry cannot enqueue it again.

## Monitoring and interactive requests

Opt in to `_tether/event` using `clientCapabilities._meta.tether`. The capabilities
`agentCapabilities._meta.tether.sessionStatus`, `clientRequests`, and `eventReplay`
are independently advertised. `session.status` events contain the public
`session_id`, `turn_id`, and `data` with current `status`, `cwd`, `title`,
`updatedAt`, and the triggering `outcome`. These lightweight events cover all
allowed sessions without attaching to or replaying their transcripts. Treat
unavailable/disconnected session state as unknown rather than idle.

`_tether/requests/list` returns `requests` with stable `requestId`, public
`sessionId`, standard ACP `method`, and unmodified request `params` except for the
public session ID. `client.requested` and `client.resolved` events signal changes.
Respond using `_tether/requests/respond` with `sessionId`, `requestId`, and a
standard ACP `response` (`outcome` for permissions; `action`/`content` for
`elicitation/create`). Attachment is not required. Invalid answers leave the
request pending. Responses are consumed atomically; a second client's stale
response fails instead of being applied to another question. Agent-side
`$/cancel_request` withdraws the pending request.

`_tether/events/list` recovers missed terminal-turn alerts using a string `cursor`.
Without a cursor it establishes the current position and returns no historical
alerts. Subsequent pages contain `events` (`session_id`, `turn_id`, `kind`), a new
`cursor`, and `more`. The last 1024 terminal events are stored in SQLite; `truncated`
indicates the cursor has fallen outside that window. Store the cursor only after
processing each page, and deduplicate live/replayed alerts by session, turn and
kind. This journal contains result metadata, not transcript or tool-output replay.

The packaged Codex adapter treats omitted/null deprecated `autoResolutionMs` as
no timeout, including newer `isBlocking` requests. Explicit numeric timeouts are
preserved. Regression checks execute the patched adapter's actual question handler.
