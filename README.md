# Tether

A Rust execution service that speaks **Agent Client Protocol (ACP) v1** to both
applications and agents. One shared ACP client launches Codex, Claude Code or
another ACP agent; upstream adapters own the native protocol integration.

Tether adds persistent conversations that can continue across agents while
retaining one session identity and transcript.

## What it does

- Standard ACP session creation, listing, loading, prompts and cancellation.
- Standard configuration selectors for the active agent, models, reasoning and
  other settings advertised by the agent.
- ACP message, reasoning, plan, tool, diff and command updates, with rich prompt
  content passed through using the standard types.
- Standard stdio and a documented custom WebSocket transport.
- Persistent managed history, including original prompt content, readable when
  an agent is unavailable.
- Separate native session bindings and bounded text handover between agents.
- Native discovery, direct session adoption and managed import where supported.

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

Interactive permission requests receive ACP's cancelled outcome. Tether does
not advertise client filesystem or terminal services. There is no approval UI,
automatic prompt retry, durable transport replay or message idempotency key.

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

See [API](docs/api.md), [architecture](docs/architecture.md) and
[interop references](docs/interop.md).
