# NixOS service

Tether exports an importable NixOS module, a Rust package and an optional overlay.
The flake supports `x86_64-linux` and `aarch64-linux`.

## Import from a system flake

Add the input and module to your existing system configuration:

```nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    tether.url = "github:louisdutton/tether";
    tether.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { nixpkgs, tether, ... }: {
    nixosConfigurations.my-host = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ./configuration.nix
        tether.nixosModules.default
        ({ lib, pkgs, ... }: {
          services.tether = {
            enable = true;
            user = "louis";
            workspaceRoot = "/home/louis/projects";
            agents.codex = {
              command = lib.getExe tether.packages.${pkgs.stdenv.hostPlatform.system}.codex-acp;
              permanentDelete.method = "_tether/session/delete_permanently";
            };
          };
        })
      ];
    };
  };
}
```

Replace the hostname, user and workspace directory with your existing values.
The module runs as that user and does not create an account. The workspace must
exist and be accessible to that user. `HOME` defaults to the user's home from
`users.users`, so existing provider logins and agent configuration remain available.
Set `services.tether.home` explicitly when the account is managed externally.

The agent command points to an **ACP adapter**, rather than the native `codex`
or `claude` executable. Using `lib.getExe` includes the adapter in the system
closure; it does not need to be added to `environment.systemPackages` separately.
Authenticate the underlying runtime as the service user before starting Tether.

Apply your system configuration normally, then inspect the service:

```sh
systemctl status tether.service
journalctl -u tether.service -f
curl --fail http://127.0.0.1:8787/healthz
```

## Agents and runtime tools

Configure agents by stable ID. IDs are persisted in native conversation bindings,
so keep them stable when updating the system:

```nix
services.tether = {
  agents = {
    codex.command = lib.getExe pkgs.codex-acp;
    claude.command = lib.getExe pkgs.claude-agent-acp;
  };
  extraPackages = [ pkgs.ripgrep pkgs.nodejs ];
};
```

The Claude adapter depends on its native runtime; permit that package through
your existing nixpkgs unfree-package configuration if required.

Each agent also accepts an `args` list. Commands and arguments are passed
directly to the executable, without shell evaluation. Agent IDs may contain
letters, digits, hyphens and underscores. At least one agent must be configured.
Bash, coreutils and Git are available on the service PATH; add workspace-specific
tools through `extraPackages`.

## Native catalogs and permanent deletion

`packages.<system>.codex-acp` is a version-checked patch of the nixpkgs adapter.
It enumerates active and archived threads across all model providers and source
kinds, reopens archived threads for load/resume, and exposes the supported native
`thread/delete` API through `_tether/session/delete_permanently`. Use it together
with the `permanentDelete.method` configuration in the complete example above.
The adapter uses the same Codex home, credentials and runtime as ordinary sessions.

Deletion is disabled per agent unless `permanentDelete` is configured. The stock
Codex ACP adapter archives on session/delete; configuring that call as permanent
deletion is incorrect. An alternative integration may configure an absolute
`permanentDelete.command` and fixed `args`; Tether appends `--` and the exact bound
native session ID without invoking a shell. Configure exactly one command or ACP
extension method, and ensure its documented behavior permanently removes native
history. Both operation success and fresh-catalog absence are required. Every
binding of a cross-agent session must support deletion, or the request fails and
the conversation stays visible. Do not use Codex's external delete CLI for loaded
adapter sessions; the packaged same-server API handles that case.

## State, networking and environment

The service creates `/var/lib/tether` with mode `0700`, owned by its configured
user. SQLite history and per-agent session/config records persist there across
restarts and system upgrades. Include this directory and the user's native
agent storage when backing up. `stateDirectory` changes the directory name
beneath `/var/lib`.

The default listener is loopback and the firewall stays unchanged. To use a
trusted network address, configure it explicitly:

```nix
services.tether = {
  listenAddress = "100.64.0.10";
  port = 8787;
  # Optional; otherwise configure your firewall/tunnel separately.
  openFirewall = true;
};
```

IPv6 addresses are supported, for example `listenAddress = "::1"`. The ACP
endpoint is `/acp` and health is `/healthz`. There is no application authentication;
access control belongs to your tunnel, trusted network or reverse proxy.

Add non-secret environment values through `environment`. For credentials loaded
at runtime, use systemd environment files:

```nix
services.tether = {
  environment.RUST_LOG = "tether=debug";
  environmentFiles = [ "/run/secrets/tether-env" ];
};
```

These are runtime path strings, not Nix file literals: their contents are not
copied into the Nix store. Inline `environment` values and agent arguments do
become part of the generated system configuration.

The module sets the working directory to `workspaceRoot`, restarts after failure
and stops the service's process group on shutdown. It uses the agent's native
execution policy. Interactive permission requests currently receive ACP's
cancelled outcome; there is no approval UI.

## Flake outputs and checks

- `nixosModules.default` and `nixosModules.tether`: the same NixOS module.
- `packages.<system>.default` and `packages.<system>.tether`: the Rust package.
- `packages.<system>.codex-acp`: complete Codex catalog and permanent deletion integration.
- `apps.<system>.default`: run Tether with `nix run` and the usual environment.
- `overlays.default`: optionally add `pkgs.tether` to another package set.
- `checks.<system>.package`: package build and Rust integration tests.
- `checks.<system>.nixos`: NixOS VM service test with a deterministic ACP agent.

`services.tether.package` can override the module's package. By default it is
built using the importing system's nixpkgs. Following the system's `nixpkgs` input
keeps package and module versions aligned.

```sh
nix build
nix flake check
nix flake check --all-systems --no-build
```

The VM test checks startup, the health endpoint, execution as the configured user,
private persistent state, restart and shutdown. It does not require provider
credentials or make model calls.
