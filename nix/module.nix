{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.tether;
  absolutePath = lib.types.addCheck lib.types.str (lib.hasPrefix "/");
  address =
    if lib.hasInfix ":" cfg.listenAddress then "[${cfg.listenAddress}]" else cfg.listenAddress;
  agentCommands = lib.mapAttrsToList (id: agent: {
    inherit id;
    inherit (agent) command args;
  }) cfg.agents;
in
{
  options.services.tether = {
    enable = lib.mkEnableOption "Tether ACP execution service";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage ./package.nix { };
      defaultText = lib.literalExpression "pkgs.callPackage ./package.nix { }";
      description = "Tether package to run.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      example = "louis";
      description = ''
        Existing user that owns the repositories and provider credentials.
        The module does not create this user.
      '';
    };

    group = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Service group; null uses the user's primary group.";
    };

    home = lib.mkOption {
      type = absolutePath;
      default = config.users.users.${cfg.user}.home or "/var/lib/tether";
      defaultText = lib.literalExpression ''config.users.users.''${cfg.user}.home or "/var/lib/tether"'';
      description = "HOME for agent configuration and existing provider logins.";
    };

    workspaceRoot = lib.mkOption {
      type = absolutePath;
      example = "/home/louis/projects";
      description = "Existing directory containing the permitted repositories.";
    };

    agents = lib.mkOption {
      default = { };
      description = "ACP stdio agents, keyed by stable agent ID.";
      type = lib.types.attrsOf (
        lib.types.submodule {
          options = {
            command = lib.mkOption {
              type = absolutePath;
              example = "/run/current-system/sw/bin/codex-acp";
              description = "Absolute path to the ACP agent executable.";
            };
            args = lib.mkOption {
              type = lib.types.listOf lib.types.str;
              default = [ ];
              description = "Arguments passed directly to the ACP executable, without a shell.";
            };
          };
        }
      );
    };

    listenAddress = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1";
      example = "100.64.0.10";
      description = "Address for the ACP WebSocket and health endpoints.";
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 8787;
      description = "TCP port for the ACP WebSocket and health endpoints.";
    };

    openFirewall = lib.mkEnableOption "opening the Tether TCP port in the firewall";

    stateDirectory = lib.mkOption {
      type = lib.types.strMatching "[a-zA-Z0-9_.-]+";
      default = "tether";
      description = "State directory name beneath /var/lib, owned by the service user.";
    };

    extraPackages = lib.mkOption {
      type = lib.types.listOf lib.types.package;
      default = [ ];
      description = "Additional tools made available on the agents' PATH.";
    };

    environment = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      description = "Additional environment variables for Tether and its agents.";
    };

    environmentFiles = lib.mkOption {
      type = lib.types.listOf absolutePath;
      default = [ ];
      example = [ "/run/secrets/tether-env" ];
      description = "Runtime systemd environment files, such as provider credentials.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.agents != { };
        message = "services.tether.agents must configure at least one ACP executable.";
      }
      {
        assertion = lib.all (id: builtins.match "[a-zA-Z0-9_-]+" id != null) (
          builtins.attrNames cfg.agents
        );
        message = "services.tether.agents IDs must contain only letters, digits, '-' or '_'.";
      }
    ];

    networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ cfg.port ];

    systemd.services.tether = {
      description = "Tether ACP execution service";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      unitConfig.RequiresMountsFor = [
        cfg.workspaceRoot
        cfg.home
      ];
      path = [
        pkgs.bash
        pkgs.coreutils
        pkgs.git
      ]
      ++ cfg.extraPackages;
      environment = {
        HOME = cfg.home;
        TETHER_WORKSPACE_ROOT = cfg.workspaceRoot;
        TETHER_DATA_DIR = "/var/lib/${cfg.stateDirectory}";
        TETHER_BIND = "${address}:${toString cfg.port}";
        TETHER_AGENTS = builtins.toJSON agentCommands;
      }
      // cfg.environment;
      serviceConfig = {
        User = cfg.user;
        WorkingDirectory = cfg.workspaceRoot;
        ExecStart = lib.getExe cfg.package;
        Restart = "on-failure";
        RestartSec = 5;
        StateDirectory = cfg.stateDirectory;
        StateDirectoryMode = "0700";
        UMask = "0077";
        EnvironmentFile = cfg.environmentFiles;
        KillMode = "control-group";
        TimeoutStopSec = 30;
      }
      // lib.optionalAttrs (cfg.group != null) { Group = cfg.group; };
    };
  };
}
