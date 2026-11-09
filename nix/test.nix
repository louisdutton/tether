{
  testers,
  writeShellApplication,
  bash,
  coreutils,
  curl,
  jq,
  tetherPackage,
}:
let
  agent = writeShellApplication {
    name = "tether-test-agent";
    runtimeInputs = [
      bash
      coreutils
      jq
    ];
    # The same deterministic ACP peer used by the Rust integration tests.
    text = builtins.readFile ../tests/fixtures/acp;
    checkPhase = "";
  };
in
testers.runNixOSTest {
  name = "tether-service";
  nodes.machine = {
    imports = [ ./module.nix ];
    environment.systemPackages = [ curl ];
    users.groups.agent = { };
    users.users.agent = {
      isSystemUser = true;
      group = "agent";
      home = "/home/agent";
      createHome = true;
    };
    systemd.tmpfiles.rules = [ "d /srv/projects 0700 agent agent -" ];
    services.tether = {
      enable = true;
      package = tetherPackage;
      user = "agent";
      workspaceRoot = "/srv/projects";
      agents.fixture = {
        command = "${agent}/bin/tether-test-agent";
        args = [
          "fixture"
          "/var/lib/tether/fixture"
        ];
      };
    };
  };

  testScript = ''
    import json

    machine.start()
    machine.wait_for_unit("tether.service")
    machine.wait_for_open_port(8787)
    machine.succeed("curl --fail http://127.0.0.1:8787/healthz")
    machine.succeed("test $(stat -c %U /var/lib/tether) = agent")
    machine.succeed("test $(stat -c %a /var/lib/tether) = 700")
    assert machine.succeed("systemctl show tether --property=User --value").strip() == "agent"
    assert json.loads(machine.succeed("cat /var/lib/tether/fixture/requests.jsonl"))["method"] == "initialize"
    machine.succeed("systemctl restart tether.service")
    machine.wait_for_open_port(8787)
    machine.succeed("curl --fail http://127.0.0.1:8787/healthz")
    machine.succeed("systemctl stop tether.service")
    machine.fail("curl --fail http://127.0.0.1:8787/healthz")
  '';
}
