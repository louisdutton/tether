{
  description = "Tether: persistent coding-agent conversations over ACP";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      packagesFor = system: import nixpkgs { inherit system; };
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = packagesFor system;
          tether = pkgs.callPackage ./nix/package.nix { };
        in
        {
          inherit tether;
          codex-acp = pkgs.callPackage ./nix/codex-acp.nix { };
          default = tether;
        }
      );

      apps = forAllSystems (system: {
        default = {
          type = "app";
          program = nixpkgs.lib.getExe self.packages.${system}.tether;
          meta.description = "Run the Tether ACP execution service";
        };
      });

      overlays.default = final: _prev: {
        tether = final.callPackage ./nix/package.nix { };
      };

      nixosModules = {
        tether = import ./nix/module.nix;
        default = self.nixosModules.tether;
      };

      checks = forAllSystems (
        system:
        let
          pkgs = packagesFor system;
        in
        {
          package = self.packages.${system}.tether;
          nixos = pkgs.callPackage ./nix/test.nix {
            tetherPackage = self.packages.${system}.tether;
          };
        }
      );

      formatter = forAllSystems (system: (packagesFor system).nixfmt-tree);
    };
}
