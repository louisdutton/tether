{ pkgs, ... }:
{
  languages.rust.enable = true;
  packages = [
    pkgs.git
    pkgs.curl
    pkgs.jq
  ];
  scripts.tether-check.exec = "cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test";
  scripts.tether-serve.exec = "cargo run";
  enterTest = "tether-check";
}
