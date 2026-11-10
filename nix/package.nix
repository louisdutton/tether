{
  lib,
  rustPlatform,
  bash,
  jq,
}:
rustPlatform.buildRustPackage {
  pname = "tether";
  version = (builtins.fromTOML (builtins.readFile ../Cargo.toml)).package.version;

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.toml
      ../Cargo.lock
      ../LICENSE
      ../src
      ../tests
    ];
  };

  cargoLock.lockFile = ../Cargo.lock;

  nativeCheckInputs = [
    bash
    jq
  ];

  postPatch = ''
    patchShebangs tests/fixtures
  '';

  meta = {
    description = "Persistent coding-agent conversations over Agent Client Protocol";
    homepage = "https://github.com/louisdutton/tether";
    license = lib.licenses.mit;
    platforms = lib.platforms.linux;
    mainProgram = "tether";
  };
}
