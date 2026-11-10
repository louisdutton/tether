{
  codex-acp,
  python3,
  nodejs,
}:
codex-acp.overrideAttrs (old: {
  nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [
    python3
    nodejs
  ];
  postInstall = (old.postInstall or "") + ''
    python3 ${./patch-codex-catalog.py} "$out/lib/node_modules/@agentclientprotocol/codex-acp/dist/index.js" ${./codex-catalog.js}
    node --check "$out/lib/node_modules/@agentclientprotocol/codex-acp/dist/index.js"
    node ${../tests/codex-catalog.mjs} ${./codex-catalog.js}
  '';
})
