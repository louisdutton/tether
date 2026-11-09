# Interop implementation reference

Use **ACP first on both sides of Tether**. Applications connect to the ACP server;
configured agent executables connect through one shared ACP client. Use upstream
ACP adapters for Codex and Claude rather than maintaining native app-server or
Claude stream-json translations inside Tether.

- [ACP v1](https://agentclientprotocol.com/protocol/v1/overview)
- [Rust SDK](https://agentclientprotocol.com/libraries/rust)
- [Codex ACP adapter](https://github.com/agentclientprotocol/codex-acp)
- [Claude ACP adapter](https://github.com/agentclientprotocol/claude-agent-acp)

Prefer standard lifecycle methods, config options, content blocks and session
updates. Preserve agent-advertised catalogs and capabilities. Add a Tether
extension only for behavior the standard cannot express; ordinary clients should
not need extension methods to choose an agent/model or read conversation history.
See [API](api.md) for the supported surface and [architecture](architecture.md)
for the storage and ownership boundary.

Stdio is the standard transport. Tether's WebSocket framing is a documented
custom ACP transport; it does not claim the proposed ACP HTTP transport.

## Happier reference

Use [Happier](https://github.com/happier-dev/happier) as the implementation
reference for session ownership and cross-agent continuation semantics that ACP
does not define. Consult its existing code and failure cases before inventing
behavior. Prefer upstream ACP integration over copying native protocol adapters.

Previously reviewed snapshot:
[`7b37d65ca75e58560f8f3ca850958ac5407d2e75`](https://github.com/happier-dev/happier/commit/7b37d65ca75e58560f8f3ca850958ac5407d2e75).
Useful reference areas include `apps/cli/src/backends/types.ts`, provider prompt
acceptance implementations and `packages/agents`.

Design rules carried into Tether:

1. Keep a conversation identity above individual agent-native sessions.
2. Make managed versus direct transcript ownership explicit.
3. Preserve standard structured content and provider attribution.
4. Expose capability differences instead of assuming parity.
5. Resume a returning agent's native conversation and provide its away-context.
6. Treat delivery and completion separately. ACP has no receipt acknowledgement;
   preserve ambiguity on errors/cancellation and never retry automatically.
7. Verify idleness before changing execution ownership.

Happier's product documentation covers
[session ownership](https://docs.happier.dev/sessions/continuing-a-session),
[agent switching](https://docs.happier.dev/sessions/continue-with-another-agent)
and [agent capabilities](https://docs.happier.dev/agents/capabilities).
