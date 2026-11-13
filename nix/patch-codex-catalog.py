"""Adapt codex-acp's native catalog through supported app-server operations."""
from pathlib import Path
import sys

path = Path(sys.argv[1])
source = path.read_text()
start_marker = "  async listSessions(request) {"
end_marker = "  async turnInterrupt(params) {"
assert source.count(start_marker) == 1, "codex-acp catalog implementation changed"
start = source.index(start_marker)
end = source.index(end_marker, start)
source = source[:start] + Path(sys.argv[2]).read_text() + source[end:]
for signature in ["  async loadSession(request, onSubscribed) {", "  async resumeSession(request, onSubscribed) {"]:
    assert source.count(signature) == 1, "codex-acp session-open implementation changed"
    source = source.replace(signature, signature + "\n    await this.ensureTetherSessionActive(request.sessionId);", 1)
archive = "await this.codexClient.threadArchive({ threadId: sessionId });"
assert source.count(archive) == 1, "codex-acp delete implementation changed"
source = source.replace(archive, 'await this.deleteTetherSessionPermanently(sessionId);', 1)
registration = ".onRequest(methods.agent.session.delete, (ctx) => getAgent().deleteSession(ctx.params))"
assert source.count(registration) == 1, "codex-acp delete registration changed"
source = source.replace(registration, registration + '.onRequest("_tether/session/delete_permanently", zDeleteSessionRequest, (ctx) => getAgent().deleteSession(ctx.params))', 1)
steer_parse = """    return {
      sessionId,
      prompt
    };
  }
  createSessionConfigOptions"""
assert source.count(steer_parse) == 1, "codex-acp steering parser changed"
source = source.replace(steer_parse, steer_parse.replace("      prompt\n", '      prompt,\n      _meta: params["_meta"]\n'), 1)
steer_fallback = "    return await this.startNewTurnFromSteering(params);"
assert source.count(steer_fallback) == 1, "codex-acp steering fallback changed"
source = source.replace(steer_fallback, '''    if (params._meta?.steering?.idleBehavior === "promptRequired") {
      return { outcome: "promptRequired", reason: "noRunningTurn" };
    }
''' + steer_fallback, 1)
# New Codex versions deprecate autoResolutionMs in favour of isBlocking and
# may omit it. Missing/null means no adapter-imposed timeout, not zero ms.
question_timeout = "    if (params.autoResolutionMs === null) {"
assert source.count(question_timeout) == 1, "codex-acp question timeout changed"
source = source.replace(question_timeout, "    if (params.autoResolutionMs == null) {", 1)
path.chmod(path.stat().st_mode | 0o200)
path.write_text(source)
