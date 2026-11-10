  async listSessions(request) {
    let page = { archived: false, cursor: null };
    if (request.cursor) {
      page = JSON.parse(Buffer.from(request.cursor, "base64url").toString("utf8"));
      if (typeof page.archived !== "boolean" || !(page.cursor === null || typeof page.cursor === "string")) {
        throw new Error("Invalid native catalog cursor");
      }
    }
    const response = await this.codexClient.threadList({
      archived: page.archived,
      cursor: page.cursor,
      limit: 100,
      // Explicit discovery reconciles native files as well as the state database.
      useStateDbOnly: false,
      modelProviders: [],
      sourceKinds: ["cli", "vscode", "exec", "appServer", "subAgent", "subAgentReview", "subAgentCompact", "subAgentThreadSpawn", "subAgentOther", "unknown"]
    });
    const next = response.nextCursor != null
      ? { archived: page.archived, cursor: response.nextCursor }
      : !page.archived ? { archived: true, cursor: null } : null;
    return {
      sessions: response.data
        .filter((thread) => !request.cwd || arePathsEqual(thread.cwd, request.cwd))
        .map((thread) => ({
          sessionId: thread.id,
          cwd: thread.cwd,
          title: (thread.name ?? thread.preview) || null,
          updatedAt: Number.isFinite(thread.updatedAt) ? new Date(thread.updatedAt * 1000).toISOString() : null,
          _meta: { tether: { internal:
            thread.source?.subAgent?.other === "guardian" ||
            thread.source?.subagent?.other === "guardian"
          } }
        })),
      nextCursor: next ? Buffer.from(JSON.stringify(next)).toString("base64url") : null
    };
  }
  async ensureTetherSessionActive(sessionId) {
    const response = await this.codexClient.threadRead({threadId: sessionId, includeTurns: false});
    const path = response.thread.path;
    if (typeof path === "string" && path.replaceAll("\\", "/").split("/").includes("archived_sessions")) {
      await this.codexClient.sendRequest({method: "thread/unarchive", params: {threadId: sessionId}});
    }
  }
