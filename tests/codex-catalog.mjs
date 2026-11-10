import assert from "node:assert/strict";
import fs from "node:fs";
const implementation = fs.readFileSync(process.argv[2], "utf8");
const Adapter = new Function("arePathsEqual", `return class { ${implementation} }`)((a, b) => a === b);
const calls = [];
const session = id => ({id, cwd:"/workspace", preview:"Same title", updatedAt:1794301200, source:"appServer"});
const pages = [
  {data:[session("active-1")],nextCursor:"native-page-2"},
  {data:[session("active-2")],nextCursor:null},
  {data:[session("archived-1")],nextCursor:"archive-page-2"},
  {data:[session("archived-2")],nextCursor:null},
];
const adapter = new Adapter();
adapter.codexClient = {
  async threadList(params) {calls.push(params); return pages.shift();},
  async threadRead() {return {thread:{path:"/home/test/.codex/archived_sessions/rollout.jsonl"}};},
  async sendRequest(request) {calls.push(request);return {};},
};
const ids = [];
let cursor;
do {
  const page = await adapter.listSessions({cursor});
  ids.push(...page.sessions.map(s => s.sessionId));
  cursor = page.nextCursor;
} while(cursor);
assert.deepEqual(ids,["active-1","active-2","archived-1","archived-2"]);
assert.deepEqual(calls.map(c=>[c.archived,c.cursor]),[[false,null],[false,"native-page-2"],[true,null],[true,"archive-page-2"]]);
assert(calls.every(c=>c.useStateDbOnly === true));
assert(calls.every(c=>Array.isArray(c.modelProviders)&&c.modelProviders.length===0));
assert(calls.every(c=>c.sourceKinds.includes("subAgentThreadSpawn")));
await adapter.ensureTetherSessionActive("archived-1");
assert.deepEqual(calls.at(-1),{method:"thread/unarchive",params:{threadId:"archived-1"}});
const count = calls.length;
adapter.codexClient.threadRead = async()=>({thread:{path:"/home/test/.codex/sessions/rollout.jsonl"}});
await adapter.ensureTetherSessionActive("active-1");
assert.equal(calls.length,count);
adapter.codexClient.threadList = async () => ({data:[
  {...session("guardian"), source:{subAgent:{other:"guardian"}}, name:"Guardian review"},
  {...session("guardian-db-shape"), source:{subagent:{other:"guardian"}}, name:"Guardian review"},
  {...session("real"), name:"Guardian review"},
  {...session("other-subagent"), source:{subAgent:{other:"worker"}}},
], nextCursor:null});
const classified = await adapter.listSessions({});
assert.deepEqual(classified.sessions.map(s=>s._meta.tether.internal), [true,true,false,false]);
assert(classified.sessions.every(s=>s.updatedAt === "2026-11-10T09:00:00.000Z"));
console.log("Codex catalog: all profiles, native pages, active/archived phases and archive reopening passed");
