import assert from 'node:assert/strict';
import fs from 'node:fs';
const source = fs.readFileSync(process.argv[2], 'utf8');
const match = source.match(/  async requestUserInputElicitation\(params\) \{([\s\S]*?)\n  \}\n  createMcpElicitationContext/);
assert.ok(match, 'actual adapter question handler must be present');
const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
const run = new AsyncFunction('params','methods','setTimeout','clearTimeout',match[1]);
const methods = {client:{elicitation:{create:'elicitation/create'}}};
for (const params of [{isBlocking:true},{isBlocking:false},{autoResolutionMs:null}]) {
  let resolve; const timers=[];
  const response = new Promise(r=>{resolve=r;});
  const handler = {buildUserInputRequest:p=>p, requestOptions:()=>({}), connection:{request:()=>response}};
  const pending = run.call(handler,params,methods,(callback,delay)=>{timers.push({callback,delay});},()=>{});
  assert.equal(timers.length,0,'omitted/null timeout must not auto-cancel a question');
  const accepted = {action:'accept',content:{target:'phone'}};
  resolve(accepted);
  assert.equal(await pending,accepted);
}
for (const delay of [0,100]) {
  let timer; let signal;
  const handler = {buildUserInputRequest:p=>p,requestOptions:s=>({signal:s}),connection:{request:(_m,_p,options)=>{signal=options.signal;return new Promise(()=>{});}}};
  const pending = run.call(handler,{autoResolutionMs:delay},methods,(callback,ms)=>{timer={callback,ms};return 1;},()=>{});
  assert.equal(timer.ms,delay);
  timer.callback();
  assert.equal(await pending,null);
  assert.equal(signal.aborted,true);
}
console.log('Codex question timeout compatibility checks passed');
