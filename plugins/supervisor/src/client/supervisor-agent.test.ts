import { test, expect, afterEach } from "bun:test";
import supervisorAgent from "./supervisor-agent.extension.js";
const originalFetch = globalThis.fetch;
const oldUrl=process.env.SUPERVISOR_URL, oldToken=process.env.SUPERVISOR_TOKEN;
afterEach(()=>{globalThis.fetch=originalFetch;if(oldUrl===undefined)delete process.env.SUPERVISOR_URL;else process.env.SUPERVISOR_URL=oldUrl;if(oldToken===undefined)delete process.env.SUPERVISOR_TOKEN;else process.env.SUPERVISOR_TOKEN=oldToken;});
test("interactive registration, delivery deduplication and lifecycle cleanup", async()=>{
 process.env.SUPERVISOR_URL="http://127.0.0.1:18200";process.env.SUPERVISOR_TOKEN="test-enrollment";
 const hooks=new Map(), timers=new Map(), injected=[],custom=[],previews=[],calls=[],acks=[];let next=0,registered=0;
 const finishTurn=(text)=>{
  const end=hooks.get("agent_end");
  end?.({type:"agent_end",willContinue:true,messages:[{role:"assistant",content:"not the reply"}]});
  end?.({type:"agent_end",messages:[{role:"assistant",content:[{type:"text",text:`reply:${text}`}]}]});
 };
 globalThis.fetch=async(url,opts)=>{
  const p=new URL(url).pathname;calls.push(p);
  if(p.endsWith("/register")){registered++;return Response.json({agent_id:"agt_test",agent_token:"test-session"});}
  if(p.endsWith("/inbox"))return Response.json({deliveries:[{id:"delivery1",text:"test",mode:"follow_up",from:"Worker",source:{kind:"agent",from:"Worker",name:"Researcher",title:"Observed task",route:"supervisor"}},{id:"human1",text:"human instruction",mode:"steer",from:"Human label"}]});
  if(p.endsWith("/ack"))acks.push(JSON.parse(opts.body));
  return Response.json({ok:true});
 };
 const pi={on:(event,fn)=>hooks.set(event,fn),sendUserMessage:(text,options)=>{injected.push({text,...options});finishTurn(text);},sendMessage:(message,options)=>{custom.push({message,options});finishTurn(message.details.message);},sendIrc:(...args)=>previews.push(args),getSessionName:()=>"smoke"};
 const ctx={hasUI:true,cwd:"/test",sessionManager:{getSessionId:()=>"test-session"},ui:{notify(){}},isIdle:()=>true,
  setTimeout:(fn,delay)=>{let id=++next;timers.set(id,{fn,interval:false});return id;},
  setInterval:(fn,delay)=>{let id=++next;timers.set(id,{fn,interval:true});return id;},clearTimer:id=>timers.delete(id)};
 supervisorAgent(pi);await hooks.get("session_start")({},ctx);
 const poll=async()=>{const [id,t]=[...timers].find(([_,t])=>!t.interval);timers.delete(id);await t.fn();};
 expect(registered).toBe(1);await poll();await poll();
 expect(injected).toEqual([{text:"human instruction"}]);
 expect(custom).toEqual([{message:{customType:"irc:incoming",content:expect.stringContaining("Worker"),display:true,attribution:"agent",details:{id:"delivery1",from:"Worker",message:"test",fromPeer:{name:"Researcher",title:"Observed task",route:"supervisor"}}},options:{triggerTurn:true}}]);
 expect(previews).toEqual([]);
 // Peer messages ack with no result (the turn's text is never their reply); the human prompt acks with its own reply. Redelivery re-acks from cache.
 expect(acks.filter((ack)=>ack.delivery_id==="delivery1").map((ack)=>ack.result)).toEqual(["",""]);
 expect(acks.filter((ack)=>ack.delivery_id==="human1").map((ack)=>ack.result)).toEqual(["reply:human instruction","reply:human instruction"]);
 expect(acks.every((ack)=>ack.status==="ok"&&!String(ack.result).includes("injected into session"))).toBe(true);
 expect(timers.size).toBe(2);
 await hooks.get("session_shutdown")();expect(timers.size).toBe(0);expect(calls.at(-1)).toContain("unregister");
 await hooks.get("session_start")({},ctx);expect(registered).toBe(2);await poll();expect(injected.length).toBe(2);await hooks.get("session_shutdown")();expect(timers.size).toBe(0);
});
test("invalid agent source is not delivered as a human prompt", async()=>{
 process.env.SUPERVISOR_URL="http://127.0.0.1:18200";process.env.SUPERVISOR_TOKEN="test-enrollment";
 const hooks=new Map(), timers=new Map(), injected=[],custom=[],acks=[];let next=0;
 globalThis.fetch=async(url,opts)=>{
  const p=new URL(url).pathname;
  if(p.endsWith("/register"))return Response.json({agent_id:"agt_test",agent_token:"test-session"});
  if(p.endsWith("/inbox"))return Response.json({deliveries:[{id:"bad1",text:"/danger",mode:"steer",from:"Pat",source:{kind:"human",from:"Pat"}}]});
  if(p.endsWith("/ack"))acks.push(JSON.parse(opts.body));
  return Response.json({ok:true});
 };
 const pi={on:(event,fn)=>hooks.set(event,fn),sendUserMessage:(text,options)=>injected.push({text,...options}),sendMessage:(message,options)=>custom.push({message,options}),sendIrc(){},getSessionName:()=>"smoke"};
 const ctx={hasUI:true,cwd:"/test",sessionManager:{getSessionId:()=>"test-session"},ui:{notify(){}},isIdle:()=>true,
  setTimeout:(fn)=>{let id=++next;timers.set(id,{fn,interval:false});return id;},
  setInterval:(fn)=>{let id=++next;timers.set(id,{fn,interval:true});return id;},clearTimer:id=>timers.delete(id)};
 supervisorAgent(pi);await hooks.get("session_start")({},ctx);
 const poll=async()=>{const [id,t]=[...timers].find(([_,entry])=>!entry.interval);timers.delete(id);await t.fn();};
 await poll();
 expect(injected).toEqual([]);expect(custom).toEqual([]);
 expect(acks).toEqual([{delivery_id:"bad1",status:"error",result:"",error:"Invalid agent message source"}]);
 await hooks.get("session_shutdown")();
});
test("refuses Supervisor deliveries addressed to this session and stamps the sender session on peers", async()=>{
 process.env.SUPERVISOR_URL="http://127.0.0.1:18200";process.env.SUPERVISOR_TOKEN="test-enrollment";
 const hooks=new Map(), timers=new Map();let next=0;
 globalThis.fetch=async(url)=>{
  const p=new URL(url).pathname;
  if(p.endsWith("/register"))return Response.json({agent_id:"agt_self",agent_token:"agent-token"});
  return Response.json({deliveries:[]});
 };
 const pi={on:(event,fn)=>hooks.set(event,fn),sendUserMessage(){},sendMessage(){},sendIrc(){},getSessionName:()=>"smoke"};
 const ctx={hasUI:true,cwd:"/test",sessionManager:{getSessionId:()=>"sess-self"},ui:{notify(){}},isIdle:()=>true,
  setTimeout:(fn)=>{let id=++next;timers.set(id,{fn,interval:false});return id;},
  setInterval:(fn)=>{let id=++next;timers.set(id,{fn,interval:true});return id;},clearTimer:id=>timers.delete(id)};
 supervisorAgent(pi);await hooks.get("session_start")({},ctx);
 const toolCall=hooks.get("tool_call");
 for (const input of [{agent_id:"agt_self",text:"x"},{session_id:"sess-self",text:"x"}]) {
  const result=await toolCall({type:"tool_call",toolCallId:"c",toolName:"mcp__supervisor__prompt",input},ctx);
  expect(result?.block).toBe(true);expect(result?.reason).toContain("cannot prompt itself");
 }
 expect(await toolCall({type:"tool_call",toolCallId:"c",toolName:"mcp__supervisor__steer",input:{agent_id:"agt_peer",text:"x"}},ctx))
  .toEqual({input:{agent_id:"agt_peer",text:"x",sender_session_id:"sess-self"}});
 expect(await toolCall({type:"tool_call",toolCallId:"c",toolName:"mcp__supervisor__prompt",input:{agent_id:"agt_peer",text:"x",sender_session_id:"sess-peer"}},ctx))
  .toEqual({input:{agent_id:"agt_peer",text:"x",sender_session_id:"sess-self"}});
 expect(await toolCall({type:"tool_call",toolCallId:"c",toolName:"mcp__other__prompt",input:{agent_id:"agt_self",text:"x"}},ctx)).toBeUndefined();
 expect(await toolCall({type:"tool_call",toolCallId:"c",toolName:"mcp__supervisor__x__prompt",input:{agent_id:"agt_self",text:"x"}},ctx)).toBeUndefined();
 const code='a = Supervisor.agents()\nr = Supervisor.prompt(agent_id="agt_self", text="hi")\ns = Supervisor.steer( sender_session_id="sess-peer", job_id="sub_1", text="go")\nresult = Other.prompt(x=1)';
 expect(await toolCall({type:"tool_call",toolCallId:"c",toolName:"mcp__bifrost__executeToolCode",input:{code}},ctx)).toEqual({input:{code:
  'a = Supervisor.agents()\nr = Supervisor.prompt(sender_session_id="sess-self", agent_id="agt_self", text="hi")\ns = Supervisor.steer(sender_session_id="sess-self", job_id="sub_1", text="go")\nresult = Other.prompt(x=1)'}});
 expect(await toolCall({type:"tool_call",toolCallId:"c",toolName:"mcp__bifrost__executeToolCode",input:{code:"result = Supervisor.agents()"}},ctx)).toBeUndefined();
 await hooks.get("session_shutdown")();
});
