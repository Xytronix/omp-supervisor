import { test, expect } from "bun:test";
import { childEnv, launchOptions, modelInventory, modelPolicy, sessionConversation } from "./omp-rpc-host.mjs";
test("omitted model preserves remote default without enabling auto-approval",()=>{
 const options=launchOptions({cwd:"/tmp"},"/home/test");
 expect(options.args.some(value=>value.startsWith("--model"))).toBe(false);
 expect(options.args).not.toContain("--auto-approve");expect(options.timeout).toBe(6*3600000);expect(options.args).toContain("--max-time=6h");
});
test("model and cwd stay separate argv values, not shell syntax",()=>{
 const options=launchOptions({cwd:"/tmp/space ; project",model:"provider/model; echo nope",max_time:"30s"});
 expect(options.args).toContain("--model=provider/model; echo nope");expect(options.cwd).toBe("/tmp/space ; project");
});
test("rejects malformed launch fields and unbounded runtimes",()=>{
 for(const input of [{model:[]},{cwd:"bad\0path"},{yolo:"true"},{max_time:"0s"},{max_time:"1000h"}])expect(()=>launchOptions(input)).toThrow();
});
test("models inventory reads roles, fallback chains and the catalog without launching",()=>{
 const fs=require("node:fs"),os=require("node:os"),path=require("node:path");
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),"rpc-host-models-"));const bin=path.join(dir,"omp");
 fs.writeFileSync(bin,`#!/bin/sh
case "$*" in
 "config get modelRoles") echo '{"smol":"p/haiku:low"}';;
 "config get retry.fallbackChains") echo '{"smol":["p/flash:max"]}';;
 "models --json") echo '{"models":[{"provider":"p","id":"haiku","thinking":["low"]}]}';;
 *) exit 3;;
esac
`,{mode:0o755});
 expect(modelInventory(bin,{op:"models"})).toEqual({type:"models",roles:{smol:"p/haiku:low"},fallbackChains:{smol:["p/flash:max"]}});
 expect(modelInventory(bin,{op:"models",catalog:true}).models).toEqual([{provider:"p",id:"haiku",thinking:["low"]}]);
});
test("policy op returns the destination policy, or null when omp predates --policy",()=>{
 const fs=require("node:fs"),os=require("node:os"),path=require("node:path");
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),"rpc-host-policy-"));
 const fresh=path.join(dir,"omp-new"),old=path.join(dir,"omp-old"),ignores=path.join(dir,"omp-ignores");
 fs.writeFileSync(fresh,`#!/bin/sh
[ "$*" = "models --policy --json" ] && echo '{"configured":true,"mode":"report","models":[],"roles":[]}' && exit 0
exit 3
`,{mode:0o755});
 fs.writeFileSync(old,"#!/bin/sh\necho 'unknown flag --policy' >&2\nexit 1\n",{mode:0o755});
 fs.writeFileSync(ignores,"#!/bin/sh\necho '{\"models\":[]}'\n",{mode:0o755});
 expect(modelPolicy(fresh)).toEqual({type:"policy",policy:{configured:true,mode:"report",models:[],roles:[]}});
 expect(modelPolicy(old)).toEqual({type:"policy",policy:null});
 expect(modelPolicy(ignores)).toEqual({type:"policy",policy:null});
});
test("children run with the CLI model policy check on",()=>{
 expect(childEnv({PATH:"/bin",OMP_MODEL_POLICY:"off"})).toEqual({PATH:"/bin",OMP_MODEL_POLICY:"enforce"});
});
test("session op reads only the exact session, never another file",async()=>{
 const fs=require("node:fs"),os=require("node:os"),path=require("node:path");
 const home=fs.mkdtempSync(path.join(os.tmpdir(),"rpc-host-session-"));const dir=path.join(home,".omp","agent","sessions","-tmp");fs.mkdirSync(dir,{recursive:true});
 const write=(file,id,text)=>fs.writeFileSync(path.join(dir,file),[{type:"session",id,cwd:"/tmp"},{type:"title",title:"T "+text},{type:"message",message:{role:"user",content:[{type:"text",text}]}},{type:"message",message:{role:"toolResult",content:"secret tool output"}},{type:"message",message:{role:"assistant",content:[{type:"thinking",thinking:"x"},{type:"text",text:"answer"}]}}].map(e=>JSON.stringify(e)).join("\n")+"\n");
 write("2026-11-21T00-00-00-000Z_01a1aaaa-0000-7000-8000-000000000001.jsonl","01a1aaaa-0000-7000-8000-000000000001","mine");
 write("2026-11-22T00-00-00-000Z_01a1bbbb-0000-7000-8000-000000000002.jsonl","01a1bbbb-0000-7000-8000-000000000002","newest other");
 write("2026-11-22T00-00-00-000Z_01a1cccc-0000-7000-8000-000000000003.jsonl","01a1bbbb-0000-7000-8000-000000000002","mislabeled");
 const own=await sessionConversation({session_id:"omp:01a1aaaa-0000-7000-8000-000000000001",max_messages:5},home);
 expect(own.found).toBe(true);expect(own.session.session_id).toBe("01a1aaaa-0000-7000-8000-000000000001");expect(own.session.title).toBe("T mine");
 expect(own.messages).toEqual([{role:"user",text:"mine"},{role:"assistant",text:"answer"}]);
 expect(await sessionConversation({session_id:"01a1dddd-0000-7000-8000-000000000004"},home)).toEqual({type:"session",found:false});
 expect(await sessionConversation({session_id:"01a1aaaa"},home)).toEqual({type:"session",found:false});
 expect((await sessionConversation({session_id:"01a1cccc-0000-7000-8000-000000000003"},home)).found).toBe(false);
 expect((await sessionConversation({session_id:"../../etc/passwd"},home)).error).toBe("Invalid session_id");
});
