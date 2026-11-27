import { test, expect, beforeEach, afterEach, spyOn } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import os, { tmpdir } from "node:os";
import { join } from "node:path";
import supervisorAgent from "./supervisor-agent.extension.js";

// Behavior of the converged client: a native `remote` keeps the plugin tool and
// tracker unregistered, hub-admin commands refuse on clients, and URL/token
// resolution keeps its order. A build with a default hub never bootstraps; a
// build without one (DEFAULT_HUB_URL = "") bootstraps only its own local hub.
const DEFAULT_HUB_URL = /const DEFAULT_HUB_URL = "([^"]*)";/.exec(readFileSync(new URL("./supervisor-agent.extension.js", import.meta.url), "utf8"))[1];

const originalFetch = globalThis.fetch;
const envKeys = ["HOME", "USERPROFILE", "SUPERVISOR_URL", "SUPERVISOR_TOKEN", "SUPERVISOR_AGENT_FORCE", "SUPERVISOR_MACHINE", "SUPERVISOR_MCP_URL", "SUPERVISOR_BOOTSTRAP"];
let oldEnv, home, homedirSpy;
beforeEach(() => {
  oldEnv = Object.fromEntries(envKeys.map(key => [key, process.env[key]]));
  home = mkdtempSync(join(tmpdir(), "supervisor-converge-"));
  homedirSpy = spyOn(os, "homedir").mockReturnValue(home);
  process.env.HOME = home; process.env.USERPROFILE = home;
  for (const key of envKeys.slice(2)) delete process.env[key];
});
afterEach(() => {
  globalThis.fetch = originalFetch;
  homedirSpy.mockRestore();
  for (const key of envKeys) if (oldEnv[key] === undefined) delete process.env[key]; else process.env[key] = oldEnv[key];
  rmSync(home, { recursive: true, force: true });
});

function writeConnection(value) {
  mkdirSync(join(home, ".omp", "agent"), { recursive: true });
  writeFileSync(join(home, ".omp", "agent", "supervisor-connection.json"), JSON.stringify(value));
}

const mcpText = (id, value) => Response.json({ jsonrpc: "2.0", id, result: { content: [{ type: "text", text: JSON.stringify(value) }] } });

function harness({ nativeRemote = true, settings = {}, jobs = [], healthy = true } = {}) {
  const hooks = new Map(), timers = new Map(), calls = [], tools = [], commands = new Map(), notifications = [], widgets = [], entriesRead = [];
  let next = 0;
  const ctx = {
    hasUI: true, cwd: "/test", isIdle: () => true,
    sessionManager: { getSessionId: () => "sess-converge", getEntries: () => { entriesRead.push(1); return []; } },
    ui: { notify: (text, level) => notifications.push({ text, level }), setWidget: (...args) => widgets.push(args) },
    agents: { registerExternal: () => { throw new Error("plugin tracker must stay off"); } },
    asyncJobs: { register: () => { throw new Error("plugin tracker must stay off"); } },
    setTimeout: (fn, delay) => { const id = ++next; timers.set(id, { fn, delay }); return id; },
    setInterval: (fn, delay) => { const id = ++next; timers.set(id, { fn, delay }); return id; },
    clearTimer: id => timers.delete(id),
  };
  const sent = [];
  const pi = {
    on: (event, fn) => hooks.set(event, fn), getSessionName: () => "smoke",
    registerCommand: (name, spec) => commands.set(name, spec),
    registerTool: tool => tools.push(tool.name),
    typebox: { Type: new Proxy({}, { get: () => (...args) => ({ args }) }) },
    getAllTools: () => (nativeRemote ? [{ name: "read" }, { name: "remote" }] : [{ name: "read" }]),
    pi: { settings: { getGlobalSettings: () => settings, getProjectSettings: () => ({}) } },
    sendUserMessage() {}, sendMessage: message => sent.push(message), appendEntry() {},
  };
  globalThis.fetch = async (url, options = {}) => {
    const u = new URL(url);
    calls.push({ url: String(url), path: u.pathname, options });
    if (u.pathname.endsWith("/register")) return Response.json({ agent_id: "agt_converge", agent_token: "converge-session" });
    if (u.pathname.endsWith("/inbox")) return Response.json({ deliveries: [] });
    if (u.pathname === "/health") return healthy ? Response.json({ status: "healthy" }) : new Response("down", { status: 503 });
    if (u.hostname === "github.com") return new Response("not found", { status: 404 });
    if (u.pathname.endsWith("/mcp")) {
      const body = JSON.parse(options.body);
      if (body.method === "tools/list") return Response.json({ jsonrpc: "2.0", id: body.id, result: { tools: [] } });
      if (body.params?.name === "jobs") {
        const offset = body.params.arguments.offset ?? 0;
        const page = jobs.slice(offset, offset + body.params.arguments.limit);
        const nextOffset = offset + page.length < jobs.length ? offset + page.length : undefined;
        return mcpText(body.id, { jobs: page, count: page.length, total: jobs.length, offset, ...(nextOffset ? { next_offset: nextOffset } : {}) });
      }
    }
    return Response.json({ ok: true });
  };
  supervisorAgent(pi);
  const run = async (args) => {
    const out = [];
    const write = spyOn(process.stdout, "write").mockImplementation(text => { out.push(String(text)); return true; });
    try { await commands.get("supervisor").handler(args, { ...ctx, hasUI: false }); }
    finally { write.mockRestore(); }
    return out.join("");
  };
  return {
    calls, tools, commands, notifications, widgets, entriesRead, timers, sent, run,
    start: () => hooks.get("session_start")({}, ctx),
    stop: () => hooks.get("session_shutdown")(),
  };
}

const neverBootstraps = h => {
  expect(h.calls.some(call => /github\.com/.test(call.url))).toBe(false);
  expect(h.calls.some(call => call.url.startsWith("http://127.0.0.1:18200"))).toBe(false);
  expect(existsSync(join(home, ".omp", "supervisor"))).toBe(false);
};

test("connection file pointing at the hub registers there and never bootstraps", async () => {
  writeConnection({ url: "https://hub.example.test/", token: "conn-token", machine: "box-a" });
  const h = harness();
  try {
    await h.start();
    const register = h.calls.find(call => call.path === "/api/agents/register");
    expect(register.url).toBe("https://hub.example.test/api/agents/register");
    expect(register.options.headers.Authorization).toBe("Bearer conn-token");
    expect(JSON.parse(register.options.body).machine).toBe("box-a");
    neverBootstraps(h);
  } finally { await h.stop(); }
});

test("SUPERVISOR_URL/SUPERVISOR_TOKEN win over the connection file, as before", async () => {
  writeConnection({ url: "https://file.example.test", token: "file-token" });
  process.env.SUPERVISOR_URL = "https://env.example.test"; process.env.SUPERVISOR_TOKEN = "env-token";
  const h = harness();
  try {
    await h.start();
    const register = h.calls.find(call => call.path === "/api/agents/register");
    expect(register.url).toBe("https://env.example.test/api/agents/register");
    expect(register.options.headers.Authorization).toBe("Bearer env-token");
    neverBootstraps(h);
  } finally { await h.stop(); }
});

test.if(Boolean(DEFAULT_HUB_URL))("SUPERVISOR_URL naming 127.0.0.1:18200 is a client, not a hub to bootstrap", async () => {
  process.env.SUPERVISOR_URL = "http://127.0.0.1:18200"; process.env.SUPERVISOR_TOKEN = "env-token";
  const h = harness();
  try {
    await h.start();
    expect(h.calls.some(call => /github\.com/.test(call.url))).toBe(false);
    expect(h.calls.filter(call => call.path === "/health")).toHaveLength(0);
    expect(existsSync(join(home, ".omp", "supervisor"))).toBe(false);
    expect(await h.run("status")).toContain("mode: client");
  } finally { await h.stop(); }
});

test.if(Boolean(DEFAULT_HUB_URL))("no configuration at all diagnoses the missing token instead of bootstrapping", async () => {
  const h = harness();
  try {
    await h.start();
    expect(h.calls).toHaveLength(0);
    expect(h.notifications.at(-1).text).toMatch(/SUPERVISOR_TOKEN/);
    neverBootstraps(h);
  } finally { await h.stop(); }
});

test.if(!DEFAULT_HUB_URL)("without a default hub, no configuration means the local hub and its bootstrap", async () => {
  const h = harness({ healthy: false });
  try {
    await h.start();
    expect(h.calls[0].url).toBe("http://127.0.0.1:18200/health");
    const sums = h.calls.find(call => call.path.endsWith("/SHA256SUMS"));
    expect(sums.url).toMatch(/^https:\/\/github\.com\/[^/]+\/[^/]+\/releases\/download\/v[0-9.]+\/SHA256SUMS$/);
    expect(h.notifications.at(-1).text).toMatch(/SUPERVISOR_TOKEN/);
    const status = await h.run("status");
    expect(status).toContain("mode: hub-local");
    expect(status).toContain("url: http://127.0.0.1:18200 (local hub)");
    expect(status).not.toContain("bootstrap: off");
  } finally { await h.stop(); }
});

test.if(!DEFAULT_HUB_URL)("without a default hub, SUPERVISOR_URL naming another host is a client and never bootstraps", async () => {
  process.env.SUPERVISOR_URL = "https://hub.example.test"; process.env.SUPERVISOR_TOKEN = "env-token";
  const h = harness({ healthy: false });
  try {
    await h.start();
    neverBootstraps(h);
    expect(h.calls.find(call => call.path === "/api/agents/register").url).toBe("https://hub.example.test/api/agents/register");
    expect(await h.run("status")).toContain("bootstrap: off");
  } finally { await h.stop(); }
});

test("the loose copy does not export the token into process.env", async () => {
  writeConnection({ url: "https://hub.example.test", token: "conn-token" });
  const h = harness();
  try {
    await h.start();
    expect(process.env.SUPERVISOR_URL).toBeUndefined();
    expect(process.env.SUPERVISOR_TOKEN).toBeUndefined();
  } finally { await h.stop(); }
});

test("a native remote tool keeps the plugin tool and tracker off", async () => {
  writeConnection({ url: "https://hub.example.test", token: "conn-token" });
  const h = harness({ nativeRemote: true });
  try {
    await h.start();
    expect(h.tools).toEqual([]);
    expect(h.entriesRead).toHaveLength(0);
    expect(h.widgets).toHaveLength(0);
    const status = await h.run("status");
    expect(status).toContain("remote tool: native remote tool");
    expect(status).toContain("job tracking: native omp tracker (plugin tracker off)");
    expect(status).toContain("bootstrap: off");
  } finally { await h.stop(); }
});

test("without a native remote the plugin registers its own tool", async () => {
  writeConnection({ url: "https://hub.example.test", token: "conn-token" });
  const h = harness({ nativeRemote: false });
  try {
    await h.start();
    expect(h.tools).toEqual(["remote"]);
    expect(await h.run("status")).toContain("remote tool: registered by this plugin");
  } finally { await h.stop(); }
});

test("hub and target refuse on a client and change nothing", async () => {
  writeConnection({ url: "https://hub.example.test", token: "conn-token" });
  const h = harness();
  try {
    await h.start();
    for (const args of ["hub 0.0.0.0", "target add box box.example", "target list", "target remove box"]) {
      const out = await h.run(args);
      expect(out).toContain("is a hub-admin command");
      expect(out).toContain("client of https://hub.example.test");
    }
    expect(existsSync(join(home, ".omp", "supervisor"))).toBe(false);
  } finally { await h.stop(); }
});

test("MCP calls follow omp's supervisor.mcpUrl without sending the registry token there", async () => {
  writeConnection({ url: "https://hub.example.test", token: "conn-token" });
  const jobs = [
    { job_id: "sub_mine", status: "running", machine: "box-a", parent_session_id: "sess-converge", root_session_id: "sess-converge", start_time: new Date().toISOString() },
    { job_id: "sub_other", status: "done", machine: "box-b", parent_session_id: "someone-else", root_session_id: "someone-else" },
    { job_id: "sub_child", status: "done", machine: "mac", parent_session_id: "sess-sub", root_session_id: "sess-converge" },
  ];
  const h = harness({ settings: { supervisor: { mcpUrl: "http://127.0.0.1:18200/mcp" } }, jobs });
  try {
    await h.start();
    const out = await h.run("jobs");
    expect(out).toContain("sub_mine on box-a: running");
    expect(out).toContain("sub_child on mac: done");
    expect(out).not.toContain("sub_other");
    const mcp = h.calls.filter(call => call.path === "/mcp");
    expect(mcp.length).toBeGreaterThan(0);
    for (const call of mcp) {
      expect(call.url).toBe("http://127.0.0.1:18200/mcp");
      expect(call.options.headers.Authorization).toBeUndefined();
    }
    expect(await h.run("status")).toContain("mcp: http://127.0.0.1:18200/mcp (omp setting supervisor.mcpUrl)");
  } finally { await h.stop(); }
});

test("MCP on the hub origin carries the bearer when no separate endpoint is set", async () => {
  writeConnection({ url: "https://hub.example.test", token: "conn-token" });
  const h = harness();
  try {
    await h.start();
    await h.run("jobs");
    const mcp = h.calls.find(call => call.path === "/mcp");
    expect(mcp.url).toBe("https://hub.example.test/mcp");
    expect(mcp.options.headers.Authorization).toBe("Bearer conn-token");
  } finally { await h.stop(); }
});
