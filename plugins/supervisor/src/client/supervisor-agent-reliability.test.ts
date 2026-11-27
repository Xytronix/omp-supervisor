import { test, expect, beforeEach, afterEach, spyOn } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import os, { hostname, tmpdir } from "node:os";
import { join } from "node:path";
import supervisorAgent from "./supervisor-agent.extension.js";

const originalFetch = globalThis.fetch;
const originalTimeout = globalThis.setTimeout;
const envKeys = ["HOME", "USERPROFILE", "SUPERVISOR_URL", "SUPERVISOR_TOKEN", "SUPERVISOR_AGENT_FORCE", "SUPERVISOR_MACHINE", "SUPERVISOR_BOOTSTRAP"];
let oldEnv, home, homedirSpy;
beforeEach(() => {
  oldEnv = Object.fromEntries(envKeys.map(key => [key, process.env[key]]));
  home = mkdtempSync(join(tmpdir(), "supervisor-test-"));
  // Pin the actual lookup so ambient per-user configuration cannot leak in.
  homedirSpy = spyOn(os, "homedir").mockReturnValue(home);
  process.env.HOME = home; process.env.USERPROFILE = home;
  process.env.SUPERVISOR_URL = "http://127.0.0.1:18200";
  process.env.SUPERVISOR_TOKEN = "test-enrollment";
  delete process.env.SUPERVISOR_AGENT_FORCE;
  // Delivery tests: a build without a default hub must not probe or start a local service here.
  process.env.SUPERVISOR_BOOTSTRAP = "0";
});
afterEach(() => {
  globalThis.fetch = originalFetch; globalThis.setTimeout = originalTimeout;
  homedirSpy.mockRestore();
  for (const key of envKeys) if (oldEnv[key] === undefined) delete process.env[key]; else process.env[key] = oldEnv[key];
  rmSync(home, { recursive: true, force: true });
});

function harness({ idle = true, deliveries = [], registerError = null, finish = true, ackStatuses = [] } = {}) {
  const hooks = new Map(), timers = new Map(), calls = [], injected = [], acks = [], notifications = [], warnings = [];
  let next = 0, attempts = 0;
  const end = text => hooks.get("agent_end")?.({ messages: [{ role: "assistant", content: text }] });
  const ctx = {
    hasUI: true, cwd: "/test", isIdle: () => idle,
    sessionManager: { getSessionId: () => "reliability-session" },
    ui: { notify: (text, level) => notifications.push({ text, level }) },
    setTimeout: (fn, delay) => { const id = ++next; timers.set(id, { fn, delay, interval: false }); return id; },
    setInterval: (fn, delay) => { const id = ++next; timers.set(id, { fn, delay, interval: true }); return id; },
    clearTimer: id => timers.delete(id),
  };
  const pi = {
    on: (event, fn) => hooks.set(event, fn), getSessionName: () => "smoke", registerCommand() {},
    logger: { warn: (message, meta) => warnings.push({ message, meta }) },
    sendUserMessage: (text, options) => { injected.push({ text, options }); if (finish) end("reply:" + text); },
    sendMessage: (message, options) => { injected.push({ message, options }); if (finish) end("reply:" + message.details.message); },
  };
  globalThis.fetch = async (url, options) => {
    const path = new URL(url).pathname;
    calls.push({ path, options });
    if (path.endsWith("/register")) {
      attempts++;
      if (registerError && attempts === 1) return Response.json({ error: "test failure", code: registerError.code }, { status: registerError.status });
      return Response.json({ agent_id: "agt_test", agent_token: "test-session", heartbeat_interval_seconds: 30 });
    }
    if (path.endsWith("/inbox")) return Response.json({ deliveries: typeof deliveries === "function" ? deliveries() : deliveries });
    if (path.endsWith("/ack")) {
      acks.push(JSON.parse(options.body));
      const status = ackStatuses.shift();
      if (status && status !== 200) return Response.json({ error: "ack failure", code: "test" }, { status });
    }
    return Response.json({ ok: true });
  };
  supervisorAgent(pi);
  return {
    hooks, timers, calls, injected, acks, notifications, warnings, ctx, end,
    attempts: () => attempts,
    start: () => hooks.get("session_start")({}, ctx),
    stop: () => hooks.get("session_shutdown")(),
    next: async () => {
      const entry = [...timers].find(([, value]) => !value.interval);
      if (!entry) throw new Error("No retry/poll timer scheduled");
      timers.delete(entry[0]); await entry[1].fn();
    },
  };
}

test("registration and heartbeat keep execution alias separate from actual host identity", async () => {
  process.env.SUPERVISOR_MACHINE = `launch-alias:${hostname()}`;
  const h = harness();
  try {
    await h.start();
    const registration = JSON.parse(h.calls.find(call => call.path.endsWith("/register")).options.body);
    const actualHost = { hostname: hostname(), platform: process.platform, architecture: process.arch };
    expect(registration.machine).toBe(process.env.SUPERVISOR_MACHINE);
    expect(registration.host).toEqual(actualHost);
    expect(registration.host.hostname).not.toBe(registration.machine);
    const [, heartbeat] = [...h.timers].find(([, value]) => value.interval);
    await heartbeat.fn();
    const update = JSON.parse(h.calls.find(call => call.path.endsWith("/heartbeat")).options.body);
    expect(update.host).toEqual(actualHost);
    expect(update).not.toHaveProperty("machine");
  } finally { await h.stop(); }
});

test("idle human follow-up starts a turn without an explicit queue-only deliverAs", async () => {
  const h = harness({ deliveries: [{ id: "d1", text: "work", mode: "follow_up" }] });
  try { await h.start(); await h.next(); expect(h.injected[0].options?.deliverAs).toBeUndefined(); expect(h.acks[0].result).toBe("reply:work"); }
  finally { await h.stop(); }
});
test("busy human follow-up retains queue semantics", async () => {
  const h = harness({ idle: false, deliveries: [{ id: "d1", text: "work", mode: "follow_up" }] });
  try { await h.start(); await h.next(); expect(h.injected[0].options).toEqual({ deliverAs: "followUp" }); }
  finally { await h.stop(); }
});
test("idle peer delivery starts a custom agent-attributed turn", async () => {
  const h = harness({ deliveries: [{ id: "d1", text: "work", mode: "follow_up", source: { kind: "agent", from: "peer" } }] });
  try { await h.start(); await h.next(); expect(h.injected[0].message.attribution).toBe("agent"); expect(h.injected[0].options).toEqual({ triggerTurn: true }); }
  finally { await h.stop(); }
});

for (const [mode, deliverAs] of [["steer", "steer"], ["aside", "aside"], ["follow_up", "followUp"]]) {
  test(`busy peer ${mode} passes explicit mode and presentation to OMP`, async () => {
    const fromPeer = { name: "Researcher", title: "Observed task", route: "supervisor" };
    const h = harness({ idle: false, deliveries: [{ id: "d1", text: "peer input", mode, source: { kind: "agent", from: "sub_worker", ...fromPeer } }] });
    try {
      await h.start(); await h.next();
      const delivery = h.injected[0];
      expect(delivery.options).toEqual({ deliverAs, triggerTurn: true });
      expect(delivery.message.attribution).toBe("agent");
      expect(delivery.message.details).toEqual({ id: "d1", from: "sub_worker", message: "peer input", fromPeer });
    } finally { await h.stop(); }
  });
}

for (const author of ["human", "agent"]) {
  test(`${author} aside retains its mode when an idle snapshot becomes busy before injection`, async () => {
    const source = author === "agent" ? { kind: "agent", from: "sub_worker" } : undefined;
    const h = harness({ finish: false, deliveries: [{ id: "d1", text: "non-interrupting input", mode: "aside", source }] });
    let busy = false;
    h.ctx.isIdle = () => {
      const idle = !busy;
      busy = true;
      return idle;
    };
    try {
      await h.start();
      const poll = h.next();
      for (let i = 0; i < 20 && !h.injected.length; i++) await Promise.resolve();
      expect(busy).toBe(true);
      expect(h.injected).toHaveLength(1);
      expect(h.injected[0].options).toEqual(author === "agent"
        ? { deliverAs: "aside", triggerTurn: true }
        : { deliverAs: "aside" });
      if (author === "agent") {
        // A peer message is a notification: acked at injection with no result,
        // and the turn's text never becomes its reply.
        await poll;
        expect(h.acks).toEqual([{ delivery_id: "d1", status: "ok", result: "", error: "" }]);
        h.end("turn completed");
        await poll;
        expect(h.acks).toHaveLength(1);
      } else {
        expect(h.acks).toEqual([]);
        h.end("turn completed");
        await poll;
        for (let i = 0; i < 20 && !h.acks.length; i++) await Promise.resolve();
        expect(h.acks).toEqual([{ delivery_id: "d1", status: "ok", result: "turn completed", error: "" }]);
      }
    } finally { await h.stop(); }
  });
}
test("transient initial registration failure retries without restarting OMP", async () => {
  const h = harness({ registerError: { status: 503, code: "unavailable" } });
  try { await h.start(); expect(h.attempts()).toBe(1); expect(h.timers.size).toBe(1); await h.next(); expect(h.attempts()).toBe(2); expect(h.timers.size).toBe(2); }
  finally { await h.stop(); }
});
test("authentication rejection is actionable and is not retried endlessly", async () => {
  const h = harness({ registerError: { status: 401, code: "unauthorized" } });
  try { await h.start(); expect(h.timers.size).toBe(0); expect(h.notifications.at(-1).text).toContain("enrollment"); expect(h.notifications.at(-1).text).not.toContain("test-enrollment"); }
  finally { await h.stop(); }
});
test("missing enrollment settings are diagnosed instead of silently ignored", async () => {
  delete process.env.SUPERVISOR_URL; delete process.env.SUPERVISOR_TOKEN;
  const h = harness();
  try {
    await h.start();
    expect(h.calls).toHaveLength(0);
    expect(h.timers.size).toBe(0);
    expect(h.notifications).toHaveLength(1);
    expect(h.notifications[0].level).toBe("warning");
    expect(h.notifications[0].text).toMatch(/enrollment/i);
    expect(h.notifications[0].text).toMatch(/url/i);
    expect(h.notifications[0].text).toMatch(/token|credential/i);
  }
  finally { await h.stop(); }
});
test("configuration is re-read at each session start", async () => {
  const h = harness(); process.env.SUPERVISOR_TOKEN = "updated-test-enrollment";
  try { await h.start(); expect(h.calls[0].options.headers.Authorization).toBe("Bearer updated-test-enrollment"); }
  finally { await h.stop(); }
});
test("shutdown while registration is pending cannot create orphan timers", async () => {
  const h = harness(); let release, entered;
  const enteredPromise = new Promise(resolve => { entered = resolve; });
  const oldFetch = globalThis.fetch;
  globalThis.fetch = async (url, options) => {
    if (new URL(url).pathname.endsWith("/register")) { entered(); return await new Promise(resolve => { release = resolve; }); }
    return oldFetch(url, options);
  };
  const starting = h.start(); await enteredPromise; await h.stop();
  release(Response.json({ agent_id: "agt_late", agent_token: "late-test-token" })); await starting;
  try { expect(h.timers.size).toBe(0); expect(h.calls.some(call => call.path === "/api/agents/agt_late/unregister")).toBe(true); }
  finally { await h.stop(); }
});
test("reply deadline reports an error, never a stale successful response", async () => {
  let expire;
  globalThis.setTimeout = (fn, ms, ...args) => ms === 1800000 ? (expire = fn, 123456789) : originalTimeout(fn, ms, ...args);
  const h = harness({ finish: false, deliveries: [{ id: "d1", text: "work", mode: "steer" }] });
  try {
    await h.start(); const poll = h.next();
    for (let i = 0; i < 20 && !expire; i++) await Promise.resolve();
    expect(typeof expire).toBe("function"); expire(); await poll;
    expect(h.acks[0].status).toBe("error"); expect(h.acks[0].result).toBe(""); expect(h.acks[0].error).toContain("Timed out");
  } finally { await h.stop(); }
});
test("aborted assistant turn is not acknowledged as a successful answer", async () => {
  const h = harness({ finish: false, deliveries: [{ id: "d1", text: "work", mode: "steer" }] });
  try {
    await h.start(); const poll = h.next();
    for (let i = 0; i < 20 && !h.injected.length; i++) await Promise.resolve();
    h.hooks.get("agent_end")({ messages: [{ role: "assistant", stopReason: "aborted", content: "partial" }] });
    await poll; expect(h.acks[0].status).toBe("error"); expect(h.acks[0].result).toBe("");
  } finally { await h.stop(); }
});

const settle = async (predicate, rounds = 200) => {
  for (let i = 0; i < rounds && !predicate(); i++) await Bun.sleep(2);
};
const peer = (id, text, extra = {}) => ({ id, text, mode: "steer", source: { kind: "agent", from: "sub_peer" }, ...extra });

test("peer messages queued together are injected back-to-back in order and acked without waiting for a turn", async () => {
  const created = "2026-11-15T10:00:00Z";
  const h = harness({ idle: false, finish: false, deliveries: [peer("p1", "first", { created_at: created }), peer("p2", "second")] });
  try {
    await h.start(); await h.next();
    expect(h.injected.map(entry => entry.message.details.id)).toEqual(["p1", "p2"]);
    expect(h.injected[0].message.details.sentAt).toBe(Date.parse(created));
    expect(h.injected[1].message.details).not.toHaveProperty("sentAt");
    expect(h.acks).toEqual([
      { delivery_id: "p1", status: "ok", result: "", error: "" },
      { delivery_id: "p2", status: "ok", result: "", error: "" },
    ]);
    h.end("unrelated assistant text");
    for (let i = 0; i < 20; i++) await Promise.resolve();
    expect(h.acks).toHaveLength(2);
  } finally { await h.stop(); }
});

test("overlapping human prompts run one at a time and each gets its own reply", async () => {
  let batch = [{ id: "h1", text: "first prompt", mode: "follow_up" }, { id: "h2", text: "second prompt", mode: "follow_up" }];
  const h = harness({ idle: false, finish: false, deliveries: () => { const out = batch; batch = []; return out; } });
  try {
    await h.start(); await h.next();
    expect(h.injected.map(entry => entry.text)).toEqual(["first prompt"]);
    // A peer message is not held behind the pending human reply.
    batch = [peer("p3", "while waiting")];
    await h.next();
    expect(h.injected.map(entry => entry.text ?? entry.message.details.id)).toEqual(["first prompt", "p3"]);
    expect(h.acks).toEqual([{ delivery_id: "p3", status: "ok", result: "", error: "" }]);
    h.end("answer to first");
    await settle(() => h.injected.length === 3);
    expect(h.injected[2].text).toBe("second prompt");
    await settle(() => h.acks.length === 2);
    expect(h.acks[1]).toEqual({ delivery_id: "h1", status: "ok", result: "answer to first", error: "" });
    h.end("answer to second");
    await settle(() => h.acks.length === 3);
    expect(h.acks[2]).toEqual({ delivery_id: "h2", status: "ok", result: "answer to second", error: "" });
  } finally { await h.stop(); }
});

test("a turn started by the first delivery claims the session before the next is injected", async () => {
  const h = harness({ finish: false, deliveries: [{ id: "h1", text: "human first", mode: "steer" }, peer("p2", "peer second")] });
  let claimed = false, claimedAtSecond;
  h.ctx.isIdle = () => !claimed;
  const record = h.injected.push.bind(h.injected);
  h.injected.push = entry => {
    // The session claims the turn asynchronously, after the injection call returns.
    if (entry.text === "human first") originalTimeout(() => { claimed = true; }, 20);
    else claimedAtSecond = claimed;
    return record(entry);
  };
  try {
    await h.start(); await h.next();
    expect(h.injected.map(entry => entry.text ?? entry.message.details.id)).toEqual(["human first", "p2"]);
    expect(claimedAtSecond).toBe(true);
    expect(h.injected[0].options).toBeUndefined();
    expect(h.injected[1].options).toEqual({ deliverAs: "steer", triggerTurn: true });
  } finally { await h.stop(); }
});

test("a transient ack failure is retried until it lands", async () => {
  globalThis.setTimeout = (fn, ms, ...args) => originalTimeout(fn, ms >= 1000 && ms < 1800000 ? 0 : ms, ...args);
  const h = harness({ idle: false, finish: false, ackStatuses: [503, 200], deliveries: [peer("p1", "one")] });
  try {
    await h.start(); await h.next();
    expect(h.acks.map(ack => ack.delivery_id)).toEqual(["p1", "p1"]);
    expect(h.warnings).toEqual([]);
    expect(h.injected).toHaveLength(1);
  } finally { await h.stop(); }
});

test("a final ack failure is logged instead of swallowed", async () => {
  const h = harness({ idle: false, finish: false, ackStatuses: [404], deliveries: [peer("p1", "one")] });
  try {
    await h.start(); await h.next();
    expect(h.acks).toHaveLength(1);
    expect(h.warnings).toHaveLength(1);
    expect(h.warnings[0].meta).toMatchObject({ deliveryId: "p1", status: 404, attempts: 1 });
  } finally { await h.stop(); }
});

test("turn start and end push busy/idle heartbeats with the turn end time", async () => {
  const h = harness({ idle: false });
  try {
    await h.start();
    const beats = () => h.calls.filter(call => call.path.endsWith("/heartbeat")).map(call => JSON.parse(call.options.body));
    h.hooks.get("agent_start")({ type: "agent_start" });
    expect(beats().at(-1).busy).toBe(true);
    h.hooks.get("agent_end")({ type: "agent_end", willContinue: true, messages: [] });
    expect(beats().length).toBe(1);
    const before = Date.now();
    // The session still reads busy while the prompt unwinds; the turn end wins.
    h.hooks.get("agent_end")({ type: "agent_end", messages: [] });
    const end = beats().at(-1);
    expect(end.busy).toBe(false);
    expect(Date.parse(end.turn_ended_at)).toBeGreaterThanOrEqual(before);
  } finally { await h.stop(); }
});
