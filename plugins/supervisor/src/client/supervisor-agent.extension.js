// Interactive OMP registration and authenticated Supervisor inbox delivery.
// Install as ~/.omp/agent/extensions/supervisor-agent.js. Configuration is read
// at every session start from supervisor-connection.json or SUPERVISOR_URL/TOKEN.
// Missing credentials are diagnosed, never manufactured or bypassed.
import os from "node:os";
import { readFileSync } from "node:fs";
import path from "node:path";

const POLL_WAIT_SECONDS = 25;
const ASSISTANT_REPLY_TIMEOUT_MS = 30 * 60 * 1000;
const MAX_DELIVERY_CACHE = 256;
// A turn this bridge just started gets this long to claim the session before
// the next delivery is injected, so a later message cannot win the turn.
const TURN_CLAIM_WAIT_MS = 250;
// The server never redelivers a claimed delivery, so a lost ack is final.
const ACK_ATTEMPTS = 4;
// After a terminal agent_end the session can still read busy while its prompt
// unwinds; the immediate turn-end heartbeat reports idle for this long.
const TURN_END_GRACE_MS = 2000;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function lastAssistant(messages) {
  if (!Array.isArray(messages)) return undefined;
  return [...messages].reverse().find(message => message?.role === "assistant");
}
function assistantText(message) {
  if (typeof message?.content === "string") return message.content;
  if (!Array.isArray(message?.content)) return "";
  return message.content.filter(part => part?.type === "text" && part.text?.trim()).map(part => part.text).join(String.fromCharCode(10));
}
function retryDelay(attempt, retryAfterMs = 0) {
  return Math.max(retryAfterMs, Math.min(60000, 1000 * 2 ** Math.min(attempt, 6)) + Math.floor(Math.random() * 1000));
}
function permanentRegistrationError(error) {
  return [400, 401, 403, 404, 405, 410].includes(error?.status);
}
// Supervisor delivery tools mounted directly as an OMP MCP server.
const SUPERVISOR_DELIVERY_TOOL = /^mcp__(supervisor|subagent)(?:_[a-z0-9]+)*__(prompt|steer|message)$/i;
// Gateway code mode (Bifrost executeToolCode): Starlark source calling Supervisor.prompt(...).
const GATEWAY_CODE_TOOL = /^mcp__[a-z0-9_-]+__executetoolcode$/i;
const GATEWAY_DELIVERY_CALL = /\b(Supervisor|Subagent)\.(prompt|steer|message)\(\s*/g;
const GATEWAY_SENDER_ARG = /\bsender_session_id\s*=\s*(?:"[^"\n]*"|'[^'\n]*')\s*,?\s*/g;
// Rewrites every Supervisor delivery call to carry the runtime session id,
// replacing any value the model wrote. Returns undefined when nothing changes.
function stampGatewayCode(code, sessionId) {
  if (typeof code !== "string" || !GATEWAY_DELIVERY_CALL.test(code)) return undefined;
  GATEWAY_DELIVERY_CALL.lastIndex = 0;
  const literal = JSON.stringify(sessionId);
  return code.replace(GATEWAY_SENDER_ARG, "").replace(GATEWAY_DELIVERY_CALL, (_m, server, tool) => `${server}.${tool}(sender_session_id=${literal}, `);
}
function deliveryTarget(input) {
  if (!input || typeof input !== "object") return "";
  for (const key of ["agent_id", "agent", "id", "session_id", "name", "job_id"]) {
    if (typeof input[key] === "string" && input[key].trim()) return input[key].trim();
  }
  return "";
}

function agentPromptSource(source) {
  if (source == null) return undefined;
  if (typeof source !== "object" || Array.isArray(source) || source.kind !== "agent" || typeof source.from !== "string" || !source.from.trim()) {
    throw new Error("Invalid agent message source");
  }
  const out = { kind: "agent", from: source.from };
  for (const key of ["name", "title", "route"]) {
    if (source[key] !== undefined) {
      if (typeof source[key] !== "string") throw new Error("Invalid agent message source");
      out[key] = source[key];
    }
  }
  if (out.route !== undefined && !["omp", "supervisor", "grok-webhook"].includes(out.route)) {
    throw new Error("Invalid agent message source");
  }
  return out;
}

export default function supervisorAgent(pi) {
  let stopCurrent = async () => {};
  let onAgentEnd = () => {};
  let onTurn = () => {};
  let selfIdentity = () => ({ agentId: "", sessionId: "" });
  let generation = 0;
  pi.on("agent_start", () => onTurn(true));
  pi.on("agent_end", event => {
    onAgentEnd(event);
    if (event?.willContinue !== true) onTurn(false);
  });
  // A registered session appears in Supervisor `agents` like any peer. Refuse a
  // delivery addressed to this session and stamp the runtime session id on the
  // rest so Supervisor can refuse aliases (names, prefixes) that resolve here.
  pi.on("tool_call", (event, ctx) => {
    const toolName = String(event?.toolName || "");
    const input = event?.input;
    if (!input || typeof input !== "object") return;
    const { agentId, sessionId: registeredSession } = selfIdentity();
    const sessionId = registeredSession || ctx?.sessionManager?.getSessionId?.() || "";
    if (GATEWAY_CODE_TOOL.test(toolName)) {
      // Supervisor refuses any target resolving to this session once stamped.
      const code = sessionId ? stampGatewayCode(input.code, sessionId) : undefined;
      return code === undefined || code === input.code ? undefined : { input: { ...input, code } };
    }
    if (!SUPERVISOR_DELIVERY_TOOL.test(toolName)) return;
    const target = deliveryTarget(input);
    if (!target) return;
    if ((agentId && target === agentId) || (sessionId && target === sessionId)) {
      return { block: true, reason: `Refused: ${target} is this session's own Supervisor registration; a session cannot prompt itself. Pick a different agent from agents.` };
    }
    // Runtime-owned: overwrite whatever the model supplied.
    if (sessionId && input.sender_session_id !== sessionId) return { input: { ...input, sender_session_id: sessionId } };
  });
  pi.on("session_shutdown", async () => { generation++; await stopCurrent(); });
  pi.on("session_start", async (_event, ctx) => {
    const currentGeneration = ++generation;
    await stopCurrent();
    if (generation !== currentGeneration) return;
    onAgentEnd = () => {};
    onTurn = () => {};
    if (!ctx.hasUI && process.env.SUPERVISOR_AGENT_FORCE !== "1") return;

    const configPath = path.join(os.homedir(), ".omp", "agent", "supervisor-connection.json");
    const notify = (text, level = "warn") => ctx.ui?.notify?.(text, level);
    let connection = {};
    try {
      const parsed = JSON.parse(readFileSync(configPath, "utf8"));
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("invalid config");
      connection = parsed;
    } catch (error) {
      if (error?.code !== "ENOENT" && !(process.env.SUPERVISOR_URL && process.env.SUPERVISOR_TOKEN)) {
        notify("Supervisor enrollment unavailable: cannot read valid supervisor-connection.json. Check its JSON and permissions; credentials were not printed.");
        return;
      }
    }
    const url = String(process.env.SUPERVISOR_URL || connection.url || "").replace(/\/+$/, "");
    const enrollToken = process.env.SUPERVISOR_TOKEN || connection.token || "";
    if (!url || typeof enrollToken !== "string" || !enrollToken.trim()) {
      notify("Supervisor enrollment missing: configure URL and enrollment token in ~/.omp/agent/supervisor-connection.json or SUPERVISOR_URL/SUPERVISOR_TOKEN, then resume the session.");
      return;
    }
    try {
      const parsed = new URL(url);
      if (!["https:", "http:"].includes(parsed.protocol) || parsed.username || parsed.password) throw new Error("invalid URL");
    } catch {
      notify("Supervisor enrollment URL is invalid: use an HTTP(S) URL without embedded credentials.");
      return;
    }
    const configuredTTL = Number(process.env.SUPERVISOR_TTL_SECONDS || 90);
    const ttlSeconds = Number.isFinite(configuredTTL) ? Math.max(15, Math.min(900, Math.floor(configuredTTL))) : 90;
    const machine = process.env.SUPERVISOR_MACHINE || connection.machine || os.hostname();
    const host = { hostname: os.hostname(), platform: process.platform, architecture: process.arch };
    const name = process.env.SUPERVISOR_AGENT_NAME || `${machine}:${path.basename(ctx.cwd || process.cwd())}:${(ctx.sessionManager?.getSessionId?.() || String(process.pid)).slice(-8)}`;
    const identity = () => {
      let model = "", title = "";
      try { const current = ctx.models?.current?.() || ctx.model; if (current) model = current.provider ? `${current.provider}/${current.id}` : current.id; } catch {}
      try { title = pi.getSessionName?.() || ""; } catch {}
      return { host, session_id: ctx.sessionManager?.getSessionId?.() || "", cwd: ctx.cwd || process.cwd(), model, title };
    };

    let stopped = false, registrationBlocked = false, registrationPromise = null;
    let agentId = "", agentToken = "", heartbeatSeconds = Math.max(5, Math.floor(ttlSeconds / 3));
    // Human/API prompts (no source) are request/reply: one runs at a time and
    // later ones wait in `humanQueue` until the active reply settles, so each
    // terminal agent_end answers exactly one prompt. Peer (agent) deliveries are
    // notifications: injected at once, in order, and acked with no result.
    const humanQueue = [], queuedHumanIds = new Set();
    let activeReply = null, injectChain = Promise.resolve();
    let startedLoops = false;
    const timers = new Set(), outcomes = new Map(), controller = new AbortController();
    selfIdentity = () => ({ agentId: stopped ? "" : agentId, sessionId: ctx.sessionManager?.getSessionId?.() || "" });
    const schedule = (fn, delay) => {
      if (stopped || registrationBlocked) return;
      const timer = ctx.setTimeout(async () => { timers.delete(timer); if (!stopped && !registrationBlocked) await fn(); }, delay);
      timers.add(timer);
    };
    const api = async (method, urlPath, token, body) => {
      const res = await fetch(`${url}${urlPath}`, {
        method, redirect: "error",
        signal: urlPath.endsWith("/unregister") ? AbortSignal.timeout(2000) : AbortSignal.any([controller.signal, AbortSignal.timeout(35000)]),
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      let payload = {};
      try { payload = await res.json(); } catch {}
      if (!res.ok) {
        // Never echo remote error text: proxies may include request credentials.
        const error = new Error(`Supervisor request failed (HTTP ${res.status})`);
        error.status = res.status;
        error.code = payload.code;
        const retry = res.headers.get("Retry-After");
        const seconds = retry === null ? NaN : Number(retry);
        error.retryAfterMs = Number.isFinite(seconds) ? Math.max(0, seconds * 1000) : Math.max(0, Date.parse(retry || "") - Date.now()) || 0;
        throw error;
      }
      return payload;
    };
    const unregister = async (id, token) => {
      if (!id) return;
      try { await api("POST", `/api/agents/${encodeURIComponent(id)}/unregister`, token); } catch {}
    };
    // Install cleanup BEFORE the first network await. A late registration is
    // unregistered below rather than creating timers after shutdown/session switch.
    stopCurrent = async () => {
      if (stopped) return;
      stopped = true; controller.abort();
      for (const timer of timers) ctx.clearTimer(timer);
      timers.clear();
      activeReply?.finish(null);
      humanQueue.length = 0; queuedHumanIds.clear();
      await unregister(agentId, agentToken);
    };
    const register = async () => {
      if (stopped || registrationBlocked) return;
      if (registrationPromise) return registrationPromise;
      registrationPromise = (async () => {
        const result = await api("POST", "/api/agents/register", enrollToken, {
          name, machine, harness: "omp-tty", version: process.env.OMP_VERSION || "", pid: process.pid,
          capabilities: ["prompt", "steer", "aside", "follow_up"],
          labels: { platform: process.platform, surface: "interactive", client_protocol: "reply-ack-v2" },
          ttl_seconds: ttlSeconds, ...identity(),
        });
        if (typeof result.agent_id !== "string" || !result.agent_id || typeof result.agent_token !== "string" || !result.agent_token) throw new Error("Invalid registration response");
        if (stopped || generation !== currentGeneration) { await unregister(result.agent_id, result.agent_token); return; }
        agentId = result.agent_id; agentToken = result.agent_token;
        const interval = Number(result.heartbeat_interval_seconds);
        if (Number.isFinite(interval) && interval > 0) heartbeatSeconds = interval;
        notify(`Supervisor: advertised ${name} (${agentId})`, "info");
      })();
      try { await registrationPromise; }
      finally { registrationPromise = null; }
    };
    const handleRegistrationFailure = error => {
      if (stopped) return;
      if (permanentRegistrationError(error)) {
        registrationBlocked = true;
        notify(`Supervisor enrollment rejected (HTTP ${error.status}). Check the configured enrollment credential, URL and server allowlist; automatic authentication retries stopped.`);
      } else notify("Supervisor temporarily unavailable; registration will retry with backoff.");
    };
    const recoverRegistration = async (error, failedId) => {
      if (stopped) return;
      if (error?.code === "unknown_agent") {
        if (agentId === failedId) { agentId = ""; agentToken = ""; }
        if (!agentId) await register();
      } else if ([401, 403].includes(error?.status)) handleRegistrationFailure(error);
    };
    const waitForReply = deliveryId => {
      let resolve, timer;
      const promise = new Promise(done => { resolve = done; });
      const waiter = {
        id: deliveryId,
        // Assistant text seen since THIS prompt was injected; never an earlier run's.
        lastText: "",
        finish(outcome) {
          if (activeReply !== waiter) return;
          clearTimeout(timer); activeReply = null; resolve(outcome);
        },
      };
      activeReply = waiter;
      timer = setTimeout(() => waiter.finish({ status: "error", reply: "", error: "Timed out waiting for the assistant reply; completion is unverified." }), ASSISTANT_REPLY_TIMEOUT_MS);
      return { promise, cancel: () => waiter.finish(null) };
    };
    onAgentEnd = event => {
      const waiter = activeReply;
      if (stopped || !waiter) return;
      const message = lastAssistant(event?.messages), text = assistantText(message);
      if (text) waiter.lastText = text;
      if (event?.willContinue === true) return;
      if (message?.stopReason === "aborted" || message?.stopReason === "error" || message?.errorMessage) {
        waiter.finish({ status: "error", reply: "", error: "Assistant turn failed or was interrupted; completion is unverified." });
      } else waiter.finish({ status: "ok", reply: waiter.lastText || "(no assistant text)", error: "" });
    };
    const remember = (id, outcome) => {
      outcomes.set(id, outcome);
      if (outcomes.size > MAX_DELIVERY_CACHE) outcomes.delete(outcomes.keys().next().value);
    };
    // The server never requeues a claimed delivery, so retry transient failures
    // here; a final failure is logged because the sender will not see the outcome.
    const ack = async (delivery, outcome) => {
      for (let attempt = 1; !stopped; attempt++) {
        try {
          await api("POST", `/api/agents/${encodeURIComponent(agentId)}/ack`, agentToken, {
            delivery_id: delivery.id, status: outcome.status, result: outcome.status === "ok" ? outcome.reply : "", error: outcome.error,
          });
          return;
        } catch (error) {
          if (stopped) return;
          if (attempt >= ACK_ATTEMPTS || [400, 401, 403, 404].includes(error?.status)) {
            pi.logger?.warn?.("Supervisor ack failed; the sender will not see this delivery's outcome", {
              deliveryId: delivery.id, status: error?.status ?? null, code: error?.code ?? null, attempts: attempt,
            });
            return;
          }
          await sleep(retryDelay(attempt - 1, error?.retryAfterMs));
        }
      }
    };
    const sentAtMs = delivery => {
      for (const key of ["created_at", "sent_at"]) {
        const ms = typeof delivery[key] === "number" ? delivery[key] : Date.parse(delivery[key] ?? "");
        if (Number.isFinite(ms) && ms > 0) return ms;
      }
      return undefined;
    };
    // Injects one delivery synchronously and returns an immediate outcome
    // (peer messages, failures) or a reply promise (human/API prompts).
    const inject = (delivery, idle) => {
      const deliverAs = delivery.mode === "follow_up" ? "followUp" : delivery.mode === "aside" ? "aside" : "steer";
      const options = idle && deliverAs !== "aside" ? undefined : { deliverAs };
      let pending;
      try {
        const source = agentPromptSource(delivery.source);
        if (source) {
          const from = source.from;
          const fromPeer = {};
          for (const key of ["name", "title", "route"]) if (source[key] !== undefined) fromPeer[key] = source[key];
          const details = { id: delivery.id, from, message: delivery.text };
          const sentAt = sentAtMs(delivery);
          if (sentAt !== undefined) details.sentAt = sentAt;
          if (Object.keys(fromPeer).length > 0) details.fromPeer = fromPeer;
          pi.sendMessage({
            customType: "irc:incoming",
            content: `<irc>\nIncoming message from agent \`${from}\`:\n\n${delivery.text}\n</irc>`,
            display: true, attribution: "agent", details,
          }, options ? { ...options, triggerTurn: true } : { triggerTurn: true });
          // A peer message is a notification, not an RPC: the receiver answers
          // (if at all) with its own message. Ack now with no result.
          const outcome = { status: "ok", reply: "", error: "" };
          remember(delivery.id, outcome);
          return { outcome, injected: true };
        }
        // Steer/follow-up are queue-only when explicit; an idle aside starts
        // a turn and retains its non-interrupting mode if another turn starts.
        pending = waitForReply(delivery.id);
        Promise.resolve(pi.sendUserMessage(delivery.text, options)).catch(() => {
          if (activeReply?.id === delivery.id) activeReply.finish({ status: "error", reply: "", error: "Unable to inject Supervisor delivery." });
        });
        return { reply: pending.promise, injected: true };
      } catch (error) {
        pending?.cancel();
        const outcome = { status: "error", reply: "", error: error?.message === "Invalid agent message source" ? error.message : "Unable to inject Supervisor delivery." };
        remember(delivery.id, outcome);
        return { outcome };
      }
    };
    // All injections run through one chain so poll batches and queued human
    // prompts never interleave out of order.
    const serially = work => (injectChain = injectChain.then(work, work));
    // Injects one delivery, then lets a turn it started claim the session
    // before anything else is injected. Resolves to the pending ack (if the
    // outcome is immediate) without waiting for that ack.
    const injectOne = async delivery => {
      const idle = ctx.isIdle?.() ?? true;
      const result = inject(delivery, idle);
      if (result.reply) {
        void result.reply.then(outcome => {
          if (outcome == null || stopped) return;
          remember(delivery.id, outcome);
          void serially(pumpHumanQueue);
          return ack(delivery, outcome);
        });
      }
      const pendingAck = result.outcome ? ack(delivery, result.outcome) : undefined;
      if (result.injected && idle) {
        for (let waited = 0; waited < TURN_CLAIM_WAIT_MS && !stopped && (ctx.isIdle?.() ?? false); waited += 5) await sleep(5);
      }
      return { pendingAck };
    };
    const pumpHumanQueue = async () => {
      while (!stopped && !activeReply && humanQueue.length > 0) {
        const delivery = humanQueue.shift();
        queuedHumanIds.delete(delivery.id);
        await injectOne(delivery);
      }
    };
    // Peer notifications are injected at once in send order. A human/API prompt
    // waits for any active prompt's reply first. Redeliveries re-ack only.
    const deliverAll = async deliveries => {
      const acks = await serially(async () => {
        const pending = [];
        for (const delivery of deliveries) {
          if (stopped) break;
          const cached = outcomes.get(delivery.id);
          if (cached) { pending.push(ack(delivery, cached)); continue; }
          if (delivery.source == null) {
            if (activeReply?.id === delivery.id || queuedHumanIds.has(delivery.id)) continue;
            if (activeReply || humanQueue.length > 0) {
              humanQueue.push(delivery); queuedHumanIds.add(delivery.id);
              continue;
            }
          }
          pending.push((await injectOne(delivery)).pendingAck);
        }
        return pending;
      });
      // Acks retry with backoff outside the chain so they never delay injection.
      await Promise.all(acks ?? []);
    };
    // Turn state: agent_start/agent_end push an immediate heartbeat so agents
    // shows busy/idle without waiting for the interval. isIdle stays the source
    // of truth, except just after a terminal agent_end, when the session may
    // still be unwinding the prompt and would briefly read as busy.
    let turnBusy = null, turnEndedAt = null, turnEndedMs = 0;
    const currentBusy = () => {
      if (turnBusy === false && Date.now() - turnEndedMs < TURN_END_GRACE_MS) return false;
      return !(ctx.isIdle?.() ?? true);
    };
    const heartbeat = async () => {
      if (stopped || registrationBlocked) return;
      const id = agentId;
      try {
        if (!id) { await register(); return; }
        const body = { ...identity(), busy: currentBusy(), ttl_seconds: ttlSeconds };
        if (turnEndedAt) body.turn_ended_at = turnEndedAt;
        await api("POST", `/api/agents/${encodeURIComponent(id)}/heartbeat`, agentToken, body);
      } catch (error) {
        try { await recoverRegistration(error, id); } catch (registrationError) { handleRegistrationFailure(registrationError); }
      }
    };
    onTurn = busy => {
      if (stopped) return;
      turnBusy = busy;
      if (!busy) { turnEndedMs = Date.now(); turnEndedAt = new Date(turnEndedMs).toISOString(); }
      if (agentId) void heartbeat();
    };
    let pollFailures = 0;
    const poll = async () => {
      if (stopped || registrationBlocked) return;
      let delay = 250;
      const id = agentId;
      try {
        if (!id) await register();
        if (stopped || !agentId) return;
        const result = await api("GET", `/api/agents/${encodeURIComponent(agentId)}/inbox?wait=${POLL_WAIT_SECONDS}`, agentToken);
        pollFailures = 0;
        await deliverAll(result.deliveries || []);
      } catch (error) {
        delay = retryDelay(pollFailures++, error.retryAfterMs);
        try { await recoverRegistration(error, id); } catch (registrationError) { handleRegistrationFailure(registrationError); }
      }
      schedule(poll, delay);
    };
    let registrationAttempts = 0;
    const start = async () => {
      if (stopped || registrationBlocked) return;
      try { await register(); }
      catch (error) { handleRegistrationFailure(error); schedule(start, retryDelay(registrationAttempts++, error.retryAfterMs)); return; }
      if (stopped || !agentId || startedLoops) return;
      startedLoops = true;
      timers.add(ctx.setInterval(heartbeat, heartbeatSeconds * 1000));
      schedule(poll, 250);
    };
    await start();
  });
}
