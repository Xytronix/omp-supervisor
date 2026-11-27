import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { closeSync, createReadStream, existsSync, mkdirSync, openSync, readdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
const POLL_WAIT_SECONDS = 25;
const ASSISTANT_REPLY_TIMEOUT_MS = 30 * 60 * 1000;
const MAX_DELIVERY_CACHE = 256;
const TURN_CLAIM_WAIT_MS = 250;
const ACK_ATTEMPTS = 4;
const TURN_END_GRACE_MS = 2000;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
function lastAssistant(messages) {
    if (!Array.isArray(messages))
        return undefined;
    return [...messages].reverse().find(message => message?.role === "assistant");
}
function assistantText(message) {
    if (typeof message?.content === "string")
        return message.content;
    if (!Array.isArray(message?.content))
        return "";
    return message.content.filter(part => part?.type === "text" && part.text?.trim()).map(part => part.text).join(String.fromCharCode(10));
}
function retryDelay(attempt, retryAfterMs = 0) {
    return Math.max(retryAfterMs, Math.min(60000, 1000 * 2 ** Math.min(attempt, 6)) + Math.floor(Math.random() * 1000));
}
function permanentRegistrationError(error) {
    return [400, 401, 403, 404, 405, 410].includes(error?.status);
}
const SUPERVISOR_DELIVERY_TOOL = /^mcp__(supervisor|subagent)(?:_[a-z0-9]+)*__(prompt|steer|message)$/i;
const GATEWAY_CODE_TOOL = /^mcp__[a-z0-9_-]+__executetoolcode$/i;
const GATEWAY_DELIVERY_CALL = /\b(Supervisor|Subagent)\.(prompt|steer|message)\(\s*/g;
const GATEWAY_SENDER_ARG = /\bsender_session_id\s*=\s*(?:"[^"\n]*"|'[^'\n]*')\s*,?\s*/g;
function stampGatewayCode(code, sessionId) {
    if (typeof code !== "string" || !GATEWAY_DELIVERY_CALL.test(code))
        return undefined;
    GATEWAY_DELIVERY_CALL.lastIndex = 0;
    const literal = JSON.stringify(sessionId);
    return code.replace(GATEWAY_SENDER_ARG, "").replace(GATEWAY_DELIVERY_CALL, (_m, server, tool) => `${server}.${tool}(sender_session_id=${literal}, `);
}
function deliveryTarget(input) {
    if (!input || typeof input !== "object")
        return "";
    for (const key of ["agent_id", "agent", "id", "session_id", "name", "job_id"]) {
        if (typeof input[key] === "string" && input[key].trim())
            return input[key].trim();
    }
    return "";
}
function agentPromptSource(source) {
    if (source == null)
        return undefined;
    if (typeof source !== "object" || Array.isArray(source) || source.kind !== "agent" || typeof source.from !== "string" || !source.from.trim()) {
        throw new Error("Invalid agent message source");
    }
    const out = { kind: "agent", from: source.from };
    for (const key of ["name", "title", "route"]) {
        if (source[key] !== undefined) {
            if (typeof source[key] !== "string")
                throw new Error("Invalid agent message source");
            out[key] = source[key];
        }
    }
    if (out.route !== undefined && !["omp", "supervisor", "grok-webhook"].includes(out.route)) {
        throw new Error("Invalid agent message source");
    }
    return out;
}

const SUPERVISOR_PLUGIN_VERSION = "0.3.1";
const SUPERVISOR_RELEASE_REPO = "Xytronix/omp-supervisor";
const SUPERVISOR_PORT = Number.isSafeInteger(Number(process.env.SUPERVISOR_PORT)) && Number(process.env.SUPERVISOR_PORT) > 0 ? Number(process.env.SUPERVISOR_PORT) : 18200;
const HOME_DIR = os.homedir();
const AGENT_DIR = path.join(HOME_DIR, ".omp", "agent");
const SUPERVISOR_HOME = path.join(HOME_DIR, ".omp", "supervisor");
const CONNECTION_PATH = path.join(AGENT_DIR, "supervisor-connection.json");
const HUB_CONFIG_PATH = path.join(SUPERVISOR_HOME, "plugin-hub.json");
const SERVICE_STATE_PATH = path.join(SUPERVISOR_HOME, "plugin-service.json");
const LOOSE_COPY_PATH = path.join(AGENT_DIR, "extensions", "supervisor-agent.js");
const RPC_HOST_SOURCE = fileURLToPath(new URL("../src/client/omp-rpc-host.mjs", import.meta.url));
const RESULT_MAX_CHARS = 8000;
const REMOTE_JOB_DEFAULT_MAX_TIME = "6h";
const JOB_ENTRY_TYPE = "supervisor-job";
const RESULT_MESSAGE_TYPE = "supervisor-result";
const COMMAND_MESSAGE_TYPE = "supervisor-command";
const WIDGET_KEY = "supervisor-remote-jobs";
const HOOK_POLL_MS = 3000;
const PUMP_MS = 2000;
const TRANSCRIPT_POLL_MS = 1500;
const EVENTS_PAGE_LIMIT = 200;
const PUMP_MAX_PAGES = 20;
const ARG_MAX_CHARS = 200;
const MESSAGE_MAX_CHARS = 8000;
const TOOL_RESULT_MAX_CHARS = 2000;
const LIVE_TEXT_MAX = 4000;
const DISPLAY_MAX_LINES = 5000;
const TRANSCRIPT_TEXT_LINES = 200;
const USER_ENV = (globalThis[Symbol.for("omp-supervisor.user-env")] ??= {
    url: String(process.env.SUPERVISOR_URL || "").trim(),
    token: String(process.env.SUPERVISOR_TOKEN || "").trim(),
});

const errorMessage = error => error instanceof Error ? error.message : String(error);
const trimSlash = value => String(value || "").trim().replace(/\/+$/, "");
const toPosix = value => String(value).split("\\").join("/");

function readJsonObject(file) {
    let text;
    try {
        text = readFileSync(file, "utf8");
    } catch (error) {
        if (error?.code === "ENOENT") return undefined;
        throw error;
    }
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error(`${file} is not a JSON object`);
    return parsed;
}

function writeJsonFile(file, value) {
    mkdirSync(path.dirname(file), { recursive: true });
    const tmp = `${file}.${process.pid}.tmp`;
    writeFileSync(tmp, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
    renameSync(tmp, file);
}

function dataDir() {
    return String(process.env.SUPERVISOR_DATA_DIR || "").trim() || SUPERVISOR_HOME;
}

function tokenFilePath() {
    return String(process.env.SUPERVISOR_REGISTRY_TOKEN_FILE || "").trim() || path.join(dataDir(), "registry-token");
}

function machinesFilePath() {
    return String(process.env.SUPERVISOR_REMOTE_MACHINES_FILE || "").trim() || path.join(SUPERVISOR_HOME, "machines.json");
}

function readLocalToken() {
    const fromEnv = String(process.env.SUPERVISOR_REGISTRY_TOKEN || "").split(",").map(part => part.trim()).find(Boolean);
    if (fromEnv) return fromEnv;
    try {
        return readFileSync(tokenFilePath(), "utf8").split(/\r?\n/)[0].trim();
    } catch {
        return "";
    }
}

function hubListenHost() {
    try {
        const host = readJsonObject(HUB_CONFIG_PATH)?.listenHost;
        if (typeof host === "string" && host.trim()) return host.trim();
    } catch { }
    return "127.0.0.1";
}

function isLoopbackHost(host) {
    const h = String(host || "").replace(/^\[|\]$/g, "").toLowerCase();
    return h === "localhost" || h === "::1" || /^127\./.test(h);
}

function urlForHost(host) {
    const h = host === "0.0.0.0" || host === "::" ? "127.0.0.1" : host;
    return `http://${h.includes(":") ? `[${h}]` : h}:${SUPERVISOR_PORT}`;
}

function localUrl() {
    return urlForHost(hubListenHost());
}

function readConnection() {
    try {
        return { connection: readJsonObject(CONNECTION_PATH) ?? {}, invalid: false };
    } catch {
        return { connection: {}, invalid: true };
    }
}

function resolveEndpoint() {
    const { connection, invalid } = readConnection();
    const local = localUrl();
    const configured = typeof connection.url === "string" ? trimSlash(connection.url) : "";
    const url = trimSlash(USER_ENV.url || configured || local);
    const mode = url === local ? "hub-local" : "client";
    let token = USER_ENV.token, tokenSource = token ? "SUPERVISOR_TOKEN" : "";
    if (!token && typeof connection.token === "string" && connection.token.trim()) {
        token = connection.token.trim();
        tokenSource = CONNECTION_PATH;
    }
    if (!token && mode === "hub-local") {
        token = readLocalToken();
        if (token) tokenSource = process.env.SUPERVISOR_REGISTRY_TOKEN ? "SUPERVISOR_REGISTRY_TOKEN" : tokenFilePath();
    }
    return { url, token, tokenSource, mode, local, connection, invalid };
}

function exportClientEnv() {
    const endpoint = resolveEndpoint();
    if (!USER_ENV.url) process.env.SUPERVISOR_URL = endpoint.url;
    if (!USER_ENV.token && endpoint.token) process.env.SUPERVISOR_TOKEN = endpoint.token;
}

function supervisorAssetName() {
    const osMap = { linux: "linux", darwin: "darwin", win32: "windows" };
    const archMap = { x64: "amd64", arm64: "arm64" };
    const osName = osMap[process.platform];
    const archName = archMap[process.arch];
    if (!osName || !archName) return "";
    return `supervisor-${osName}-${archName}${process.platform === "win32" ? ".exe" : ""}`;
}

function cachedBinaryName() {
    return `supervisor-${SUPERVISOR_PLUGIN_VERSION}${process.platform === "win32" ? ".exe" : ""}`;
}

async function supervisorHealthy(url = localUrl()) {
    try {
        const res = await fetch(`${url}/health`, { signal: AbortSignal.timeout(1500) });
        return res.ok;
    } catch {
        return false;
    }
}

function supervisorBinDir() {
    const dir = path.join(SUPERVISOR_HOME, "bin");
    mkdirSync(dir, { recursive: true });
    return dir;
}

async function sha256File(file) {
    return new Promise((resolve, reject) => {
        const hash = createHash("sha256");
        const stream = createReadStream(file);
        stream.on("data", chunk => hash.update(chunk));
        stream.on("end", () => resolve(hash.digest("hex")));
        stream.on("error", reject);
    });
}

async function ensureSupervisorBinary() {
    const asset = supervisorAssetName();
    if (!asset) return "";
    const dir = supervisorBinDir();
    const name = cachedBinaryName();
    const target = path.join(dir, name);
    if (existsSync(target)) return target;
    const base = `https://github.com/${SUPERVISOR_RELEASE_REPO}/releases/download/v${SUPERVISOR_PLUGIN_VERSION}`;
    const sumsRes = await fetch(`${base}/SHA256SUMS`, { signal: AbortSignal.timeout(15000) });
    if (!sumsRes.ok) throw new Error(`SHA256SUMS download failed: ${sumsRes.status}`);
    const sums = await sumsRes.text();
    const expected = sums.split(/\r?\n/).map(line => line.trim().split(/\s+/)).filter(parts => parts[1]?.replace(/^\*/, "") === asset)[0]?.[0];
    if (!expected) throw new Error(`no checksum for ${asset} in this release`);
    const tmp = `${target}.download`;
    const binRes = await fetch(`${base}/${asset}`, { signal: AbortSignal.timeout(120000) });
    if (!binRes.ok) throw new Error(`binary download failed: ${binRes.status}`);
    writeFileSync(tmp, Buffer.from(await binRes.arrayBuffer()), { mode: 0o700 });
    const actual = await sha256File(tmp);
    if (actual !== expected) {
        try { unlinkSync(tmp); } catch { }
        throw new Error(`checksum mismatch for ${asset}`);
    }
    renameSync(tmp, target);
    for (const entry of readdirSync(dir)) {
        if (entry.startsWith("supervisor-") && entry !== name && !entry.endsWith(".download")) {
            try { unlinkSync(path.join(dir, entry)); } catch { }
        }
    }
    return target;
}

function readServiceState() {
    try {
        const state = readJsonObject(SERVICE_STATE_PATH);
        return state && Number.isSafeInteger(state.pid) && state.pid > 0 ? state : undefined;
    } catch {
        return undefined;
    }
}

function processAlive(pid) {
    try {
        process.kill(pid, 0);
        return true;
    } catch (error) {
        return error?.code === "EPERM";
    }
}

function pluginOwnsProcess(state) {
    if (!state || !processAlive(state.pid)) return false;
    if (process.platform !== "linux") return true;
    try {
        return readFileSync(`/proc/${state.pid}/cmdline`, "utf8").split("\0")[0] === state.binary;
    } catch {
        return false;
    }
}

function spawnSupervisor(binary, listenHost) {
    mkdirSync(SUPERVISOR_HOME, { recursive: true });
    const log = openSync(path.join(SUPERVISOR_HOME, "plugin-service.log"), "a");
    const child = spawn(binary, [], {
        detached: true,
        windowsHide: true,
        stdio: ["ignore", log, log],
        cwd: SUPERVISOR_HOME,
        env: { ...process.env, HOME: process.env.HOME || HOME_DIR, FASTMCP_HOST: listenHost, FASTMCP_PORT: String(SUPERVISOR_PORT) },
    });
    child.unref();
    closeSync(log);
    writeJsonFile(SERVICE_STATE_PATH, { pid: child.pid, binary, listenHost, port: SUPERVISOR_PORT, startedAt: new Date().toISOString() });
    return child.pid;
}

async function waitHealthy(url, wantHealthy, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (await supervisorHealthy(url) === wantHealthy) return true;
        await sleep(250);
    }
    return false;
}

async function ensureSupervisorService() {
    const url = localUrl();
    if (await supervisorHealthy(url)) return true;
    let binary;
    try {
        binary = await ensureSupervisorBinary();
    } catch {
        return false;
    }
    if (!binary) return false;
    spawnSupervisor(binary, hubListenHost());
    return waitHealthy(url, true, 10000);
}

function writeConnectionConfig() {
    let existing = {};
    try {
        existing = readJsonObject(CONNECTION_PATH) ?? {};
    } catch { }
    const token = readLocalToken();
    const merged = {
        ...existing,
        url: typeof existing.url === "string" && existing.url.trim() ? existing.url : localUrl(),
        token: typeof existing.token === "string" && existing.token.trim() ? existing.token : token,
        machine: typeof existing.machine === "string" && existing.machine.trim() ? existing.machine : os.hostname(),
    };
    if (!merged.token) return false;
    if (!USER_ENV.url && !USER_ENV.token) writeJsonFile(CONNECTION_PATH, merged);
    return true;
}

function mcpResultText(result) {
    if (!result || typeof result !== "object" || !Array.isArray(result.content)) return "";
    return result.content.filter(block => block && typeof block.text === "string").map(block => block.text).join("\n");
}

function parseRecord(text) {
    try {
        const parsed = JSON.parse(text);
        return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : undefined;
    } catch {
        return undefined;
    }
}

async function callSupervisorTool(endpoint, name, args, { signal, timeoutMs = 30000 } = {}) {
    if (signal?.aborted) return { ok: false, error: "aborted" };
    const timeout = AbortSignal.timeout(timeoutMs);
    const combined = signal ? AbortSignal.any([signal, timeout]) : timeout;
    const headers = { "Content-Type": "application/json", Accept: "application/json, text/event-stream" };
    if (endpoint.token) headers.Authorization = `Bearer ${endpoint.token}`;
    let res, payload;
    try {
        res = await fetch(`${endpoint.url}/mcp`, {
            method: "POST",
            redirect: "error",
            signal: combined,
            headers,
            body: JSON.stringify({ jsonrpc: "2.0", id: `omp-plugin-${Date.now()}`, method: "tools/call", params: { name, arguments: args } }),
        });
        if (res.status === 401) return { ok: false, error: `HTTP 401: ${endpoint.url} requires its registry token; set SUPERVISOR_TOKEN to the hub's registry-token` };
        if (!res.ok) return { ok: false, error: `HTTP ${res.status}` };
        if ((res.headers.get("content-type") ?? "").includes("text/event-stream")) {
            for (const line of (await res.text()).split("\n")) {
                const trimmed = line.trim();
                if (!trimmed.startsWith("data:")) continue;
                const data = trimmed.slice(5).trim();
                if (!data || data === "[DONE]") continue;
                try {
                    payload = JSON.parse(data);
                } catch { }
            }
        } else {
            payload = await res.json();
        }
    } catch (error) {
        if (timeout.aborted) return { ok: false, error: `Supervisor ${name} timed out` };
        if (signal?.aborted) return { ok: false, error: "aborted" };
        return { ok: false, error: `${endpoint.url} unreachable: ${errorMessage(error)}` };
    }
    if (!payload || typeof payload !== "object") return { ok: false, error: "empty MCP response" };
    if (payload.error) return { ok: false, error: typeof payload.error.message === "string" ? payload.error.message : "MCP error" };
    const result = payload.result;
    if (result?.isError) return { ok: false, error: mcpResultText(result).trim() || `${name} rejected` };
    return { ok: true, result, text: mcpResultText(result) };
}

const NAME_STOP_WORDS = new Set("a an the and or then with for to of in on at by from into via using use run do please just that this it is are be after before when if so as your you my our also not no me will can should must reply write output return".split(" "));

function supervisorAgentName(requested, task) {
    const ident = typeof requested === "string" ? requested.trim() : "";
    if (/^[A-Za-z][A-Za-z0-9]{0,31}$/.test(ident) && !/^sub_/i.test(ident)) return ident.charAt(0).toUpperCase() + ident.slice(1);
    const source = ident || String(task || "");
    const tokens = source.split(/[^A-Za-z0-9]+/).filter(token => token && !/^\d+$/.test(token) && !NAME_STOP_WORDS.has(token.toLowerCase()));
    const preferred = tokens.filter(token => token.length >= 3);
    const chosen = (preferred.length > 0 ? preferred : tokens).slice(0, 2);
    const name = chosen.map(token => token.charAt(0).toUpperCase() + token.slice(1, 16).toLowerCase()).join("");
    return name || "RemoteJob";
}

function isTerminalStatus(status) {
    return typeof status === "string" && status !== "" && !["running", "queued", "pending", "starting"].includes(status);
}

function abortableSleep(ms, signal) {
    return new Promise(resolve => {
        if (signal?.aborted) return resolve();
        const timer = setTimeout(done, ms);
        function done() {
            clearTimeout(timer);
            signal?.removeEventListener("abort", done);
            resolve();
        }
        signal?.addEventListener("abort", done, { once: true });
    });
}

function elapsedLabel(ms) {
    const s = Math.max(0, Math.floor(ms / 1000));
    return s < 60 ? `${s}s` : s < 3600 ? `${Math.floor(s / 60)}m${String(s % 60).padStart(2, "0")}s` : `${Math.floor(s / 3600)}h${String(Math.floor(s % 3600 / 60)).padStart(2, "0")}m`;
}

function jobLabel(job) {
    return job.name ? `${job.name} (${job.jobId})` : job.jobId;
}

function oneLine(value, max = 160) {
    const text = String(value ?? "").replace(/\s+/g, " ").trim();
    return text.length > max ? `${text.slice(0, max - 3)}...` : text;
}

function clipText(text, max) {
    return text.length > max ? `${text.slice(0, max)}\n... (${text.length - max} more chars)` : text;
}

const isRecord = value => value !== null && typeof value === "object" && !Array.isArray(value);

function contentText(content) {
    if (typeof content === "string") return content;
    if (!Array.isArray(content)) return "";
    return content.filter(part => isRecord(part) && part.type === "text" && typeof part.text === "string").map(part => part.text).join("\n");
}

function compactValue(value, depth = 0) {
    if (typeof value === "string") return value.length > ARG_MAX_CHARS ? `${value.slice(0, ARG_MAX_CHARS)}...` : value;
    if (Array.isArray(value)) return depth >= 2 ? `[${value.length} items]` : value.slice(0, 20).map(item => compactValue(item, depth + 1));
    if (isRecord(value)) {
        if (depth >= 2) return "{...}";
        const out = {};
        for (const [key, item] of Object.entries(value).slice(0, 30)) out[key] = compactValue(item, depth + 1);
        return out;
    }
    return value;
}

function argsLine(args) {
    if (!isRecord(args)) return "";
    return oneLine(Object.entries(args).map(([key, value]) => `${key}=${typeof value === "string" ? value : JSON.stringify(value)}`).join(" "), 120);
}

function lastTextLine(text) {
    const lines = String(text || "").split("\n").map(line => line.trim()).filter(Boolean);
    return lines.length ? oneLine(lines[lines.length - 1]) : "";
}

function effectiveThinking(value) {
    return typeof value === "string" && value && value !== "auto" ? value : "";
}

function compactMessage(message) {
    if (!isRecord(message)) return undefined;
    const timestamp = typeof message.timestamp === "number" ? message.timestamp : Date.now();
    if (message.role === "user" || message.role === "developer") {
        const text = contentText(message.content).trim();
        return text ? { role: message.role, content: [{ type: "text", text: clipText(text, MESSAGE_MAX_CHARS) }], timestamp } : undefined;
    }
    if (message.role === "toolResult") {
        return {
            role: "toolResult", toolCallId: String(message.toolCallId || ""), toolName: String(message.toolName || ""), isError: message.isError === true,
            content: [{ type: "text", text: clipText(contentText(message.content), TOOL_RESULT_MAX_CHARS) }], timestamp,
        };
    }
    if (message.role !== "assistant") return undefined;
    const content = [];
    for (const part of Array.isArray(message.content) ? message.content : [{ type: "text", text: contentText(message.content) }]) {
        if (!isRecord(part)) continue;
        if (part.type === "text" && typeof part.text === "string" && part.text.trim()) content.push({ type: "text", text: clipText(part.text, MESSAGE_MAX_CHARS) });
        else if (part.type === "toolCall" && typeof part.name === "string") content.push({ type: "toolCall", id: String(part.id || ""), name: part.name, arguments: compactValue(isRecord(part.arguments) ? part.arguments : {}) });
    }
    const errorMessage = typeof message.errorMessage === "string" ? oneLine(message.errorMessage, 500) : "";
    if (content.length === 0 && !errorMessage) return undefined;
    const out = { role: "assistant", content, stopReason: typeof message.stopReason === "string" ? message.stopReason : "stop", timestamp };
    for (const key of ["api", "provider", "model"]) if (typeof message[key] === "string") out[key] = message[key];
    if (errorMessage) out.errorMessage = errorMessage;
    return out;
}

function messageDisplay(message) {
    const lines = [];
    if (message.role === "assistant") {
        for (const part of message.content) {
            if (part.type === "text") lines.push(...part.text.split("\n"));
            else lines.push(`* ${part.name}${argsLine(part.arguments) ? ` ${argsLine(part.arguments)}` : ""}`);
        }
        if (message.errorMessage) lines.push(`! ${message.errorMessage}`);
    } else if (message.role === "toolResult") {
        const body = contentText(message.content).split("\n").filter(line => line.trim());
        const shown = body.slice(0, 6).map(line => `    ${oneLine(line, 200)}`);
        lines.push(`  ${message.isError ? "error" : "result"} ${message.toolName}${body.length ? ":" : " (empty)"}`, ...shown);
        if (body.length > shown.length) lines.push(`    (${body.length - shown.length} more lines)`);
    } else {
        for (const line of contentText(message.content).split("\n")) lines.push(`> ${line}`);
    }
    lines.push("");
    return lines;
}

function createJobFeed(jobId) {
    let inflight;
    const feed = {
        jobId, cursor: 0, generation: 0, base: 0, bytes: 0, chunks: [], cache: undefined, display: [], liveText: "", seq: 0,
        status: "running", streamClosed: false, hasMore: false, captureError: "", missing: "", noLog: false,
        model: "", thinking: "", activity: "running", progress: "",
    };
    const show = lines => {
        feed.display.push(...lines);
        if (feed.display.length > DISPLAY_MAX_LINES) feed.display.splice(0, feed.display.length - DISPLAY_MAX_LINES);
    };
    const append = (type, fields, timestamp = Date.now()) => {
        const line = `${JSON.stringify({ type, id: `${jobId}:${feed.generation}:${feed.seq++}`, parentId: null, timestamp: new Date(timestamp).toISOString(), ...fields })}\n`;
        feed.chunks.push(line);
        feed.bytes += Buffer.byteLength(line, "utf8");
        feed.cache = undefined;
    };
    const observeModel = model => {
        if (!model || model === feed.model) return;
        feed.model = model;
        append("model_change", { model });
        show([`[model ${model}]`]);
    };
    const observeThinking = level => {
        const value = effectiveThinking(level);
        if (!value || value === feed.thinking) return;
        feed.thinking = value;
        show([`[thinking ${value}]`]);
    };
    const reset = () => {
        feed.generation++;
        feed.base += feed.bytes + 1;
        Object.assign(feed, { bytes: 0, chunks: [], cache: undefined, display: [], liveText: "", model: "", thinking: "", activity: "running", progress: "" });
    };
    const project = event => {
        switch (event.type) {
            case "agent_start":
                feed.activity = "running";
                break;
            case "agent_end":
                if (event.isTerminal !== false) feed.activity = "idle";
                feed.liveText = "";
                break;
            case "message_start":
                if (isRecord(event.message) && event.message.role === "assistant") feed.liveText = "";
                break;
            case "message_update": {
                const update = event.assistantMessageEvent;
                if (isRecord(update) && update.type === "text_delta" && typeof update.delta === "string") feed.liveText = (feed.liveText + update.delta).slice(-LIVE_TEXT_MAX);
                else if (isRecord(event.message) && event.message.role === "assistant") feed.liveText = contentText(event.message.content).slice(-LIVE_TEXT_MAX);
                else break;
                const line = lastTextLine(feed.liveText);
                if (line) feed.progress = line;
                break;
            }
            case "message_end": {
                const message = compactMessage(event.message);
                if (!message) break;
                if (message.role === "assistant") {
                    feed.liveText = "";
                    if (message.provider && message.model) observeModel(`${message.provider}/${message.model}`);
                    const line = lastTextLine(contentText(message.content));
                    if (line) feed.progress = line;
                }
                append("message", { message }, message.timestamp);
                show(messageDisplay(message));
                break;
            }
            case "tool_execution_start":
                if (typeof event.toolName === "string") feed.progress = oneLine(`tool ${event.toolName} ${argsLine(event.args)}`);
                break;
            case "model_changed":
                if (isRecord(event.model) && typeof event.model.provider === "string" && typeof event.model.id === "string") observeModel(`${event.model.provider}/${event.model.id}`);
                break;
            case "thinking_level_changed":
                observeThinking(event.resolved ?? event.thinkingLevel);
                break;
        }
    };
    const pumpPages = async signal => {
        for (let pages = 0; pages < PUMP_MAX_PAGES; pages++) {
            const res = await callSupervisorTool(resolveEndpoint(), "events", { job_id: jobId, after: feed.cursor, limit: EVENTS_PAGE_LIMIT }, { signal, timeoutMs: 30000 });
            if (!res.ok) {
                if (/not found|unknown job|no such job/i.test(res.error)) feed.missing = res.error;
                else if (/no RPC event log/i.test(res.error)) feed.noLog = true;
                else throw new Error(res.error);
                return;
            }
            const page = parseRecord(res.text);
            if (!page || !Number.isSafeInteger(page.next_cursor) || !Array.isArray(page.events)) throw new Error("Supervisor returned an invalid event page");
            if (page.reset === true) reset();
            else if (page.from_cursor !== feed.cursor) throw new Error("Supervisor returned an event page for a different cursor");
            for (const row of page.events) if (isRecord(row) && isRecord(row.event)) project(row.event);
            feed.cursor = page.next_cursor;
            if (typeof page.status === "string") feed.status = page.status;
            feed.streamClosed = page.stream_closed === true;
            feed.hasMore = page.has_more === true;
            feed.captureError = typeof page.capture_error === "string" ? page.capture_error : "";
            if (!feed.model && typeof page.model_resolved === "string") observeModel(page.model_resolved);
            if (typeof page.thinking === "string") observeThinking(page.thinking);
            if (!feed.hasMore) return;
        }
    };
    feed.pump = signal => {
        inflight ??= pumpPages(signal).finally(() => { inflight = undefined; });
        return inflight;
    };
    feed.drained = () => isTerminalStatus(feed.status) && feed.streamClosed && !feed.hasMore;
    feed.readTranscript = async fromByte => {
        try {
            await feed.pump();
        } catch { }
        if (feed.missing && feed.bytes === 0) return { text: "", newSize: feed.base, error: `The Supervisor no longer knows job ${jobId}: ${feed.missing}` };
        const start = Number.isSafeInteger(fromByte) && fromByte >= 0 ? fromByte : 0;
        const newSize = feed.base + feed.bytes;
        const reset = start < feed.base || start > newSize;
        feed.cache ??= Buffer.from(feed.chunks.join(""), "utf8");
        const text = feed.cache.subarray(reset ? 0 : start - feed.base).toString("utf8");
        return reset ? { text, newSize, reset: true } : { text, newSize };
    };
    feed.view = () => (feed.liveText.trim() ? [...feed.display, ...feed.liveText.split("\n")] : feed.display);
    return feed;
}

const MECHANISMS = { external: "external agent", hook: "async job hook", fallback: "fallback poller" };

const completionStatus = status => (status === "done" ? "done" : /^cancel/.test(String(status)) ? "cancelled" : "failed");

function createJobTracker(pi) {
    let ctx;
    let timer;
    let pumping = false;
    let widgetShown = false;
    const jobs = new Map();
    const known = new Map();
    const feeds = new Map();
    const settled = new Set();

    const mode = () => (typeof ctx?.agents?.registerExternal === "function" ? "external" : typeof ctx?.asyncJobs?.register === "function" ? "hook" : "fallback");

    const feedFor = jobId => {
        let feed = feeds.get(jobId);
        if (!feed) {
            feed = createJobFeed(jobId);
            feeds.set(jobId, feed);
        }
        return feed;
    };

    const append = (state, job, extra = {}) => {
        try {
            pi.appendEntry(JOB_ENTRY_TYPE, { state, jobId: job.jobId, name: job.name, machine: job.machine, startedAt: job.startedAt, ...extra });
        } catch { }
    };

    const progressLine = job => {
        const feed = feeds.get(job.jobId);
        const state = feed?.activity === "idle" ? "idle" : feed?.status || "running";
        const model = feed?.model || job.model;
        const thinking = feed?.thinking || job.thinking;
        return `${state}, ${elapsedLabel(Date.now() - job.startedAt)}${model ? `, ${model}${thinking ? `:${thinking}` : ""}` : ""}${feed?.progress ? `: ${feed.progress}` : ""}`;
    };

    const renderWidget = () => {
        if (!ctx?.hasUI || typeof ctx.ui?.setWidget !== "function") return;
        const shown = [...jobs.values()].filter(job => job.tracking === "fallback");
        if (shown.length === 0) {
            if (widgetShown) ctx.ui.setWidget(WIDGET_KEY, undefined);
            widgetShown = false;
            return;
        }
        widgetShown = true;
        ctx.ui.setWidget(WIDGET_KEY, [`Remote jobs running: ${shown.length}`, ...shown.map(job => oneLine(`  ${jobLabel(job)}${job.machine ? ` on ${job.machine}` : ""}, ${progressLine(job)}`, 240))]);
    };

    const stopTimerIfIdle = () => {
        if (timer === undefined || [...jobs.values()].some(job => job.tracking !== "hook")) return;
        ctx?.clearTimer?.(timer);
        timer = undefined;
    };

    const settle = (job, status) => {
        if (settled.has(job.jobId)) return false;
        settled.add(job.jobId);
        jobs.delete(job.jobId);
        job.finalStatus = status;
        append("settled", job, { status, cursor: feeds.get(job.jobId)?.cursor ?? 0 });
        renderWidget();
        stopTimerIfIdle();
        return true;
    };

    const finalReport = async (job, status, signal) => {
        const res = await callSupervisorTool(resolveEndpoint(), "result", { job_id: job.jobId, max_chars: RESULT_MAX_CHARS }, { signal, timeoutMs: 30000 });
        const payload = res.ok ? parseRecord(res.text) : undefined;
        const finalStatus = payload?.status || status;
        const machine = payload?.machine || job.machine;
        const model = payload?.model_resolved || payload?.model;
        const head = `Remote job ${jobLabel(job)} ended: ${finalStatus}${machine ? ` on ${machine}` : ""}${model ? `, model ${model}` : ""}.`;
        if (!res.ok) return { status: finalStatus, text: `${head}\nThe result could not be read (${res.error}); use remote op result with to ${job.jobId}.` };
        const report = typeof payload?.report === "string" && payload.report.trim() ? payload.report.trim() : res.text.trim() || "(no report)";
        return { status: finalStatus, text: `${head}\n\n${report}${payload?.truncated ? `\n\n(report truncated; remote op result with to ${job.jobId} reads it again)` : ""}` };
    };

    const readStatus = async (job, signal) => {
        const res = await callSupervisorTool(resolveEndpoint(), "status", { job_id: job.jobId }, { signal, timeoutMs: 30000 });
        if (!res.ok) return { error: res.error, missing: /not found|unknown job|no such job/i.test(res.error) };
        const payload = parseRecord(res.text);
        if (!payload) return { error: "unreadable status" };
        return { payload };
    };

    const observe = async (job, signal) => {
        const feed = feedFor(job.jobId);
        try {
            await feed.pump(signal);
        } catch {
            return undefined;
        }
        if (feed.missing) return { status: "lost", text: `Remote job ${jobLabel(job)} is no longer known to the Supervisor: ${feed.missing}` };
        if (feed.captureError) {
            const outcome = await finalReport(job, "error", signal);
            return { status: "error", failed: true, text: `${outcome.text}\n\nThe Supervisor lost the job's event stream: ${feed.captureError}` };
        }
        if (feed.noLog) {
            const state = await readStatus(job, signal);
            if (state.missing) return { status: "lost", text: `Remote job ${jobLabel(job)} is no longer known to the Supervisor: ${state.error}` };
            if (state.payload) {
                feed.status = state.payload.status || feed.status;
                if (typeof state.payload.last_tool === "string" && state.payload.last_tool) feed.progress = `tool ${state.payload.last_tool}`;
                if (isTerminalStatus(state.payload.status)) return finalReport(job, state.payload.status, signal);
            }
            return undefined;
        }
        return feed.drained() ? finalReport(job, feed.status, signal) : undefined;
    };

    const runHookJob = job => async ({ signal, reportProgress }) => {
        for (;;) {
            if (signal.aborted) {
                await callSupervisorTool(resolveEndpoint(), "cancel", { job_id: job.jobId }, { timeoutMs: 15000 });
                settle(job, "canceled");
                return `Remote job ${jobLabel(job)} canceled.`;
            }
            const outcome = await observe(job, signal);
            if (signal.aborted) continue;
            if (outcome) {
                settle(job, outcome.status);
                if (outcome.failed) throw new Error(outcome.text);
                return outcome.text;
            }
            try {
                await reportProgress(progressLine(job));
            } catch { }
            await abortableSleep(HOOK_POLL_MS, signal);
        }
    };

    const deliver = (job, text, status) => {
        if (!settle(job, status)) return;
        pi.sendMessage({ customType: RESULT_MESSAGE_TYPE, content: text, display: true, details: { jobId: job.jobId, name: job.name, machine: job.machine, status } }, { deliverAs: "followUp", triggerTurn: true });
    };

    const tickExternal = async job => {
        const outcome = await observe(job);
        const feed = feedFor(job.jobId);
        const patch = { status: feed.activity === "idle" ? "idle" : "running" };
        if (feed.progress) patch.progress = feed.progress;
        if (feed.model) patch.model = feed.model;
        if (feed.thinking) patch.thinking = feed.thinking;
        const signature = JSON.stringify(patch);
        if (signature !== job.lastPatch) {
            job.lastPatch = signature;
            try {
                job.handle.update(patch);
            } catch { }
        }
        if (!outcome || !settle(job, outcome.status)) return;
        try {
            job.handle.complete({ status: outcome.failed ? "failed" : completionStatus(outcome.status), text: outcome.text });
        } catch { }
    };

    const pumpAll = async () => {
        if (pumping) return;
        pumping = true;
        try {
            for (const job of [...jobs.values()]) {
                if (!jobs.has(job.jobId)) continue;
                try {
                    if (job.tracking === "external") await tickExternal(job);
                    else if (job.tracking === "fallback") {
                        const outcome = await observe(job);
                        if (outcome) deliver(job, outcome.text, outcome.status);
                    }
                } catch { }
            }
        } finally {
            pumping = false;
            renderWidget();
        }
    };

    const ensureTimer = () => {
        if (timer === undefined) timer = ctx.setInterval(() => void pumpAll(), PUMP_MS);
        void pumpAll();
    };

    const steerJob = async (job, text, steerMode) => {
        const tool = steerMode === "follow_up" ? "message" : "steer";
        const sessionId = ctx?.sessionManager?.getSessionId?.();
        const args = { job_id: job.jobId, text, mode: steerMode === "follow_up" ? "follow_up" : "steer" };
        if (sessionId) args.sender_session_id = sessionId;
        const res = await callSupervisorTool(resolveEndpoint(), tool, args, { timeoutMs: 30000 });
        if (!res.ok) throw new Error(`Supervisor ${tool} failed: ${res.error}`);
    };

    const cancelJob = async job => {
        const res = await callSupervisorTool(resolveEndpoint(), "cancel", { job_id: job.jobId }, { timeoutMs: 15000 });
        if (!res.ok) throw new Error(`Supervisor cancel failed: ${res.error}`);
    };

    const follow = job => {
        if (!ctx || settled.has(job.jobId) || jobs.has(job.jobId)) return;
        known.set(job.jobId, job);
        jobs.set(job.jobId, job);
        const how = mode();
        if (how === "external") {
            try {
                const execution = { backend: "supervisor", jobId: job.jobId };
                if (job.machine) execution.machine = job.machine;
                if (job.model) execution.model = job.model;
                if (job.thinking) execution.thinking = job.thinking;
                job.handle = ctx.agents.registerExternal({
                    label: job.name || job.jobId,
                    execution,
                    readTranscript: fromByte => feedFor(job.jobId).readTranscript(fromByte),
                    steer: (text, steerMode) => steerJob(job, text, steerMode),
                    cancel: () => cancelJob(job),
                });
                job.agentId = job.handle.id;
                job.tracking = "external";
                ensureTimer();
                return;
            } catch { }
        } else if (how === "hook") {
            try {
                ctx.asyncJobs.register(`remote ${jobLabel(job)}`, runHookJob(job), { intent: `Remote job ${job.name || job.jobId}${job.machine ? ` on ${job.machine}` : ""}` });
                job.tracking = "hook";
                return;
            } catch { }
        }
        job.tracking = "fallback";
        ensureTimer();
        renderWidget();
    };

    const find = ref => {
        const wanted = String(ref || "").trim();
        if (!wanted) return undefined;
        const lower = wanted.toLowerCase();
        const matches = [...known.values()].filter(job => job.jobId === wanted || job.agentId === wanted || job.name?.toLowerCase() === lower);
        const job = matches.length > 0 ? matches[matches.length - 1] : wanted.startsWith("sub_") ? { jobId: wanted, startedAt: Date.now() } : undefined;
        return job ? { job, feed: feedFor(job.jobId) } : undefined;
    };

    return {
        mechanism: () => MECHANISMS[mode()],
        count: () => jobs.size,
        owns: jobId => jobs.has(jobId),
        find,
        list() {
            if (known.size === 0) return "No remote jobs tracked in this session.";
            const lines = [`Remote jobs in this session (tracking: ${MECHANISMS[mode()]}):`];
            for (const job of known.values()) {
                const live = jobs.has(job.jobId);
                lines.push(`- ${jobLabel(job)}${job.machine ? ` on ${job.machine}` : ""}: ${live ? progressLine(job) : job.finalStatus || "settled"}${job.agentId ? ` [agent ${job.agentId}]` : ""}`);
            }
            lines.push("/supervisor transcript <job id or name> shows a job's transcript.");
            return lines.join("\n");
        },
        start(sessionCtx) {
            this.stop();
            ctx = sessionCtx;
            let entries = [];
            try {
                entries = ctx.sessionManager?.getEntries?.() ?? [];
            } catch { }
            const pending = new Map();
            for (const entry of entries) {
                if (entry?.type !== "custom" || entry.customType !== JOB_ENTRY_TYPE || typeof entry.data?.jobId !== "string") continue;
                const data = entry.data;
                if (data.state === "spawned") pending.set(data.jobId, data);
                else if (data.state === "settled") {
                    pending.delete(data.jobId);
                    settled.add(data.jobId);
                    known.set(data.jobId, { jobId: data.jobId, name: data.name, machine: data.machine, startedAt: Number(data.startedAt) || Date.now(), finalStatus: data.status });
                }
            }
            for (const data of pending.values()) {
                follow({ jobId: data.jobId, name: data.name, machine: data.machine, startedAt: Number(data.startedAt) || Date.now() });
            }
        },
        stop() {
            if (timer !== undefined) ctx?.clearTimer?.(timer);
            timer = undefined;
            if (widgetShown && ctx?.hasUI) ctx.ui?.setWidget?.(WIDGET_KEY, undefined);
            widgetShown = false;
            jobs.clear();
            known.clear();
            feeds.clear();
            settled.clear();
            ctx = undefined;
        },
        adopt(job, execCtx) {
            ctx ??= execCtx;
            append("spawned", job);
            follow(job);
            return job;
        },
        markRead(jobId, status) {
            const job = jobs.get(jobId);
            if (job?.tracking === "fallback" && isTerminalStatus(status)) settle(job, status);
        },
    };
}

function wrapRow(line, width) {
    const text = String(line).replace(/\t/g, "    ");
    if (text.length <= width) return [text];
    const rows = [];
    for (let i = 0; i < text.length; i += width) rows.push(text.slice(i, i + width));
    return rows;
}

async function transcriptCommand(tracker, ref, ctx) {
    if (!ref) return "Usage: /supervisor transcript <job id or name> (see /supervisor jobs)";
    const target = tracker.find(ref);
    if (!target) return `No job in this session matches ${JSON.stringify(ref)}; /supervisor jobs lists them, or pass an exact sub_ id.`;
    const { job, feed } = target;
    try {
        await feed.pump();
    } catch (error) {
        return `Reading the transcript of ${jobLabel(job)} failed: ${errorMessage(error)}`;
    }
    if (feed.missing) return `The Supervisor no longer knows ${jobLabel(job)}: ${feed.missing}`;
    if (feed.noLog) return `${jobLabel(job)} has no RPC event log, so there is no transcript to show.`;
    const header = () => `${jobLabel(job)}${job.machine ? ` on ${job.machine}` : ""}: ${feed.drained() ? feed.status : feed.activity === "idle" ? "idle" : "running"}${feed.model ? `, ${feed.model}${feed.thinking ? `:${feed.thinking}` : ""}` : ""}`;
    if (!ctx.hasUI || typeof ctx.ui?.custom !== "function") return [`Transcript of ${header()}`, "", ...feed.view().slice(-TRANSCRIPT_TEXT_LINES)].join("\n");
    await ctx.ui.custom((ui, _theme, _keybindings, done) => {
        let offset = 0;
        let height = 10;
        let closed = false;
        let refreshing = false;
        const timer = ctx.setInterval(async () => {
            if (closed || refreshing || feed.drained()) return;
            refreshing = true;
            try {
                await feed.pump();
            } catch { }
            refreshing = false;
            if (!closed) ui.requestRender();
        }, TRANSCRIPT_POLL_MS);
        const close = () => {
            if (closed) return;
            closed = true;
            ctx.clearTimer(timer);
            done(undefined);
        };
        return {
            render(width) {
                const w = Math.max(20, width);
                height = Math.max(3, ui.size().rows - 4);
                const body = feed.view().flatMap(line => wrapRow(line, w));
                offset = Math.min(offset, Math.max(0, body.length - height));
                const end = body.length - offset;
                const visible = body.slice(Math.max(0, end - height), end);
                while (visible.length < height) visible.push("");
                const position = offset > 0 ? `scrolled up ${offset} rows` : feed.drained() ? "ended" : "following live";
                return [oneLine(header(), w), "-".repeat(Math.min(w, 80)), ...visible, "-".repeat(Math.min(w, 80)), oneLine(`${position}. q/Esc close, Up/Down/PgUp/PgDn scroll, End follow`, w)];
            },
            handleInput(data) {
                if (data === "q" || data === "\x1b" || data === "\x03") return close();
                if (data === "\x1b[A" || data === "k") offset++;
                else if (data === "\x1b[B" || data === "j") offset = Math.max(0, offset - 1);
                else if (data === "\x1b[5~") offset += height;
                else if (data === "\x1b[6~") offset = Math.max(0, offset - height);
                else if (data === "\x1b[F" || data === "\x1b[4~" || data === "G") offset = 0;
                else if (data === "\x1b[H" || data === "\x1b[1~" || data === "g") offset = Number.MAX_SAFE_INTEGER;
                else return;
                ui.requestRender();
            },
            dispose() {
                closed = true;
                ctx.clearTimer(timer);
            },
        };
    }, { overlay: true, fullscreen: true });
    return undefined;
}

const REMOTE_DESCRIPTION = [
    "Discover configured Supervisor hosts, spawn and steer jobs on them, reach registered agent sessions, and read saved conversations. Spawned jobs become tracked children of this session. Prefer `task` for local work; `remote` is for work that must run on another machine or outlive this process. OMP is the default; use a Grok webhook peer only when the user explicitly picks Grok.",
    "",
    "Supervisor addresses jobs (`sub_` ids, launched by `spawn`: inspectable, steerable, cancellable, queueable) and registered agents (`agt_` ids from `list`: other people's enrolled sessions, show and prompt only, never launch/cancel/resume).",
    "",
    "Ops:",
    "- `machines`: configured launch targets; each row's `id` is the exact `spawn.machine` selector. `models`: optional `machine`, `query`, `limit`: that host's roles, fallback chains and model ids.",
    "- `spawn`: `message` is the task. Optional `name` (display only), `machine`, `cwd`, `model`, `thinking`, `maxTime` (default 6h), `sessionId` to resume. Returns the `sub_` job id.",
    "- `status` / `result` / `events`: `to` is the exact `sub_` id; `events` takes `after` and `limit`. `cancel`: `to` kills a running job; `scheduleId` instead drops a queued follow-up.",
    "- `extend`: `to` + exactly one of `maxTime` (new total limit from job start, e.g. 6h) or `add` (added to the current deadline, e.g. 2h); raises or lowers a running job's time limit.",
    "- `steer` / `message`: `to` + `message` into a RUNNING job; `mode` steer (default) interrupts, aside lands without aborting, follow_up waits for the turn. `schedule`: `to` + `message` queue a prompt that starts once `to` is terminal.",
    "- `jobs`: every retained job, newest first; `limit`, `offset` (continue from `next_offset`). Listing never adopts a job.",
    "- `list`: registered agent sessions (`machineFilter`, `query`, `limit`, `includeGrok`). `agent`: `to` agent id, `includeQueue`, `includeHistory`. `prompt`: `to` agent id + `message`, `mode` (default aside), `waitFor` none|delivered|ack, `waitSeconds`.",
    "- `sessions`: search saved conversations (`query`, `cwd`, `limit`); `session`: `sessionId` reads one (`machine`, `limit`).",
    "",
    "- Use the exact returned host alias and `sub_`/`agt_` ids; never assume `local` or that an agent id is a `spawn.machine`.",
    "- `steer`/`message` reach a RUNNING `sub_` job; `prompt` reaches an `agt_` agent. Mixing them fails.",
    "- A spawn receipt is not completion; results auto-deliver. NEVER poll `status`/`result`: the spawn receipt says whether to end your turn or call `wait`.",
    "- Steering a finished job fails: `status` first, or `schedule` a follow-up. Destination `cwd`/`model` resolve on the destination.",
].join("\n");

function createRemoteTool(pi, tracker, identity) {
    const { Type } = pi.typebox;
    const ops = ["spawn", "status", "result", "cancel", "steer", "message", "schedule", "extend", "events", "jobs", "machines", "models", "list", "agent", "prompt", "sessions", "session"];
    const str = description => Type.Optional(Type.String({ description }));
    const num = description => Type.Optional(Type.Number({ description }));
    const bool = description => Type.Optional(Type.Boolean({ description }));
    const parameters = Type.Object({
        op: Type.Union(ops.map(op => Type.Literal(op)), { description: "Supervisor operation" }),
        message: str("spawn/steer/message/prompt/schedule: the task, input text, or queued prompt"),
        name: str("spawn: display name. An identifier is kept; a phrase is turned into one. Not a job address."),
        to: str("status/result/cancel/steer/message/schedule/events: exact sub_ job id; agent/prompt: registered agent id from list"),
        scheduleId: str("cancel: drop a queued follow-up instead of killing a running job"),
        mode: Type.Optional(Type.Union([Type.Literal("steer"), Type.Literal("aside"), Type.Literal("follow_up")], { description: "steer/message/prompt: steer interrupts now, aside lands without aborting, follow_up waits for the current turn" })),
        waitFor: Type.Optional(Type.Union([Type.Literal("none"), Type.Literal("delivered"), Type.Literal("ack")], { description: "prompt: none returns at once, delivered waits for pickup (default), ack waits for the agent's reply" })),
        waitSeconds: num("prompt: seconds to wait for waitFor (Supervisor default 10)"),
        includeQueue: bool("agent: include the full queued prompt list"),
        includeHistory: bool("agent: include acknowledged deliveries"),
        machineFilter: str("list: restrict registered agent sessions to one machine alias"),
        includeGrok: bool("list: explicitly include configured Grok webhook peers"),
        after: num("events: resume the durable log after this sequence number"),
        offset: num("jobs: page offset; continue with the returned next_offset"),
        limit: num("events/jobs/list/models/sessions: max rows; session: max messages"),
        query: str("models: substring of provider, id or selector; list/sessions: substring match"),
        machine: str("spawn/models/session: exact id from machines; omitted means the default launch target"),
        cwd: str("spawn/schedule: working directory on the destination; sessions: working-directory substring"),
        model: str("spawn/schedule: model override, interpreted on the destination"),
        thinking: str("spawn/schedule: reasoning effort"),
        maxTime: str("spawn/schedule: deadline such as 30m or 2h, default 6h; extend: new total limit from job start"),
        add: str("extend: time added to the current deadline, such as 2h"),
        sessionId: str("spawn: destination session to resume; session: saved conversation to read"),
    });

    const fail = (text, details) => ({ content: [{ type: "text", text }], details, isError: true });

    const call = async (name, args, params, signal, jobId) => {
        const details = { op: params.op, ...(jobId ? { jobId } : {}) };
        const waitMs = typeof args.wait_seconds === "number" ? args.wait_seconds * 1000 : 0;
        const res = await callSupervisorTool(resolveEndpoint(), name, args, { signal, timeoutMs: name === "models" ? 120000 : 30000 + waitMs });
        if (!res.ok) return fail(`Supervisor ${name} failed: ${res.error}.`, details);
        if (!res.text) return fail("Supervisor returned no result.", details);
        const payload = parseRecord(res.text);
        if (jobId && payload) tracker.markRead(jobId, payload.status);
        if (params.op === "result" && payload && typeof payload.report === "string") {
            const { report, ...rest } = payload;
            return { content: [{ type: "text", text: `${JSON.stringify(rest)}\n\nreport:\n${report}` }], details: { ...details, status: payload.status, payload: rest } };
        }
        return { content: [{ type: "text", text: res.text }], details: { ...details, ...(payload ? { status: payload.status, payload } : {}) } };
    };

    const callerSource = ctx => {
        const from = identity().agentId || ctx.agent?.id || "omp";
        return { from, source: { kind: "agent", from, route: "omp" } };
    };

    return {
        name: "remote",
        label: "Remote",
        description: REMOTE_DESCRIPTION,
        parameters,
        approval: "read",
        loadMode: "essential",
        async execute(toolCallId, params, signal, _onUpdate, ctx) {
            const op = params.op;
            const pick = (pairs) => {
                const args = {};
                for (const [key, value] of pairs) if (value !== undefined && value !== "") args[key] = value;
                return args;
            };
            if (op === "machines") return call("machines", {}, params, signal);
            if (op === "models") return call("models", pick([["machine", params.machine], ["query", params.query], ["limit", params.limit]]), params, signal);
            if (op === "jobs") return call("jobs", pick([["limit", params.limit], ["offset", params.offset]]), params, signal);
            if (op === "sessions") return call("sessions", pick([["query", params.query], ["cwd", params.cwd], ["limit", params.limit]]), params, signal);
            if (op === "session") {
                const sessionId = params.sessionId?.trim();
                if (!sessionId) return fail("session requires the saved conversation id in sessionId.", { op });
                return call("session", pick([["session_id", sessionId], ["max_chars", RESULT_MAX_CHARS], ["machine", params.machine], ["max_messages", params.limit]]), params, signal);
            }
            if (op === "spawn") {
                const task = params.message?.trim();
                if (!task) return fail("spawn requires the task in message.", { op });
                const parentSessionId = ctx?.sessionManager?.getSessionId?.() || "";
                if (!parentSessionId) return fail("Supervisor spawn requires a live parent session; no job was launched.", { op });
                try {
                    await ctx.sessionManager.ensureOnDisk?.();
                } catch { }
                const name = supervisorAgentName(params.name, task);
                const args = pick([
                    ["prompt", task], ["steerable", true], ["max_time", params.maxTime ?? REMOTE_JOB_DEFAULT_MAX_TIME], ["title", name],
                    ["machine", params.machine], ["cwd", params.cwd], ["model", params.model], ["thinking", params.thinking], ["session_id", params.sessionId],
                    ["parent_session_id", parentSessionId], ["root_session_id", parentSessionId], ["parent_agent_id", ctx.agent?.id || "Main"], ["parent_tool_call_id", toolCallId],
                ]);
                const res = await callSupervisorTool(resolveEndpoint(), "run", args, { signal, timeoutMs: 60000 });
                if (!res.ok) return fail(`Supervisor run failed: ${res.error}. Inspect the roster (op jobs) before retrying a spawn.`, { op });
                const receipt = parseRecord(res.text) ?? {};
                const jobId = typeof receipt.job_id === "string" ? receipt.job_id : "";
                if (!jobId.startsWith("sub_")) return fail(`Supervisor returned no job id: ${res.text}. Inspect the roster before retrying.`, { op });
                const machine = typeof receipt.machine === "string" && receipt.machine ? receipt.machine : params.machine;
                const model = receipt.model_resolved || receipt.model || params.model;
                const thinking = receipt.thinking || params.thinking;
                const job = tracker.adopt({ jobId, name, machine, model, thinking, startedAt: Date.now() }, ctx);
                const next = job.tracking === "external"
                    ? `It runs as subagent ${job.agentId}: its result arrives on its own, so end your turn or call \`wait\` once nothing else is left.`
                    : job.tracking === "hook"
                        ? (ctx.hasUI ? "Its result arrives as a message on its own: end your turn once nothing else is left." : "Its result arrives as a message; once nothing else is left, call `wait` instead of polling.")
                        : "Its result arrives as a follow-up message when the job ends: end your turn once nothing else is left; do not poll status/result.";
                const summary = `Spawned ${name} as ${jobId}${machine ? ` on ${machine}` : ""} (status ${receipt.status || "running"}${model ? `, model ${model}` : ""}).`;
                return { content: [{ type: "text", text: `${summary}\n${next}` }], details: { op, jobId, name, machine, status: receipt.status, model, agentId: job.agentId, payload: receipt } };
            }
            if (op === "list") {
                return call("agents", pick([["machine", params.machineFilter], ["limit", params.limit], ["query", params.query], ["include_grok", params.includeGrok === true ? true : undefined]]), params, signal);
            }
            if (op === "agent" || op === "prompt") {
                const agentId = params.to?.trim();
                if (!agentId) return fail(`${op} requires the agent id from list in to.`, { op });
                if (op === "agent") return call("agent", pick([["agent_id", agentId], ["include_queue", params.includeQueue === true ? true : undefined], ["include_history", params.includeHistory === true ? true : undefined]]), params, signal);
                if (agentId.startsWith("sub_")) return fail("prompt addresses registered agents; use steer or message for a sub_ job.", { op });
                const text = params.message?.trim();
                if (!text) return fail("prompt requires the prompt text in message.", { op });
                const { from, source } = callerSource(ctx);
                const self = identity();
                if (agentId === self.agentId) return fail(`Refused: ${agentId} is this session's own Supervisor registration; a session cannot prompt itself.`, { op });
                return call("prompt", pick([
                    ["agent_id", agentId], ["text", text], ["mode", params.mode ?? "aside"], ["from", from], ["source", source],
                    ["sender_session_id", ctx?.sessionManager?.getSessionId?.()], ["wait_for", params.waitFor], ["wait_seconds", params.waitSeconds],
                ]), params, signal);
            }
            const jobId = params.to?.trim();
            if (!jobId?.startsWith("sub_")) {
                if (op === "cancel" && params.scheduleId) return call("cancel", { schedule_id: params.scheduleId }, params, signal);
                return fail("Remote job operations require an exact sub_ job id in to.", { op });
            }
            if (op === "steer" || op === "message") {
                const text = params.message?.trim();
                if (!text) return fail(`${op} requires the text to deliver in message.`, { op, jobId });
                const { source } = callerSource(ctx);
                return call(op, pick([["job_id", jobId], ["text", text], ["source", source], ["mode", params.mode], ["sender_session_id", ctx?.sessionManager?.getSessionId?.()]]), params, signal, jobId);
            }
            if (op === "extend") {
                const maxTime = params.maxTime?.trim();
                const add = params.add?.trim();
                if (!maxTime === !add) return fail("extend requires exactly one of maxTime (new total limit) or add (added to the current deadline).", { op, jobId });
                return call("extend", pick([["job_id", jobId], ["max_time", maxTime], ["add", add]]), params, signal, jobId);
            }
            if (op === "schedule") {
                const queued = params.message?.trim();
                if (!queued) return fail("schedule requires the queued prompt in message.", { op, jobId });
                return call("schedule", pick([["job_id", jobId], ["prompt", queued], ["steerable", true], ["max_time", params.maxTime ?? REMOTE_JOB_DEFAULT_MAX_TIME], ["model", params.model], ["thinking", params.thinking], ["cwd", params.cwd]]), params, signal, jobId);
            }
            if (op === "events") return call("events", pick([["job_id", jobId], ["after", params.after], ["limit", params.limit]]), params, signal, jobId);
            return call(op, op === "result" ? { job_id: jobId, max_chars: RESULT_MAX_CHARS } : { job_id: jobId }, params, signal, jobId);
        },
    };
}

function runProcess(command, args, { input, timeoutMs = 60000 } = {}) {
    return new Promise(resolve => {
        let stdout = "", stderr = "", done = false;
        let child;
        const finish = result => {
            if (done) return;
            done = true;
            clearTimeout(timer);
            resolve(result);
        };
        try {
            child = spawn(command, args, { windowsHide: true, stdio: ["pipe", "pipe", "pipe"] });
        } catch (error) {
            resolve({ code: -1, stdout: "", stderr: errorMessage(error) });
            return;
        }
        const timer = setTimeout(() => {
            try { child.kill(); } catch { }
            finish({ code: -1, stdout, stderr: `${stderr}\ntimed out after ${timeoutMs}ms` });
        }, timeoutMs);
        child.stdout.on("data", chunk => { stdout += chunk; });
        child.stderr.on("data", chunk => { stderr += chunk; });
        child.on("error", error => finish({ code: -1, stdout, stderr: `${stderr}${errorMessage(error)}` }));
        child.on("close", code => finish({ code: code ?? -1, stdout, stderr }));
        child.stdin.on("error", () => { });
        child.stdin.end(input ?? "");
    });
}

function sshRun(sshHost, remoteCommand, options) {
    return runProcess("ssh", ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "--", sshHost, remoteCommand], options);
}

function nodeEval(nodeCommand, source) {
    return `${nodeCommand} -e "eval(Buffer.from('${Buffer.from(source).toString("base64")}','base64').toString())"`;
}

const PROBE_SOURCE = "const os=require('os');console.log(JSON.stringify({platform:process.platform,arch:process.arch,home:os.homedir(),hostname:os.hostname(),node:process.execPath,version:process.version}))";

function installSource(relDir, fileName) {
    return [
        "const fs=require('fs'),os=require('os'),path=require('path'),url=require('url');",
        `const dir=path.join(os.homedir(),...${JSON.stringify(relDir)}.split('/').filter(Boolean));`,
        `const file=path.join(dir,${JSON.stringify(fileName)});`,
        "const chunks=[];process.stdin.on('data',c=>chunks.push(c));process.stdin.on('end',()=>{",
        "fs.mkdirSync(dir,{recursive:true});fs.writeFileSync(file,Buffer.concat(chunks));",
        "const omp=process.env.OMP_BIN||(process.platform==='win32'?path.join(os.homedir(),'AppData','Local','omp','omp.exe'):path.join(os.homedir(),'.local','bin','omp'));",
        "import(url.pathToFileURL(file).href).then(m=>console.log(JSON.stringify({file:file.split(path.sep).join('/'),ok:typeof m.runHost==='function',omp,ompPresent:fs.existsSync(omp)})),e=>console.log(JSON.stringify({file,ok:false,error:String(e&&e.message||e)})));",
        "});",
    ].join("");
}

function lastJsonLine(text) {
    const lines = String(text || "").trim().split(/\r?\n/).reverse();
    for (const line of lines) {
        const parsed = parseRecord(line.trim());
        if (parsed) return parsed;
    }
    return undefined;
}

function quoteIfNeeded(value) {
    return /[\s"'$`\\]/.test(value) ? `'${value.replace(/'/g, "'\\''")}'` : value;
}

function readMachines() {
    return readJsonObject(machinesFilePath()) ?? {};
}

async function targetAdd(tokens, endpoint) {
    const positional = [];
    let osFlag = "", relDir = ".omp/agent";
    for (let i = 0; i < tokens.length; i++) {
        if (tokens[i] === "--os") osFlag = tokens[++i] ?? "";
        else if (tokens[i] === "--dir") relDir = tokens[++i] ?? "";
        else positional.push(tokens[i]);
    }
    const [name, sshHost] = positional;
    const usage = "Usage: /supervisor target add <name> <sshHost> [--os linux|darwin|windows] [--dir <dir relative to the target home, default .omp/agent>]";
    if (!name || !sshHost || positional.length > 2) return usage;
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(name)) return `Invalid target name ${JSON.stringify(name)}: use letters, digits, dot, dash or underscore.`;
    if (sshHost.startsWith("-") || /[\s\0]/.test(sshHost)) return `Invalid ssh host ${JSON.stringify(sshHost)}.`;
    if (osFlag && !["linux", "darwin", "windows"].includes(osFlag)) return `--os must be linux, darwin or windows. ${usage}`;
    if (!relDir || path.isAbsolute(relDir) || /^[A-Za-z]:/.test(relDir) || relDir.split(/[\\/]/).includes("..")) return "--dir must be a path relative to the target's home directory.";
    if (endpoint.mode !== "hub-local") return `This session is a client of ${endpoint.url}; run /supervisor target add on the hub host, where the Supervisor reads ${machinesFilePath()}.`;
    let source;
    try {
        source = readFileSync(RPC_HOST_SOURCE);
    } catch (error) {
        return `Cannot read the bundled spawn host ${RPC_HOST_SOURCE}: ${errorMessage(error)}`;
    }
    const out = [];
    const reach = await sshRun(sshHost, "echo supervisor-ssh-ok", { timeoutMs: 30000 });
    if (!reach.stdout.includes("supervisor-ssh-ok")) {
        return `ssh ${sshHost} failed with BatchMode (exit ${reach.code}): ${reach.stderr.trim() || reach.stdout.trim()}\nSet up key-based ssh from this hub to ${sshHost} first (ssh-copy-id, or an authorized_keys entry on Windows), then retry.`;
    }
    out.push(`ssh ${sshHost}: ok`);
    let nodeCommand = "node";
    let probe = lastJsonLine((await sshRun(sshHost, nodeEval(nodeCommand, PROBE_SOURCE), { timeoutMs: 30000 })).stdout);
    if (!probe && osFlag !== "windows") {
        const found = await sshRun(sshHost, "\"$SHELL\" -lc 'command -v node'", { timeoutMs: 30000 });
        const nodePath = found.stdout.trim().split(/\r?\n/).pop()?.trim();
        if (nodePath && nodePath.startsWith("/")) {
            nodeCommand = quoteIfNeeded(nodePath);
            probe = lastJsonLine((await sshRun(sshHost, nodeEval(nodeCommand, PROBE_SOURCE), { timeoutMs: 30000 })).stdout);
        }
    }
    if (!probe) return `${out.join("\n")}\nnode was not found on ${sshHost}. Install Node.js 18 or newer there (on PATH for non-interactive ssh), then retry.`;
    const platformName = probe.platform === "win32" ? "windows" : probe.platform;
    if (osFlag && osFlag !== platformName) return `${out.join("\n")}\n${sshHost} reports platform ${probe.platform}, not ${osFlag}.`;
    out.push(`node ${probe.version} on ${probe.platform}/${probe.arch}, home ${probe.home}`);
    const install = await sshRun(sshHost, nodeEval(nodeCommand, installSource(toPosix(relDir), "omp-rpc-host.mjs")), { input: source, timeoutMs: 60000 });
    const installed = lastJsonLine(install.stdout);
    if (!installed?.ok) return `${out.join("\n")}\nCopying omp-rpc-host.mjs failed: ${installed?.error || install.stderr.trim() || `exit ${install.code}`}`;
    out.push(`installed ${installed.file}`);
    if (!installed.ompPresent) out.push(`warning: omp was not found at ${installed.omp} on ${sshHost}; install omp there (or set OMP_BIN) before spawning jobs on it.`);
    const windows = probe.platform === "win32";
    const command = windows ? `node ${installed.file}` : `${probe.node ? quoteIfNeeded(probe.node) : nodeCommand} ${quoteIfNeeded(installed.file)}`;
    let machines;
    try {
        machines = readMachines();
    } catch (error) {
        return `${out.join("\n")}\nCannot read ${machinesFilePath()}: ${errorMessage(error)}`;
    }
    const previous = machines[name] && typeof machines[name] === "object" ? machines[name] : {};
    machines[name] = {
        sshHost,
        description: typeof previous.description === "string" && previous.description ? previous.description : `${probe.hostname} (${probe.platform}/${probe.arch}), added by /supervisor target add`,
        hostname: probe.hostname,
        platform: probe.platform,
        architecture: probe.arch,
        command,
        home: toPosix(probe.home),
        ...(previous.default === true ? { default: true } : {}),
    };
    writeJsonFile(machinesFilePath(), machines);
    out.push(`${previous.sshHost ? "replaced" : "added"} ${name} in ${machinesFilePath()}: ${command}`);
    const smoke = await callSupervisorTool(endpoint, "machines", {}, { timeoutMs: 30000 });
    const listed = smoke.ok ? parseRecord(smoke.text)?.machines?.find?.(row => row?.id === name) : undefined;
    if (!smoke.ok) out.push(`smoke: Supervisor machines failed: ${smoke.error}`);
    else if (!listed) out.push(`smoke: ${name} is missing from the Supervisor machines list; the running service may read another machines file (SUPERVISOR_REMOTE_MACHINES_FILE).`);
    else out.push(`smoke: Supervisor lists ${name} (${listed.platform || probe.platform}/${listed.architecture || probe.arch}); spawn with remote op spawn machine ${name}.`);
    return out.join("\n");
}

function targetList() {
    let machines;
    try {
        machines = readMachines();
    } catch (error) {
        return `Cannot read ${machinesFilePath()}: ${errorMessage(error)}`;
    }
    const names = Object.keys(machines);
    if (names.length === 0) return `No spawn targets in ${machinesFilePath()}.`;
    return [`Spawn targets in ${machinesFilePath()}:`, ...names.map(name => {
        const m = machines[name] ?? {};
        return `  ${name}${m.default ? " (default)" : ""}: ssh ${m.sshHost}, ${m.platform || "?"}/${m.architecture || "?"}, ${m.command}`;
    })].join("\n");
}

function targetRemove(name) {
    if (!name) return "Usage: /supervisor target remove <name>";
    let machines;
    try {
        machines = readMachines();
    } catch (error) {
        return `Cannot read ${machinesFilePath()}: ${errorMessage(error)}`;
    }
    if (!(name in machines)) return `${name} is not in ${machinesFilePath()}.`;
    delete machines[name];
    writeJsonFile(machinesFilePath(), machines);
    return `Removed ${name} from ${machinesFilePath()}. The copied omp-rpc-host.mjs stays on the target.`;
}

function clientSetupLines(url) {
    const file = tokenFilePath();
    return [
        "Client setup (each other machine running omp with this plugin):",
        `  1. Copy the hub token file ${file} to the client, for example to ~/.omp/supervisor/hub-token, and keep it private. It is the hub's registry token; do not paste it into chats or repos.`,
        "  2. POSIX shell profile:",
        `       export SUPERVISOR_URL=${url}`,
        "       export SUPERVISOR_TOKEN=\"$(cat ~/.omp/supervisor/hub-token)\"",
        "     PowerShell profile:",
        `       $env:SUPERVISOR_URL = "${url}"`,
        "       $env:SUPERVISOR_TOKEN = (Get-Content \"$HOME/.omp/supervisor/hub-token\" -Raw).Trim()",
        "  3. Start omp. A client whose SUPERVISOR_URL is not its own local hub address never starts a local service.",
    ];
}

async function hubCommand(listenArg) {
    const endpoint = resolveEndpoint();
    if (endpoint.mode !== "hub-local") return `This session is a client of ${endpoint.url}. Run /supervisor hub on the hub host itself (without SUPERVISOR_URL pointing elsewhere).`;
    const listenHost = String(listenArg || "127.0.0.1").trim().replace(/^\[|\]$/g, "");
    if (!/^[A-Za-z0-9.:%-]+$/.test(listenHost)) return `Invalid listen host ${JSON.stringify(listenArg)}.`;
    const previousUrl = localUrl();
    const nextUrl = urlForHost(listenHost);
    writeJsonFile(HUB_CONFIG_PATH, { listenHost });
    const out = [`Saved listen host ${listenHost} in ${HUB_CONFIG_PATH}; bootstrap starts the local service this way from now on.`];
    const state = readServiceState();
    if (pluginOwnsProcess(state)) {
        try {
            process.kill(state.pid);
        } catch (error) {
            return `${out.join("\n")}\nCould not stop the plugin-started service (pid ${state.pid}): ${errorMessage(error)}`;
        }
        await waitHealthy(previousUrl, false, 10000);
        spawnSupervisor(state.binary, listenHost);
        out.push(await waitHealthy(nextUrl, true, 15000) ? `Restarted the plugin-started service on ${nextUrl}.` : `Restarted the service, but ${nextUrl}/health is not answering yet; see ${path.join(SUPERVISOR_HOME, "plugin-service.log")}.`);
    } else if (await supervisorHealthy(previousUrl)) {
        out.push(`A Supervisor already answers on ${previousUrl}, but this plugin did not start it, so it was left running. Restart it yourself with FASTMCP_HOST=${listenHost} (and auth on), or stop it and start a new omp session.`);
    } else {
        out.push(await ensureSupervisorService() ? `Started the local service on ${nextUrl}.` : `Could not start the local service on ${nextUrl}; see ${path.join(SUPERVISOR_HOME, "plugin-service.log")}.`);
    }
    const conn = readConnection();
    if (!USER_ENV.url && !conn.invalid && (!conn.connection.url || trimSlash(conn.connection.url) === previousUrl)) {
        writeJsonFile(CONNECTION_PATH, { ...conn.connection, url: nextUrl });
        out.push(`Updated ${CONNECTION_PATH} to ${nextUrl}. Restart omp on this host so its MCP connection follows.`);
    }
    if (isLoopbackHost(listenHost)) {
        out.push("The hub listens on loopback only: other machines cannot reach it. Use /supervisor hub <private-ip> (for example the Tailscale IP) to serve clients.");
        return out.join("\n");
    }
    const clientUrl = listenHost === "0.0.0.0" || listenHost === "::" ? `http://<this-hub-address>:${SUPERVISOR_PORT}` : nextUrl;
    out.push(`MCP auth: required for ${listenHost} (non-loopback). /mcp needs Authorization: Bearer <registry token>; /health stays open.`);
    if (listenHost === "0.0.0.0" || listenHost === "::") out.push("Warning: binding every interface exposes the hub beyond your private network; prefer a Tailscale or LAN address.");
    out.push(...clientSetupLines(clientUrl));
    return out.join("\n");
}

async function statusCommand(tracker, identity, remoteOwner) {
    const endpoint = resolveEndpoint();
    const out = [`Supervisor plugin ${SUPERVISOR_PLUGIN_VERSION}`, `mode: ${endpoint.mode}`, `url: ${endpoint.url}`];
    if (endpoint.invalid) out.push(`warning: ${CONNECTION_PATH} is not valid JSON`);
    out.push(`health: ${await supervisorHealthy(endpoint.url) ? "healthy" : "unreachable"}`);
    try {
        const res = await fetch(`${endpoint.url}/mcp`, {
            method: "POST",
            signal: AbortSignal.timeout(5000),
            headers: { "Content-Type": "application/json", Accept: "application/json, text/event-stream" },
            body: JSON.stringify({ jsonrpc: "2.0", id: "omp-plugin-auth-probe", method: "tools/list", params: {} }),
        });
        out.push(`mcp auth: ${res.status === 401 ? "required (401 without a token)" : res.ok ? "not required (open /mcp)" : `unknown (HTTP ${res.status} without a token)`}`);
    } catch (error) {
        out.push(`mcp auth: unknown (${errorMessage(error)})`);
    }
    out.push(`token: ${endpoint.token ? `configured from ${endpoint.tokenSource}` : "missing"}`);
    if (endpoint.mode === "hub-local") {
        const state = readServiceState();
        out.push(`listen host: ${hubListenHost()}`);
        out.push(`local service: ${pluginOwnsProcess(state) ? `started by this plugin (pid ${state.pid})` : "not started by this plugin"}`);
    }
    out.push(`registered agent: ${identity().agentId || "not registered"}`);
    out.push(`remote tool: ${remoteOwner()}`);
    out.push(`job tracking: ${tracker.mechanism()}, ${tracker.count()} tracked`);
    return out.join("\n");
}

export default function supervisorAgent(pi) {
    if (existsSync(LOOSE_COPY_PATH)) {
        let warned = false;
        pi.on("session_start", (_event, ctx) => {
            if (warned || !ctx.hasUI) return;
            warned = true;
            ctx.ui.notify(`omp-supervisor plugin is inactive: ${LOOSE_COPY_PATH} (the standalone Supervisor client) is also installed. Running both would register this session twice and deliver Supervisor messages twice. Keep one: delete that file, or uninstall the omp-supervisor plugin.`, "warning");
        });
        return;
    }
    exportClientEnv();
    const tracker = createJobTracker(pi);
    let stopCurrent = async () => { };
    let onAgentEnd = () => { };
    let onTurn = () => { };
    let selfIdentity = () => ({ agentId: "", sessionId: "" });
    let generation = 0;
    let remoteOwner = "not registered yet";
    const ensureRemoteTool = () => {
        if (remoteOwner === "plugin" || remoteOwner === "native") return;
        let tools;
        try {
            tools = pi.getAllTools();
        } catch {
            return;
        }
        if (tools.some(tool => tool?.name === "remote")) {
            remoteOwner = "native";
            return;
        }
        pi.registerTool(createRemoteTool(pi, tracker, () => selfIdentity()));
        remoteOwner = "plugin";
    };
    pi.registerCommand("supervisor", {
        description: "Supervisor plugin: status | jobs | transcript <job> | hub [listenHost] | target add|list|remove",
        getArgumentCompletions: prefix => ["status", "jobs", "transcript", "hub", "target add", "target list", "target remove"].filter(item => item.startsWith(prefix.trim())).map(value => ({ value, label: value })),
        handler: async (args, ctx) => {
            const tokens = String(args || "").trim().split(/\s+/).filter(Boolean);
            const sub = tokens[0] || "status";
            let text;
            try {
                if (sub === "status") text = await statusCommand(tracker, () => selfIdentity(), () => remoteOwner === "native" ? "native remote tool (this omp ships its own)" : remoteOwner === "plugin" ? "registered by this plugin" : remoteOwner);
                else if (sub === "jobs") text = tracker.list();
                else if (sub === "transcript") text = await transcriptCommand(tracker, tokens[1], ctx);
                else if (sub === "hub") text = await hubCommand(tokens[1]);
                else if (sub === "target" && tokens[1] === "add") text = await targetAdd(tokens.slice(2), resolveEndpoint());
                else if (sub === "target" && tokens[1] === "list") text = targetList();
                else if (sub === "target" && tokens[1] === "remove") text = targetRemove(tokens[2]);
                else text = "Usage: /supervisor status | /supervisor jobs | /supervisor transcript <job> | /supervisor hub [listenHost] | /supervisor target add <name> <sshHost> [--os linux|darwin|windows] | /supervisor target list | /supervisor target remove <name>";
            } catch (error) {
                text = `/supervisor ${sub} failed: ${errorMessage(error)}`;
            }
            if (text === undefined) return;
            if (ctx.hasUI) pi.sendMessage({ customType: COMMAND_MESSAGE_TYPE, content: text, display: true });
            else process.stdout.write(`${text}\n`);
        },
    });
    pi.on("agent_start", () => onTurn(true));
    pi.on("agent_end", event => {
        onAgentEnd(event);
        if (event?.willContinue !== true)
            onTurn(false);
    });
    pi.on("tool_call", (event, ctx) => {
        const toolName = String(event?.toolName || "");
        const input = event?.input;
        if (!input || typeof input !== "object")
            return;
        const { agentId, sessionId: registeredSession } = selfIdentity();
        const sessionId = registeredSession || ctx?.sessionManager?.getSessionId?.() || "";
        if (GATEWAY_CODE_TOOL.test(toolName)) {
            const code = sessionId ? stampGatewayCode(input.code, sessionId) : undefined;
            return code === undefined || code === input.code ? undefined : { input: { ...input, code } };
        }
        if (!SUPERVISOR_DELIVERY_TOOL.test(toolName))
            return;
        const target = deliveryTarget(input);
        if (!target)
            return;
        if ((agentId && target === agentId) || (sessionId && target === sessionId)) {
            return { block: true, reason: `Refused: ${target} is this session's own Supervisor registration; a session cannot prompt itself. Pick a different agent from agents.` };
        }
        if (sessionId && input.sender_session_id !== sessionId)
            return { input: { ...input, sender_session_id: sessionId } };
    });
    pi.on("session_shutdown", async () => { generation++; tracker.stop(); await stopCurrent(); });
    pi.on("session_start", async (_event, ctx) => {
        const currentGeneration = ++generation;
        await stopCurrent();
        if (generation !== currentGeneration)
            return;
        onAgentEnd = () => { };
        onTurn = () => { };
        ensureRemoteTool();
        tracker.start(ctx);
        if (!ctx.hasUI && process.env.SUPERVISOR_AGENT_FORCE !== "1")
            return;
        if (process.env.SUPERVISOR_BOOTSTRAP !== "0" && resolveEndpoint().mode === "hub-local") {
            try {
                const wasRunning = await supervisorHealthy(localUrl());
                if (await ensureSupervisorService()) {
                    writeConnectionConfig();
                    if (!wasRunning && ctx.hasUI)
                        ctx.ui?.notify?.("Supervisor service started. The remote tool works now; run /mcp reconnect omp-supervisor:supervisor to load its MCP tools in this session.", "info");
                }
            } catch { }
        }
        if (generation !== currentGeneration)
            return;
        const notify = (text, level = "warning") => ctx.ui?.notify?.(text, level);
        const endpoint = resolveEndpoint();
        if (endpoint.invalid && !(USER_ENV.url && USER_ENV.token)) {
            notify("Supervisor enrollment unavailable: cannot read valid supervisor-connection.json. Check its JSON and permissions; credentials were not printed.");
            return;
        }
        const connection = endpoint.connection;
        const url = endpoint.url;
        const enrollToken = endpoint.token;
        if (!url || typeof enrollToken !== "string" || !enrollToken.trim()) {
            notify("Supervisor enrollment missing: set SUPERVISOR_URL and SUPERVISOR_TOKEN (or ~/.omp/agent/supervisor-connection.json), then resume the session. /supervisor status shows the current setup.");
            return;
        }
        try {
            const parsed = new URL(url);
            if (!["https:", "http:"].includes(parsed.protocol) || parsed.username || parsed.password)
                throw new Error("invalid URL");
        }
        catch {
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
            try {
                const current = ctx.models?.current?.() || ctx.model;
                if (current)
                    model = current.provider ? `${current.provider}/${current.id}` : current.id;
            }
            catch { }
            try {
                title = pi.getSessionName?.() || "";
            }
            catch { }
            return { host, session_id: ctx.sessionManager?.getSessionId?.() || "", cwd: ctx.cwd || process.cwd(), model, title };
        };
        let stopped = false, registrationBlocked = false, registrationPromise = null;
        let agentId = "", agentToken = "", heartbeatSeconds = Math.max(5, Math.floor(ttlSeconds / 3));
        const humanQueue = [], queuedHumanIds = new Set();
        let activeReply = null, injectChain = Promise.resolve();
        let startedLoops = false;
        const timers = new Set(), outcomes = new Map(), controller = new AbortController();
        selfIdentity = () => ({ agentId: stopped ? "" : agentId, sessionId: ctx.sessionManager?.getSessionId?.() || "" });
        const schedule = (fn, delay) => {
            if (stopped || registrationBlocked)
                return;
            const timer = ctx.setTimeout(async () => { timers.delete(timer); if (!stopped && !registrationBlocked)
                await fn(); }, delay);
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
            try {
                payload = await res.json();
            }
            catch { }
            if (!res.ok) {
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
            if (!id)
                return;
            try {
                await api("POST", `/api/agents/${encodeURIComponent(id)}/unregister`, token);
            }
            catch { }
        };
        stopCurrent = async () => {
            if (stopped)
                return;
            stopped = true;
            controller.abort();
            for (const timer of timers)
                ctx.clearTimer(timer);
            timers.clear();
            activeReply?.finish(null);
            humanQueue.length = 0;
            queuedHumanIds.clear();
            await unregister(agentId, agentToken);
        };
        const register = async () => {
            if (stopped || registrationBlocked)
                return;
            if (registrationPromise)
                return registrationPromise;
            registrationPromise = (async () => {
                const result = await api("POST", "/api/agents/register", enrollToken, {
                    name, machine, harness: "omp-tty", version: process.env.OMP_VERSION || "", pid: process.pid,
                    capabilities: ["prompt", "steer", "aside", "follow_up"],
                    labels: { platform: process.platform, surface: "interactive", client_protocol: "reply-ack-v2" },
                    ttl_seconds: ttlSeconds, ...identity(),
                });
                if (typeof result.agent_id !== "string" || !result.agent_id || typeof result.agent_token !== "string" || !result.agent_token)
                    throw new Error("Invalid registration response");
                if (stopped || generation !== currentGeneration) {
                    await unregister(result.agent_id, result.agent_token);
                    return;
                }
                agentId = result.agent_id;
                agentToken = result.agent_token;
                const interval = Number(result.heartbeat_interval_seconds);
                if (Number.isFinite(interval) && interval > 0)
                    heartbeatSeconds = interval;
                notify(`Supervisor: advertised ${name} (${agentId})`, "info");
            })();
            try {
                await registrationPromise;
            }
            finally {
                registrationPromise = null;
            }
        };
        const handleRegistrationFailure = error => {
            if (stopped)
                return;
            if (permanentRegistrationError(error)) {
                registrationBlocked = true;
                notify(`Supervisor enrollment rejected (HTTP ${error.status}). Check the configured enrollment credential, URL and server allowlist; automatic authentication retries stopped.`);
            }
            else
                notify("Supervisor temporarily unavailable; registration will retry with backoff.");
        };
        const recoverRegistration = async (error, failedId) => {
            if (stopped)
                return;
            if (error?.code === "unknown_agent") {
                if (agentId === failedId) {
                    agentId = "";
                    agentToken = "";
                }
                if (!agentId)
                    await register();
            }
            else if ([401, 403].includes(error?.status))
                handleRegistrationFailure(error);
        };
        const waitForReply = deliveryId => {
            let resolve, timer;
            const promise = new Promise(done => { resolve = done; });
            const waiter = {
                id: deliveryId,
                lastText: "",
                finish(outcome) {
                    if (activeReply !== waiter)
                        return;
                    clearTimeout(timer);
                    activeReply = null;
                    resolve(outcome);
                },
            };
            activeReply = waiter;
            timer = setTimeout(() => waiter.finish({ status: "error", reply: "", error: "Timed out waiting for the assistant reply; completion is unverified." }), ASSISTANT_REPLY_TIMEOUT_MS);
            return { promise, cancel: () => waiter.finish(null) };
        };
        onAgentEnd = event => {
            const waiter = activeReply;
            if (stopped || !waiter)
                return;
            const message = lastAssistant(event?.messages), text = assistantText(message);
            if (text)
                waiter.lastText = text;
            if (event?.willContinue === true)
                return;
            if (message?.stopReason === "aborted" || message?.stopReason === "error" || message?.errorMessage) {
                waiter.finish({ status: "error", reply: "", error: "Assistant turn failed or was interrupted; completion is unverified." });
            }
            else
                waiter.finish({ status: "ok", reply: waiter.lastText || "(no assistant text)", error: "" });
        };
        const remember = (id, outcome) => {
            outcomes.set(id, outcome);
            if (outcomes.size > MAX_DELIVERY_CACHE)
                outcomes.delete(outcomes.keys().next().value);
        };
        const ack = async (delivery, outcome) => {
            for (let attempt = 1; !stopped; attempt++) {
                try {
                    await api("POST", `/api/agents/${encodeURIComponent(agentId)}/ack`, agentToken, {
                        delivery_id: delivery.id, status: outcome.status, result: outcome.status === "ok" ? outcome.reply : "", error: outcome.error,
                    });
                    return;
                }
                catch (error) {
                    if (stopped)
                        return;
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
                if (Number.isFinite(ms) && ms > 0)
                    return ms;
            }
            return undefined;
        };
        const inject = (delivery, idle) => {
            const deliverAs = delivery.mode === "follow_up" ? "followUp" : delivery.mode === "aside" ? "aside" : "steer";
            const options = idle && deliverAs !== "aside" ? undefined : { deliverAs };
            let pending;
            try {
                const source = agentPromptSource(delivery.source);
                if (source) {
                    const from = source.from;
                    const fromPeer = {};
                    for (const key of ["name", "title", "route"])
                        if (source[key] !== undefined)
                            fromPeer[key] = source[key];
                    const details = { id: delivery.id, from, message: delivery.text };
                    const sentAt = sentAtMs(delivery);
                    if (sentAt !== undefined)
                        details.sentAt = sentAt;
                    if (Object.keys(fromPeer).length > 0)
                        details.fromPeer = fromPeer;
                    pi.sendMessage({
                        customType: "irc:incoming",
                        content: `<irc>\nIncoming message from agent \`${from}\`:\n\n${delivery.text}\n</irc>`,
                        display: true, attribution: "agent", details,
                    }, options ? { ...options, triggerTurn: true } : { triggerTurn: true });
                    const outcome = { status: "ok", reply: "", error: "" };
                    remember(delivery.id, outcome);
                    return { outcome, injected: true };
                }
                pending = waitForReply(delivery.id);
                Promise.resolve(pi.sendUserMessage(delivery.text, options)).catch(() => {
                    if (activeReply?.id === delivery.id)
                        activeReply.finish({ status: "error", reply: "", error: "Unable to inject Supervisor delivery." });
                });
                return { reply: pending.promise, injected: true };
            }
            catch (error) {
                pending?.cancel();
                const outcome = { status: "error", reply: "", error: error?.message === "Invalid agent message source" ? error.message : "Unable to inject Supervisor delivery." };
                remember(delivery.id, outcome);
                return { outcome };
            }
        };
        const serially = work => (injectChain = injectChain.then(work, work));
        const injectOne = async (delivery) => {
            const idle = ctx.isIdle?.() ?? true;
            const result = inject(delivery, idle);
            if (result.reply) {
                void result.reply.then(outcome => {
                    if (outcome == null || stopped)
                        return;
                    remember(delivery.id, outcome);
                    void serially(pumpHumanQueue);
                    return ack(delivery, outcome);
                });
            }
            const pendingAck = result.outcome ? ack(delivery, result.outcome) : undefined;
            if (result.injected && idle) {
                for (let waited = 0; waited < TURN_CLAIM_WAIT_MS && !stopped && (ctx.isIdle?.() ?? false); waited += 5)
                    await sleep(5);
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
        const deliverAll = async (deliveries) => {
            const acks = await serially(async () => {
                const pending = [];
                for (const delivery of deliveries) {
                    if (stopped)
                        break;
                    const cached = outcomes.get(delivery.id);
                    if (cached) {
                        pending.push(ack(delivery, cached));
                        continue;
                    }
                    if (delivery.source == null) {
                        if (activeReply?.id === delivery.id || queuedHumanIds.has(delivery.id))
                            continue;
                        if (activeReply || humanQueue.length > 0) {
                            humanQueue.push(delivery);
                            queuedHumanIds.add(delivery.id);
                            continue;
                        }
                    }
                    pending.push((await injectOne(delivery)).pendingAck);
                }
                return pending;
            });
            await Promise.all(acks ?? []);
        };
        let turnBusy = null, turnEndedAt = null, turnEndedMs = 0;
        const currentBusy = () => {
            if (turnBusy === false && Date.now() - turnEndedMs < TURN_END_GRACE_MS)
                return false;
            return !(ctx.isIdle?.() ?? true);
        };
        const heartbeat = async () => {
            if (stopped || registrationBlocked)
                return;
            const id = agentId;
            try {
                if (!id) {
                    await register();
                    return;
                }
                const body = { ...identity(), busy: currentBusy(), ttl_seconds: ttlSeconds };
                if (turnEndedAt)
                    body.turn_ended_at = turnEndedAt;
                await api("POST", `/api/agents/${encodeURIComponent(id)}/heartbeat`, agentToken, body);
            }
            catch (error) {
                try {
                    await recoverRegistration(error, id);
                }
                catch (registrationError) {
                    handleRegistrationFailure(registrationError);
                }
            }
        };
        onTurn = busy => {
            if (stopped)
                return;
            turnBusy = busy;
            if (!busy) {
                turnEndedMs = Date.now();
                turnEndedAt = new Date(turnEndedMs).toISOString();
            }
            if (agentId)
                void heartbeat();
        };
        let pollFailures = 0;
        const poll = async () => {
            if (stopped || registrationBlocked)
                return;
            let delay = 250;
            const id = agentId;
            try {
                if (!id)
                    await register();
                if (stopped || !agentId)
                    return;
                const result = await api("GET", `/api/agents/${encodeURIComponent(agentId)}/inbox?wait=${POLL_WAIT_SECONDS}`, agentToken);
                pollFailures = 0;
                await deliverAll(result.deliveries || []);
            }
            catch (error) {
                delay = retryDelay(pollFailures++, error.retryAfterMs);
                try {
                    await recoverRegistration(error, id);
                }
                catch (registrationError) {
                    handleRegistrationFailure(registrationError);
                }
            }
            schedule(poll, delay);
        };
        let registrationAttempts = 0;
        const start = async () => {
            if (stopped || registrationBlocked)
                return;
            try {
                await register();
            }
            catch (error) {
                handleRegistrationFailure(error);
                schedule(start, retryDelay(registrationAttempts++, error.retryAfterMs));
                return;
            }
            if (stopped || !agentId || startedLoops)
                return;
            startedLoops = true;
            timers.add(ctx.setInterval(heartbeat, heartbeatSeconds * 1000));
            schedule(poll, 250);
        };
        await start();
    });
}
