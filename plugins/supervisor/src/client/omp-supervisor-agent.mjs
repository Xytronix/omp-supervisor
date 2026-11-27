#!/usr/bin/env node
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";
export function rpcDeliveryCommand(delivery, busy) {
    const command = { type: "prompt", message: delivery.text };
    if (busy || delivery.mode === "aside") {
        command.streamingBehavior = delivery.mode === "follow_up" ? "followUp" : delivery.mode === "aside" ? "aside" : "steer";
    }
    const source = agentPromptSource(delivery.source);
    if (source)
        command.source = source;
    return command;
}
function agentPromptSource(source) {
    if (source == null)
        return undefined;
    if (typeof source !== "object" || Array.isArray(source)) {
        throw new Error("Invalid agent message source");
    }
    if (source.kind !== "agent" || typeof source.from !== "string" || source.from.trim() === "") {
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
async function runBridge() {
    const argv = process.argv.slice(2);
    function flag(name, fallback = "") {
        const idx = argv.indexOf(`--${name}`);
        if (idx >= 0 && idx + 1 < argv.length)
            return argv[idx + 1];
        const inline = argv.find((a) => a.startsWith(`--${name}=`));
        if (inline)
            return inline.slice(name.length + 3);
        return fallback;
    }
    function hasFlag(name) {
        return argv.includes(`--${name}`);
    }
    const config = {
        url: (flag("url") || process.env.SUPERVISOR_URL || "").replace(/\/+$/, ""),
        token: flag("token") || process.env.SUPERVISOR_TOKEN || "",
        machine: flag("machine") || process.env.SUPERVISOR_MACHINE || os.hostname(),
        name: flag("name") || process.env.SUPERVISOR_AGENT_NAME || "",
        model: flag("model") || process.env.SUPERVISOR_AGENT_MODEL || "",
        cwd: path.resolve(flag("cwd") || process.cwd()),
        prompt: flag("prompt") || "",
        ttl: Number(flag("ttl") || process.env.SUPERVISOR_TTL_SECONDS || 90),
        ompBin: process.env.OMP_BIN || "omp",
        yolo: hasFlag("yolo"),
        verbose: hasFlag("verbose") || process.env.SUPERVISOR_AGENT_VERBOSE === "1",
    };
    const hostIdentity = {
        hostname: os.hostname(),
        platform: process.platform,
        architecture: process.arch,
    };
    if (!config.url || !config.token) {
        console.error("SUPERVISOR_URL and SUPERVISOR_TOKEN are required.\n" +
            "  SUPERVISOR_URL=https://supervisor.example.com SUPERVISOR_TOKEN=<enrollment token> node omp-supervisor-agent.mjs");
        process.exit(2);
    }
    if (!config.name)
        config.name = `${config.machine}:${path.basename(config.cwd)}`;
    const log = (...args) => console.error("[supervisor-agent]", ...args);
    const debug = (...args) => config.verbose && log(...args);
    const ompArgs = ["--mode", "rpc", "--cwd", config.cwd];
    if (config.model)
        ompArgs.push(`--model=${config.model}`);
    if (config.yolo)
        ompArgs.push("--auto-approve");
    const child = spawn(config.ompBin, ompArgs, {
        cwd: config.cwd,
        stdio: ["pipe", "pipe", "inherit"],
        env: process.env,
    });
    child.on("error", (err) => {
        log(`failed to start ${config.ompBin}: ${err.message}`);
        process.exit(1);
    });
    let seq = 0;
    const pending = new Map();
    let ready = false;
    const readyWaiters = [];
    let lastAssistantText = "";
    const turnWaiters = [];
    function sendCommand(cmd, timeoutMs = 30_000) {
        const id = `req_${++seq}`;
        const line = JSON.stringify({ id, ...cmd }) + "\n";
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                pending.delete(id);
                reject(new Error(`rpc command ${cmd.type} timed out`));
            }, timeoutMs);
            pending.set(id, (frame) => {
                clearTimeout(timer);
                resolve(frame);
            });
            child.stdin.write(line);
        });
    }
    function onReady() {
        return ready ? Promise.resolve() : new Promise((resolve) => readyWaiters.push(resolve));
    }
    function waitForTurn(timeoutMs) {
        return new Promise((resolve) => {
            const waiter = { resolve, timer: null };
            waiter.timer = setTimeout(() => {
                const idx = turnWaiters.indexOf(waiter);
                if (idx >= 0)
                    turnWaiters.splice(idx, 1);
                resolve(null);
            }, timeoutMs);
            turnWaiters.push(waiter);
        });
    }
    function textFromMessages(messages) {
        if (!Array.isArray(messages))
            return "";
        for (let i = messages.length - 1; i >= 0; i--) {
            const msg = messages[i];
            if (msg?.role !== "assistant")
                continue;
            const content = msg.content;
            if (typeof content === "string")
                return content;
            if (Array.isArray(content)) {
                const parts = content.filter((c) => c?.type === "text" && c.text?.trim()).map((c) => c.text);
                if (parts.length)
                    return parts.join("\n");
            }
        }
        return "";
    }
    function handleFrame(frame) {
        if (frame.type === "ready") {
            ready = true;
            while (readyWaiters.length)
                readyWaiters.shift()();
            return;
        }
        if (frame.type === "response" && frame.id && pending.has(frame.id)) {
            pending.get(frame.id)(frame);
            pending.delete(frame.id);
            return;
        }
        if (frame.type === "agent_end") {
            const text = textFromMessages(frame.messages);
            if (text)
                lastAssistantText = text;
            if (frame.isTerminal !== false) {
                while (turnWaiters.length) {
                    const waiter = turnWaiters.shift();
                    clearTimeout(waiter.timer);
                    waiter.resolve(text || lastAssistantText);
                }
            }
            return;
        }
        if (frame.type === "extension_ui_request") {
            child.stdin.write(JSON.stringify({ type: "extension_ui_response", id: frame.id, cancelled: true }) + "\n");
        }
    }
    let buffer = "";
    child.stdout.on("data", (chunk) => {
        buffer += chunk.toString("utf8");
        let idx;
        while ((idx = buffer.indexOf("\n")) >= 0) {
            const line = buffer.slice(0, idx).trim();
            buffer = buffer.slice(idx + 1);
            if (!line)
                continue;
            try {
                handleFrame(JSON.parse(line));
            }
            catch {
                debug("unparsed frame:", line.slice(0, 200));
            }
        }
    });
    async function getState() {
        try {
            const frame = await sendCommand({ type: "get_state" }, 15_000);
            return frame?.data ?? {};
        }
        catch (err) {
            debug(`get_state failed: ${err.message}`);
            return {};
        }
    }
    let agentId = "";
    let agentToken = "";
    let heartbeatSeconds = Math.max(5, Math.floor(config.ttl / 3));
    let stopping = false;
    async function api(method, urlPath, { token, body, signal } = {}) {
        const res = await fetch(`${config.url}${urlPath}`, {
            method,
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${token}`,
            },
            body: body === undefined ? undefined : JSON.stringify(body),
            signal,
        });
        let payload = {};
        try {
            payload = await res.json();
        }
        catch {
        }
        if (!res.ok) {
            const err = new Error(payload.error || `HTTP ${res.status}`);
            err.status = res.status;
            err.code = payload.code;
            throw err;
        }
        return payload;
    }
    async function register() {
        const state = await getState();
        const body = {
            name: config.name,
            machine: config.machine,
            host: hostIdentity,
            harness: "omp-rpc-bridge",
            version: process.version,
            session_id: state.sessionId || "",
            cwd: config.cwd,
            model: state.model?.id ? `${state.model.provider}/${state.model.id}` : "",
            title: state.sessionName || "",
            pid: process.pid,
            capabilities: ["prompt", "steer", "aside", "follow_up", "result"],
            labels: { platform: process.platform, arch: process.arch },
            ttl_seconds: config.ttl,
        };
        const res = await api("POST", "/api/agents/register", { token: config.token, body });
        agentId = res.agent_id;
        agentToken = res.agent_token;
        heartbeatSeconds = res.heartbeat_interval_seconds || heartbeatSeconds;
        log(`registered as ${agentId} (machine=${config.machine}, session=${body.session_id || "pending"})`);
    }
    async function heartbeatLoop() {
        while (!stopping) {
            await sleep(heartbeatSeconds * 1000);
            if (stopping)
                return;
            const state = await getState();
            try {
                await api("POST", `/api/agents/${agentId}/heartbeat`, {
                    token: agentToken,
                    body: {
                        host: hostIdentity,
                        session_id: state.sessionId || "",
                        model: state.model?.id ? `${state.model.provider}/${state.model.id}` : "",
                        title: state.sessionName || "",
                        busy: Boolean(state.isStreaming),
                        ttl_seconds: config.ttl,
                    },
                });
            }
            catch (err) {
                if (err.code === "unknown_agent") {
                    log("registration expired; registering again");
                    await safeRegister();
                }
                else {
                    debug(`heartbeat failed: ${err.message}`);
                }
            }
        }
    }
    async function inboxLoop() {
        while (!stopping) {
            try {
                const res = await api("GET", `/api/agents/${agentId}/inbox?wait=25`, { token: agentToken });
                for (const delivery of res.deliveries || []) {
                    await deliver(delivery);
                }
            }
            catch (err) {
                if (err.code === "unknown_agent") {
                    log("registration expired; registering again");
                    await safeRegister();
                }
                else if (!stopping) {
                    debug(`inbox poll failed: ${err.message}`);
                    await sleep(3000);
                }
            }
        }
    }
    async function deliver(delivery) {
        log(`delivering ${delivery.id} (${delivery.mode})`);
        const state = await getState();
        let command;
        try {
            command = rpcDeliveryCommand(delivery, Boolean(state.isStreaming));
            const ack = await sendCommand(command, 60_000);
            if (ack.success === false)
                throw new Error(ack.error || "omp rejected the prompt");
        }
        catch (err) {
            await ackDelivery(delivery.id, "error", "", err.message);
            return;
        }
        const text = await waitForTurn(30 * 60 * 1000);
        await ackDelivery(delivery.id, "ok", text || lastAssistantText || "(no assistant text)", "");
    }
    async function ackDelivery(deliveryId, status, result, error) {
        try {
            await api("POST", `/api/agents/${agentId}/ack`, {
                token: agentToken,
                body: { delivery_id: deliveryId, status, result: result.slice(0, 30_000), error },
            });
        }
        catch (err) {
            debug(`ack failed: ${err.message}`);
        }
    }
    async function safeRegister() {
        for (let attempt = 1;; attempt++) {
            try {
                await register();
                return;
            }
            catch (err) {
                if (err.status === 401 || err.status === 403) {
                    log(`registration refused: ${err.message}`);
                    process.exit(3);
                }
                const wait = Math.min(60, 2 ** Math.min(attempt, 5));
                log(`registration failed (${err.message}); retrying in ${wait}s`);
                await sleep(wait * 1000);
            }
        }
    }
    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    async function shutdown(code = 0) {
        if (stopping)
            return;
        stopping = true;
        if (agentId && agentToken) {
            try {
                await api("POST", `/api/agents/${agentId}/unregister`, { token: agentToken });
                log("unregistered");
            }
            catch (err) {
                debug(`unregister failed: ${err.message}`);
            }
        }
        try {
            child.stdin.end();
        }
        catch {
        }
        setTimeout(() => process.exit(code), 500);
    }
    process.on("SIGINT", () => shutdown(0));
    process.on("SIGTERM", () => shutdown(0));
    child.on("exit", (code) => {
        log(`omp exited with code ${code}`);
        shutdown(code ?? 0);
    });
    await onReady();
    if (config.prompt) {
        await sendCommand({ type: "prompt", message: config.prompt }, 60_000);
    }
    await safeRegister();
    log(`polling ${config.url}/api/agents/${agentId}/inbox`);
    void heartbeatLoop();
    await inboxLoop();
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
    await runBridge();
}
