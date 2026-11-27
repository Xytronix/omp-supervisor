import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
export function launchOptions(input, home = os.homedir()) {
    if (!input || typeof input !== "object" || Array.isArray(input))
        throw new Error("Launch configuration must be an object");
    for (const key of ["cwd", "model", "thinking", "max_time", "session_id", "parent_session_id"]) {
        if (input[key] !== undefined && (typeof input[key] !== "string" || input[key].includes("\0")))
            throw new Error(`Invalid ${key}`);
    }
    for (const key of ["continue", "yolo"])
        if (input[key] !== undefined && typeof input[key] !== "boolean")
            throw new Error(`Invalid ${key}`);
    const cwd = path.resolve(input.cwd || home);
    const args = ["--mode=rpc", `--cwd=${cwd}`];
    if (input.model)
        args.push(`--model=${input.model}`);
    if (input.thinking)
        args.push(`--thinking=${input.thinking}`);
    if (input.session_id)
        args.push("-r", input.session_id);
    else if (input.continue)
        args.push("-c");
    else if (input.parent_session_id)
        args.push("--parent-session-id", input.parent_session_id);
    if (input.yolo === true)
        args.push("--auto-approve");
    if (cwd === path.resolve(home))
        args.push("--allow-home");
    const duration = input.max_time || "6h";
    const match = /^(\d+)(ms|s|m|h)$/.exec(duration);
    if (!match)
        throw new Error("max_time must use ms, s, m, or h");
    const timeout = Number(match[1]) * { ms: 1, s: 1000, m: 60000, h: 3600000 }[match[2]];
    if (!Number.isSafeInteger(timeout) || timeout < 1000 || timeout > 86400000)
        throw new Error("max_time must be 1 second to 24 hours");
    args.push(`--max-time=${duration}`);
    return { cwd, args, timeout };
}
export function supportsParentSessionId(binary) {
    const probe = spawnSync(binary, ["--parent-session-id", "omp:probe", "--version"], { encoding: "utf8", timeout: 10000, windowsHide: true });
    return `${probe.stdout ?? ""}${probe.stderr ?? ""}`.includes("--parent-session-id requires");
}
export function withoutParentSessionId(args) {
    const i = args.indexOf("--parent-session-id");
    return i < 0 ? args : [...args.slice(0, i), ...args.slice(i + 2)];
}
function ompJson(binary, args) {
    const run = spawnSync(binary, args, { encoding: "utf8", timeout: 60000, maxBuffer: 64 * 1024 * 1024, windowsHide: true });
    if (run.status !== 0)
        return undefined;
    try {
        return JSON.parse(run.stdout);
    }
    catch {
        return undefined;
    }
}
export function modelInventory(binary, input) {
    const roles = ompJson(binary, ["config", "get", "modelRoles"]);
    const chains = ompJson(binary, ["config", "get", "retry.fallbackChains"]);
    const out = { type: "models", roles: roles && typeof roles === "object" ? roles : {}, fallbackChains: chains && typeof chains === "object" ? chains : {} };
    if (input.catalog === true) {
        const catalog = ompJson(binary, ["models", "--json"]);
        out.models = Array.isArray(catalog?.models) ? catalog.models : [];
    }
    return out;
}
export function modelPolicy(binary) {
    const policy = ompJson(binary, ["models", "--policy", "--json"]);
    return { type: "policy", policy: policy && typeof policy === "object" && "configured" in policy ? policy : null };
}
const SESSION_ID = /^[A-Za-z0-9-]{8,64}$/;
function messageText(content) {
    if (typeof content === "string")
        return content;
    if (!Array.isArray(content))
        return "";
    return content.filter(part => part && part.type === "text" && typeof part.text === "string" && part.text).map(part => part.text).join("\n");
}
export function findSessionFile(base, id) {
    const suffix = `_${id}.jsonl`;
    let entries;
    try {
        entries = fs.readdirSync(base, { withFileTypes: true });
    }
    catch {
        return "";
    }
    for (const entry of entries) {
        const p = path.join(base, entry.name);
        if (entry.isFile() && entry.name.endsWith(suffix))
            return p;
        if (!entry.isDirectory())
            continue;
        let kids;
        try {
            kids = fs.readdirSync(p, { withFileTypes: true });
        }
        catch {
            continue;
        }
        for (const kid of kids)
            if (kid.isFile() && kid.name.endsWith(suffix))
                return path.join(p, kid.name);
    }
    return "";
}
export async function sessionConversation(input, home = os.homedir()) {
    const id = typeof input.session_id === "string" ? input.session_id.trim().replace(/^omp:/i, "").trim() : "";
    if (!SESSION_ID.test(id))
        return { type: "session", found: false, error: "Invalid session_id" };
    const file = findSessionFile(path.join(home, ".omp", "agent", "sessions"), id);
    if (!file)
        return { type: "session", found: false };
    const maxMessages = Math.max(1, Math.min(40, Number(input.max_messages) || 20));
    const maxChars = Math.max(1, Math.min(8000, Number(input.max_chars) || 4000));
    const stat = fs.statSync(file);
    const session = { session_id: "", title: "", mtime: stat.mtime.toISOString().replace(/\.\d{3}Z$/, "Z"), size: stat.size };
    const messages = [];
    let used = 0, firstUser = "";
    const lines = createInterface({ input: fs.createReadStream(file, { encoding: "utf8" }), crlfDelay: Infinity });
    for await (const raw of lines) {
        const line = raw.trim();
        if (!line)
            continue;
        let ev;
        try {
            ev = JSON.parse(line);
        }
        catch {
            continue;
        }
        if (ev.type === "session") {
            if (typeof ev.id === "string")
                session.session_id = ev.id;
            if (typeof ev.cwd === "string")
                session.cwd = ev.cwd;
        }
        else if (ev.type === "title" && typeof ev.title === "string" && ev.title.trim())
            session.title = ev.title.trim();
        else if (ev.type === "model_change" && typeof ev.model === "string" && ev.model) {
            session.model = ev.model;
            if (typeof ev.resolvedModelIsFallback === "boolean")
                session.fallback = ev.resolvedModelIsFallback;
        }
        else if (ev.type === "thinking_level_change" && typeof ev.thinkingLevel === "string")
            session.thinking = ev.thinkingLevel;
        else if (ev.type === "message" && (ev.message?.role === "user" || ev.message?.role === "assistant")) {
            const text = messageText(ev.message.content).trim();
            if (!text)
                continue;
            if (!firstUser && ev.message.role === "user")
                firstUser = text;
            if (messages.length >= maxMessages || used >= maxChars)
                continue;
            const clipped = text.slice(0, maxChars - used);
            messages.push({ role: ev.message.role, text: clipped });
            used += clipped.length;
        }
    }
    if (session.session_id !== id)
        return { type: "session", found: false, error: `Session file header is ${session.session_id || "missing"}, not ${id}` };
    if (!session.title)
        session.title = firstUser.split("\n")[0].slice(0, 80) || id;
    return { type: "session", found: true, session, messages };
}
export function childEnv(env = process.env) {
    return { ...env, OMP_MODEL_POLICY: "enforce" };
}
export async function runHost() {
    const home = os.homedir();
    const binary = process.env.OMP_BIN || (process.platform === "win32"
        ? path.join(home, "AppData", "Local", "omp", "omp.exe") : path.join(home, ".local", "bin", "omp"));
    const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
    let child, deadline, killTimer, initializing = true, closing = false, timedOut = false;
    const stop = () => {
        if (closing)
            return;
        closing = true;
        if (!child) {
            process.exitCode = 1;
            lines.close();
            return;
        }
        child.stdin.end();
        killTimer = setTimeout(() => {
            if (process.platform === "win32")
                spawnSync("taskkill.exe", ["/PID", String(child.pid), "/T", "/F"], { stdio: "ignore", timeout: 5000 });
            else {
                try {
                    process.kill(-child.pid, "SIGTERM");
                }
                catch { }
            }
            setTimeout(() => { if (process.platform !== "win32") {
                try {
                    process.kill(-child.pid, "SIGKILL");
                }
                catch { }
            } }, 1500).unref();
        }, 2000);
        killTimer.unref();
    };
    process.on("SIGTERM", stop);
    process.on("SIGINT", stop);
    process.stdin.on("end", stop);
    lines.on("line", line => {
        if (closing)
            return;
        if (Buffer.byteLength(line) > 16 * 1024 * 1024) {
            process.stderr.write("RPC frame exceeds limit\n");
            stop();
            return;
        }
        if (initializing) {
            initializing = false;
            try {
                if (Buffer.byteLength(line) > 65536)
                    throw new Error("Launch configuration exceeds limit");
                const input = JSON.parse(line);
                if (input && input.op === "policy") {
                    process.stdout.write(`${JSON.stringify(modelPolicy(binary))}\n`);
                    lines.close();
                    process.stdin.destroy();
                    return;
                }
                if (input && input.op === "models") {
                    process.stdout.write(`${JSON.stringify(modelInventory(binary, input))}\n`);
                    lines.close();
                    process.stdin.destroy();
                    return;
                }
                if (input && input.op === "session") {
                    lines.close();
                    process.stdin.destroy();
                    sessionConversation(input, home).then(reply => process.stdout.write(`${JSON.stringify(reply)}\n`), error => process.stdout.write(`${JSON.stringify({ type: "session", found: false, error: `Session read failed: ${error?.message || error}` })}\n`));
                    return;
                }
                const options = launchOptions(input);
                if (options.args.includes("--parent-session-id") && !supportsParentSessionId(binary)) {
                    process.stderr.write("OMP does not accept --parent-session-id; launching without session lineage\n");
                    options.args = withoutParentSessionId(options.args);
                }
                if (!fs.statSync(options.cwd).isDirectory())
                    throw new Error("Working directory is not a directory");
                child = spawn(binary, options.args, { cwd: options.cwd, env: childEnv(), stdio: ["pipe", "pipe", "inherit"], detached: process.platform !== "win32", windowsHide: true });
                child.stdout.pipe(process.stdout);
                child.on("error", error => { process.stderr.write(`OMP launch failed: ${error.message}\n`); process.exitCode = 1; lines.close(); process.stdin.destroy(); clearTimeout(deadline); });
                child.on("exit", code => { clearTimeout(deadline); clearTimeout(killTimer); lines.close(); process.stdin.destroy(); process.exitCode = timedOut ? 124 : code ?? 1; });
                child.stdin.on("error", stop);
                deadline = setTimeout(() => { timedOut = true; process.stderr.write("Remote agent deadline exceeded\n"); stop(); }, options.timeout);
            }
            catch (error) {
                process.stderr.write(`Remote launch rejected: ${error.message}\n`);
                process.exitCode = 1;
                lines.close();
                process.stdin.destroy();
            }
        }
        else if (child) {
            if (!child.stdin.write(line + "\n")) {
                process.stdin.pause();
                child.stdin.once("drain", () => process.stdin.resume());
            }
        }
    });
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href)
    await runHost();
