# omp-supervisor

Durable job control plane for [omp](https://github.com/can1357/oh-my-pi) sessions. Spawn jobs on any machine, steer input into them, message finished jobs to continue their session, and let agents talk to each other. Jobs survive the session that started them.

## Install

```
omp plugin marketplace add Xytronix/omp-supervisor
omp plugin install omp-supervisor@omp-supervisor
```

## Single host

Nothing to configure. On the first interactive session the extension downloads the supervisor binary for your platform (linux, darwin or windows; amd64 or arm64), verifies it against the release `SHA256SUMS`, starts it on `127.0.0.1:18200` and writes `~/.omp/agent/supervisor-connection.json`. Later sessions reuse the running service and start it again if it is gone. `SUPERVISOR_BOOTSTRAP=0` disables bootstrap. Headless sessions (print, RPC, subagents) skip bootstrap and registration unless `SUPERVISOR_AGENT_FORCE=1`.

`/supervisor status` shows the mode (`hub-local` or `client`), URL, health, whether `/mcp` requires a token, the token source, the registered agent id, and how remote jobs are tracked.

## Multiple hosts: one hub, many clients

Run the service on one machine (the hub) and point every other omp at it.

On the hub, bind the service to a private address, ideally its Tailscale IP or another address only your machines can reach:

```
/supervisor hub 100.x.y.z
```

This saves the listen host, restarts the plugin-started service on it (a service the plugin did not start is left alone), updates the hub's own connection file, and prints the client setup. Restart omp on the hub afterwards. `/supervisor hub` with no argument goes back to loopback. `/supervisor hub` and `/supervisor target` are hub-admin commands: on a client of another hub they refuse and change nothing.

On each client, copy the hub's token file (`~/.omp/supervisor/registry-token` on the hub, printed by `/supervisor hub`) to a private file, then set two variables in your shell profile:

```sh
export SUPERVISOR_URL=http://100.x.y.z:18200
export SUPERVISOR_TOKEN="$(cat ~/.omp/supervisor/hub-token)"
```

```powershell
$env:SUPERVISOR_URL = "http://100.x.y.z:18200"
$env:SUPERVISOR_TOKEN = (Get-Content "$HOME/.omp/supervisor/hub-token" -Raw).Trim()
```

Environment variables are the supported client mechanism: they reach the plugin's MCP server entry (`${SUPERVISOR_URL}` and `${SUPERVISOR_TOKEN}` in `.mcp.json`), the `remote` tool and session registration. Putting `url` and `token` into `~/.omp/agent/supervisor-connection.json` instead also works today, because the extension exports them before omp connects MCP servers, but it relies on that load order. A client whose `SUPERVISOR_URL` is not its own local hub address never starts a local service.

Auth: with the default `SUPERVISOR_MCP_AUTH=auto`, `/mcp` requires `Authorization: Bearer <registry token>` whenever the service listens on a non-loopback address, and answers 401 otherwise. `/health` stays open. `required` and `off` force it either way. Treat the token like a password: anyone holding it can run jobs on every spawn target. Keep the hub on a private network; never bind `0.0.0.0` on a host with a public interface.

## Spawn targets

The hub runs jobs on other machines over ssh. On the hub:

```
/supervisor target add mac my-mac
/supervisor target add win my-windows-box --os windows
/supervisor target list
/supervisor target remove mac
```

`target add` checks key-based `ssh` (BatchMode), finds `node` on the target (Node 18 or newer, Windows included), copies the bundled `omp-rpc-host.mjs` to `~/.omp/agent/` (`%USERPROFILE%\.omp\agent\` on Windows), adds or replaces the entry in `~/.omp/supervisor/machines.json` (or `SUPERVISOR_REMOTE_MACHINES_FILE`), and confirms the Supervisor lists it. The target needs omp installed (`~/.local/bin/omp`, `%LOCALAPPDATA%\omp\omp.exe`, or `OMP_BIN`). Spawn there with `remote` op `spawn` and `machine` set to the name.

## Windows

The hub, clients and spawn targets all run on Windows. The bootstrap uses `supervisor-windows-amd64.exe` or `supervisor-windows-arm64.exe` and starts it without a console window. For a Windows spawn target, enable the OpenSSH server and install Node.js and omp on it.

## The `remote` tool

The plugin adds a `remote` tool with the ops `spawn`, `status`, `result`, `cancel`, `steer`, `message`, `schedule`, `extend`, `events`, `jobs`, `machines`, `models`, `list`, `agent`, `prompt`, `sessions` and `session`, mapped onto the Supervisor MCP tools. If omp already ships a native `remote` tool, the plugin leaves it in place and keeps its own job tracking off, because omp tracks those jobs itself; `/supervisor jobs` then lists this session's jobs read-only from the hub.

`extend` raises or lowers a running job's time limit: pass the job in `to` and exactly one of `maxTime` (new total limit from the job's start, such as `6h`) or `add` (added to the current deadline, such as `2h`).

A spawned job is tracked by the session that started it, and its result is delivered automatically. How the job appears depends on what the running omp offers extensions; the plugin picks the first that applies:

- **omp versions with `ctx.agents`** (the external-agent API): each job is a real subagent. It appears in the agent roster and HUD, its transcript opens in the transcript viewer and follows the job live, `wait` blocks on it and receives its result once, cancelling it cancels the remote job, and messages sent to it (`agent://<id>`) reach the job as steer or follow-up input.
- **omp versions with `ctx.asyncJobs`** (the background job hook): each job is a background job. It shows up in `wait` and the job list with a live progress line (current tool or last assistant line, and the model), cancelling it cancels the remote job, and its result arrives like any background job result.
- **Any other omp**: the plugin polls its running jobs, shows them in a widget with the same live progress line, and delivers each result as a follow-up message that starts a new turn. Print mode may exit before a job finishes; read it later with `remote` op `result`.

In every case, `/supervisor jobs` lists the jobs this session started, and `/supervisor transcript <job id or name>` opens a job's transcript (assistant text, tool calls, shortened tool results, model and thinking changes) in a full-screen view that follows the job live while it runs. Progress and transcripts come from the Supervisor `events` tool. `/supervisor status` shows the mode, URL and MCP endpoint with their sources, who owns `remote`, and which job tracking is active.

The session stores only job ids and event cursors, so a resumed session keeps tracking its jobs.

## Running alongside a standalone client

If `~/.omp/agent/extensions/supervisor-agent.js` exists (the standalone Supervisor client used by fork builds of omp), the plugin stays inactive and shows one warning, because both would register the session and deliver messages twice. Delete that file or uninstall the plugin.

## Configuration

Environment, all optional: `OMP_BIN` (omp used for jobs; default is `~/.local/bin/omp`, then `omp` on PATH, then `/usr/local/bin` and `/usr/bin`), `SUPERVISOR_URL` and `SUPERVISOR_TOKEN` (client; they win over `url` and `token` in `~/.omp/agent/supervisor-connection.json`), `SUPERVISOR_MCP_URL` (MCP endpoint for `remote` and `/supervisor`; otherwise omp's `supervisor.mcpUrl` setting, then `<url>/mcp`; the registry token is sent only when the endpoint is on the hub's own origin), `SUPERVISOR_PORT` (local service port, default 18200), `SUPERVISOR_MCP_AUTH` (`auto`, `required`, `off`), `SUPERVISOR_DATA_DIR` (default `~/.omp/supervisor`), `SUPERVISOR_REGISTRY_TOKEN` or `SUPERVISOR_REGISTRY_TOKEN_FILE`, `SUPERVISOR_REMOTE_MACHINES_FILE`, `SUPERVISOR_PUBLIC_BASE_URL` for Grok webhook callbacks, `SUPERVISOR_CAPTURE_FORBIDDEN` to block a path subtree from the capture archive, `SUPERVISOR_CAPTURE_COALESCE_MS` (default 1000; keeps at most one tool progress snapshot per tool call per window in job captures, `0` for a byte-exact capture), `SUPERVISOR_CAPTURE_COMPRESS` (default on; finished captures become `<job_id>.log.gz`, read transparently by `events`, `0` disables).

## Build from source

```
cd plugins/supervisor/src && go build -o supervisor .
```

MIT licensed. Binaries in [releases](https://github.com/Xytronix/omp-supervisor/releases) are built from the vendored source in this repository.
