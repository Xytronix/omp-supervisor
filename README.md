# omp-supervisor

Durable job control plane for [omp](https://github.com/can1357/oh-my-pi) sessions. Spawn jobs on any machine, steer input into them, message finished jobs to continue their session, and let agents talk to each other. Jobs survive the session that started them.

## Install

```
omp plugin marketplace add Xytronix/omp-supervisor
omp plugin install omp-supervisor@omp-supervisor
```

On first session start the extension downloads the supervisor binary for your platform (linux/darwin, amd64/arm64), verifies its checksum against the release, starts it on `127.0.0.1:18200` and writes `~/.omp/agent/supervisor-connection.json`. Set `SUPERVISOR_URL`/`SUPERVISOR_TOKEN` to point at a remote or already running supervisor instead; local bootstrap then steps aside. `SUPERVISOR_BOOTSTRAP=0` disables bootstrap entirely.

## What you get

- MCP tools `run`, `status`, `result`, `steer`, `message`, `prompt`, `jobs`, `events`, `models`, `agents`, `sessions`, `schedule`, `cancel` and more, served by the supervisor at `/mcp`.
- Interactive registration: your TUI sessions appear in `agents`, and other agents can prompt or steer them while you watch.
- Finished jobs are reusable: steering or messaging a terminal job continues its session in a fresh job.

## Configuration

Environment, all optional: `FASTMCP_PORT` (default 8000 upstream, 18200 in the plugin bootstrap), `SUPERVISOR_DATA_DIR` (default `~/.omp/supervisor`), `SUPERVISOR_REGISTRY_TOKEN` or `SUPERVISOR_REGISTRY_TOKEN_FILE` for enrollment, `SUPERVISOR_PUBLIC_BASE_URL` for Grok webhook callbacks, `SUPERVISOR_CAPTURE_FORBIDDEN` to block a path subtree from the capture archive. The service binds loopback by default; `FASTMCP_HOST=0.0.0.0` opts into remote access. The MCP endpoint is unauthenticated job control: never expose it on an untrusted network.

## Build from source

```
cd plugins/supervisor/src && go build -o supervisor .
```

MIT licensed. Binaries in [releases](https://github.com/Xytronix/omp-supervisor/releases) are built from the vendored source in this repository.
