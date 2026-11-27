# Interactive Supervisor client: deployment and diagnosis

The server's remote-machine launcher and an independently opened interactive
OMP session are separate paths. A successful `run(machine=...)` does not prove
that a terminal has enrolled. Verify that terminal's session ID and PID in
`agents`, then verify a later heartbeat.

## Install and activate

Distribute the version of `supervisor-agent.extension.js` shipped with the
Supervisor release to `~/.omp/agent/extensions/supervisor-agent.js`. Back up the
previous client. Server deployment alone does not update host copies. Gracefully
quit and resume the existing OMP session to load a changed extension; preserve
the session ID, model and any unrelated processes. `/reload-plugins` is not an
extension-code reload.

The client reads `~/.omp/agent/supervisor-connection.json` at every session start.
Its keys are `url`, `machine`, and `token`. `SUPERVISOR_URL`, `SUPERVISOR_MACHINE`
and `SUPERVISOR_TOKEN` override those values. Provision credentials through an
approved administrator action; restrict the file to its owner. Never commit or
print enrollment credentials, nor replace authentication with anonymous access.

Updated clients advertise `aside` in `capabilities`. A registration lacking that
exact capability rejects aside input before queue mutation, with an error asking
you to restart the session using the updated Supervisor bridge. Distribute the
updated client, then gracefully quit/resume as above; do not retry by relabeling
the registration. `labels.client_protocol=reply-ack-v2` remains a diagnostic
label, not the capability gate.

Missing or invalid configuration produces an actionable local warning. Temporary
network/server errors retry with capped exponential backoff and jitter; HTTP
Retry-After is honored. Authentication/configuration rejections stop automated
retries and require correction by an authorized operator, followed by resume.

## Delivery is not proof of completed work

OMP's `sendUserMessage` with explicit `"steer"` or `"followUp"` is queue-only
even when idle, so the client omits those modes when idle. `"aside"` instead
starts a turn when idle and injects at an agent-step boundary when busy, without
interrupting the current tool batch. Ordinary peer IRC uses aside; parent/control
input can steer, and an explicit busy follow-up waits until the agent would
otherwise finish/yield, not merely its next tool iteration.

Peer-origin messages remain custom agent-attributed context, never human
instructions. Authoritative `source.kind`/`source.from` supply provenance;
optional sender name/title/route are display-only. A delivery without agent
source remains a genuine human prompt.

Legacy `injected into session as followUp` acknowledgments prove injection only.
For human/API prompts this client waits for a terminal assistant response, one
prompt at a time, so each reply belongs to its own prompt. Deadline/abort produces
an error rather than a stale successful response. Peer (agent-source) messages are
acked with an empty result as soon as they are injected; `prompt{wait_for:"ack"}`
to a peer proves delivery only, and the peer replies with its own message. Failed
acks retry with backoff because the server never redelivers a claimed delivery;
cached outcomes prevent duplicate injection. Verify actual file changes, upload
IDs, sizes and checksums separately: assistant text is not an artifact validator.

Shutdown cleanup is installed before the first registration request. A response
that arrives after shutdown is unregistered and cannot start orphan timers.

## Distinguish external blockers

A ChatGPT/tool safety denial is not a Supervisor authentication error. Changing
executors or using a subagent must not bypass that denial. An app's action
permission setting also does not disable platform safety checks. Scheduled
connector work separately requires persistent app access when requested.

Drive `rateLimitExceeded` is an API quota issue; respect Retry-After, throttle
requests and use bounded retries. A `file://` URL is local to the Google tool's
host. A Windows E: path is not accessible from a Linux-hosted connector. Use an
approved private transfer/staging path or an authorized uploader on Windows;
never publish private files at unauthenticated URLs as a workaround.

## Verification

Run all client tests with `bun test client` and server tests with `go test ./...`.
The regression suite covers idle/busy routing, provenance, initial registration
retry, missing credentials, session-start configuration refresh, late response
cleanup, reply deadlines and aborted turns.
