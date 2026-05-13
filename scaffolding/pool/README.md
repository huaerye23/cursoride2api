# RATLC pool — user guide

A three-process stack that serves the Anthropic Messages API by holding a *pool of pre-warmed* Cursor agent streams. Pay the retry lottery once per channel; reap fast responses for the rest of each stream's lifetime.

## What runs

| Process | What it does | Restart cost |
|---|---|---|
| **`pool-manager.mjs`** | Forks N `bridge-worker` child processes, maintains the pool, pings idle channels every 20 min, auto-respawns dead ones. Listens on `/tmp/ratlc-pool.sock`. | High — restarting loses all warm channels and re-pays the lottery N times |
| **`bridge-worker.mjs`** *(child)* | Owns one Cursor stream. Managed by the pool — you never start these directly. | Automatic respawn |
| **`api-server.mjs`** | HTTP `:4242` serving `/v1/messages` (Anthropic format). Stateless — translates incoming requests to pool commands. | **Free** — restart anytime, pool stays up |
| **`ratlc-ctl.mjs`** | CLI for status / scaling. | n/a |

## Bring it up

```bash
# Start (default 2 channels — first ready in 1-5 min, second follows sequentially after)
./scaffolding/pool/start.sh

# Or pick a size
./scaffolding/pool/start.sh 5
```

## Watch it warm up

```bash
node scaffolding/pool/ratlc-ctl.mjs watch 2
```

Refreshes every 2 seconds. You'll see channels transition through `spawning` → `opening` → `ready`. They open one at a time (sequential) to avoid tripping Cursor's account-wide `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED`.

Or one-shot snapshot:
```bash
node scaffolding/pool/ratlc-ctl.mjs status
```

## Use claude-code against it

The pool's tool contract is **set by the first request's `tools` field**. Once set, subsequent requests must send the same tools (or the pool recycles). For real claude-code usage, drop `--bare` so the full toolset comes through.

```bash
env -i HOME=$HOME PATH=$PATH TERM=xterm \
  ANTHROPIC_BASE_URL=http://127.0.0.1:4242 \
  ANTHROPIC_API_KEY=anything \
  claude -p --dangerously-skip-permissions "list files in /tmp"
```

For a clean text-only test (no tools, never triggers a pool recycle):

```bash
echo "What's 2+2?" | env -i HOME=$HOME PATH=$PATH TERM=xterm \
  ANTHROPIC_BASE_URL=http://127.0.0.1:4242 \
  ANTHROPIC_API_KEY=anything \
  claude --bare -p
```

## Scale at runtime

```bash
node scaffolding/pool/ratlc-ctl.mjs ramp-up 3      # add 3 channels
node scaffolding/pool/ratlc-ctl.mjs ramp-down 2    # remove 2 (kills idle ones first)
node scaffolding/pool/ratlc-ctl.mjs restart-channel ch-0   # force a single channel to respawn
```

Channels added via `ramp-up` open sequentially behind any in-progress opens.

## Restart the API server without losing the pool

```bash
# The api-server is stateless — kill and restart freely
kill $(cat /tmp/ratlc-api.pid)
nohup node scaffolding/pool/api-server.mjs > /tmp/ratlc-api.log 2>&1 &
echo $! > /tmp/ratlc-api.pid
```

The next claude-code request will reconnect to the same warm pool.

## Stop everything

```bash
node scaffolding/pool/ratlc-ctl.mjs shutdown
# or hard kill
kill $(cat /tmp/ratlc-pool.pid /tmp/ratlc-api.pid)
```

## Logs

| File | What's in it |
|---|---|
| `/tmp/ratlc-pool.log` | Pool manager + worker stdout/stderr (state transitions, retry lottery progress, ping results) |
| `/tmp/ratlc-api.log` | API server (HTTP requests, pool socket events) |

Live-tail either:
```bash
tail -f /tmp/ratlc-pool.log /tmp/ratlc-api.log
```

## Health behaviors

- **Idle ping every 20 min** — any `ready` channel idle that long gets a tiny health-check round-trip ("[health-check] Reply with exactly: OK"). Reset on success; channel killed and respawned on failure.
- **Heartbeat every 30s** — workers report state to manager. Used to detect hung processes.
- **Auto-respawn on exit** — worker process exiting (crash, OOM, shutdown signal) triggers a new spawn to maintain `currentTargetSize`.
- **Rate-limit handling** — workers back off exponentially (5s → 60s) on `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED`. Combined with sequential opens, this keeps the upstream account quiet.

## Status fields explained

```
CHANNEL   ch-N         logical id, monotonically increasing
STATE     spawning     forked, not yet started open
          opening      running the retry-until-success lottery
          ready        parked in bajie_yield, awaiting user message
          busy         serving a request OR holding a tool_use
          dead         fatal error, will be respawned
PID                    OS pid of the worker process
OPEN_ATT               how many retry attempts the lottery has taken
AGE                    time since the channel opened (first lottery win)
IDLE                   time since the last activity (last text/yield/tool)
ROUNDS                 successful user-message→yield cycles served
CURRENT                request id currently in flight on this channel
ERROR                  most recent error message (if any)
```
