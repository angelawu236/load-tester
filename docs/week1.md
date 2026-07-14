# Week 1 — Standalone Worker

The worker is a single Python script. No API, no Docker, no Kubernetes. Takes a URL, fires concurrent async requests for a set duration, and prints per-second metrics to stdout.

---

## Prerequisites

- Python 3.11+
- `pip install aiohttp`

## Run

```bash
python main.py --url URL --concurrency N --duration S [--ramp-up S]
```

| Flag | Description |
|---|---|
| `--url` | Target URL |
| `--concurrency` | Requests in-flight at a time |
| `--duration` | Test length in seconds |
| `--ramp-up` | Seconds to reach full concurrency (default: 0) |

**Quick local test:**
```bash
# Terminal 1 — something to hit
python -m http.server 8080

# Terminal 2 — run the load test
python main.py --url http://localhost:8080 --concurrency 20 --duration 15 --ramp-up 5
```

## Output

One JSON line per second, then a final summary:

```
{"ts": 1720000011, "rps": 312, "p50": 44, "p95": 118, "p99": 280, "errors": 0, "concurrency": 20, "status_codes": {"200": 312}}
{"ts": 1720000012, "rps": 298, "p50": 47, "p95": 121, "p99": 290, "errors": 0, "concurrency": 20, "status_codes": {"200": 298}}
{"summary": true, "total_requests": 4521, "total_errors": 0, "p50": 45, "p95": 119, "p99": 284, "status_codes": {"200": 4521}}
```

| Field | Description |
|---|---|
| `rps` | Requests completed that second |
| `p50/p95/p99` | Latency percentiles in milliseconds |
| `errors` | Non-2xx responses + connection failures |
| `concurrency` | Active virtual users at that moment |
| `status_codes` | Breakdown of HTTP response codes |

## How it works

One `asyncio` event loop runs three coroutine layers:

```
orchestrate()         — ramps up workers, waits for deadline, cancels all
  └── vu_worker() ×N  — each holds one request in-flight, loops until done
reporter_loop()       — wakes every 1s, snapshots results, prints JSON
```

Workers share a plain `dict`. The per-second window swap in `reporter_loop` is atomic — asyncio is single-threaded so no locks are needed.
