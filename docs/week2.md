# Week 2 — Control API + WebSocket Streaming

A FastAPI server wraps the worker. POST a test config to start a test, then connect a WebSocket to stream live metrics as they arrive. The worker runs as a local subprocess — no Docker or Kubernetes yet.

---

## Prerequisites

- Python 3.11+
- `pip install -r requirements.txt`

## Run

**Terminal 1 — something to test against:**
```bash
python -m http.server 8080
```

**Terminal 2 — the API:**
```bash
uvicorn api:app --reload
```

## Fire a test

**Terminal 3:**
```bash
# Start the test
TEST_ID=$(curl -s -X POST http://localhost:8000/tests \
  -H "Content-Type: application/json" \
  -d '{"url":"http://localhost:8080","concurrency":20,"duration":15,"ramp_up":5}' \
  | python -c "import sys,json; print(json.load(sys.stdin)['test_id'])")

echo "Test started: $TEST_ID"

# Stream live metrics
python -c "
import asyncio
from aiohttp import ClientSession

async def stream():
    async with ClientSession() as s:
        async with s.ws_connect('ws://localhost:8000/tests/$TEST_ID/metrics') as ws:
            async for msg in ws:
                print(msg.data)

asyncio.run(stream())
"
```

## API

| Endpoint | Description |
|---|---|
| `POST /tests` | Start a test. Returns `{"test_id": "..."}` |
| `WS /tests/{id}/metrics` | Stream live metrics. Closes when test finishes. |

**POST /tests body:**

| Field | Type | Description |
|---|---|---|
| `url` | string | Target URL |
| `concurrency` | int | Requests in-flight at a time |
| `duration` | int | Test length in seconds |
| `ramp_up` | int | Seconds to reach full concurrency (default: 0) |

## Output

Same JSON format as Week 1, delivered over WebSocket instead of stdout:

```
{"ts": 1720000011, "rps": 312, "p50": 44, "p95": 118, "p99": 280, "errors": 0, "concurrency": 20, "status_codes": {"200": 312}}
...
{"summary": true, "total_requests": 4521, "total_errors": 0, "p50": 45, "p95": 119, "p99": 284, "status_codes": {"200": 4521}}
```

The WebSocket closes automatically when the test finishes. Multiple clients can connect to the same test and each receives the full stream independently.

## How it works

```
POST /tests
  → spawns main.py as a subprocess
  → returns test_id immediately

WS /tests/{id}/metrics
  → each connection gets its own asyncio.Queue
  → background task reads subprocess stdout line-by-line
  → fans each JSON line into all subscriber queues
  → sends None sentinel when subprocess exits
```
