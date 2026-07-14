# LoadBlast

Distributed load-testing platform. A swarm of containerized workers hammers a target URL in parallel, streams live metrics back to a dashboard, and runs automated degradation analysis when the test completes.

---

## Weekly build log

| Week | What was built | Guide |
|---|---|---|
| 1 | Standalone async worker — fires concurrent requests, prints per-second metrics to stdout | [docs/week1.md](docs/week1.md) |
| 2 | Control API — accepts test configs over HTTP, runs worker as subprocess, streams metrics over WebSocket | [docs/week2.md](docs/week2.md) |
| 3 | Kubernetes + Redis — worker containerized as a K8s Job, metrics move from stdout to Redis pub/sub | [docs/week3.md](docs/week3.md) |

## Architecture

```
Browser
  │
  ▼
Control API (FastAPI)          ← uvicorn api:app
  │  └── WebSocket /tests/{id}/metrics
  │
  ├── creates ──► K8s Job (worker pod)
  │                   └── publishes metrics to Redis
  │
  └── subscribes ──► Redis pub/sub
                         channel: metrics:<test_id>
```

## Stack

| Layer | Technology |
|---|---|
| Worker | Python + asyncio + aiohttp |
| Control API | Python + FastAPI |
| Metrics transport | Redis pub/sub |
| Persistence | Postgres (upcoming) |
| Orchestration | Kubernetes (minikube for local dev) |
| Frontend | Upcoming — Week 5 |
