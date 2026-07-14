# Week 3 — Kubernetes + Redis

The worker is containerized and runs as a Kubernetes Job. Metrics move from stdout to Redis pub/sub — the worker pod publishes, the API subscribes and fans metrics out to WebSocket clients exactly as before.

---

## Prerequisites

- Python 3.11+
- Docker Desktop
- minikube + kubectl + helm (`brew install minikube kubectl helm`)

## One-time cluster setup

```bash
# Start the cluster
minikube start

# Deploy Redis (no auth, no replicas — local dev only)
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm install redis bitnami/redis \
  --set auth.enabled=false \
  --set replica.replicaCount=0

# Wait for Redis to be ready
kubectl get pods -w
# Press Ctrl-C once redis-master-0 shows Running
```

## Build the worker image

Run this in a terminal where minikube's Docker daemon is active. Images built here are available to the cluster without a registry.

```bash
eval $(minikube docker-env)
docker build -t loadblast-worker:latest .
```

> **Important:** `eval $(minikube docker-env)` only affects the current terminal session. Re-run it if you open a new terminal and need to rebuild.

## Run

**Terminal 1 — forward Redis to localhost:**
```bash
kubectl port-forward svc/redis-master 6379:6379
```
Keep this running. The API connects to Redis through it.

**Terminal 2 — the API (normal shell, not minikube's docker env):**
```bash
pip install -r requirements.txt
uvicorn api:app --reload
```

## Fire a test

**Terminal 3:**
```bash
# Start the test
TEST_ID=$(curl -s -X POST http://localhost:8000/tests \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com","concurrency":10,"duration":15}' \
  | python -c "import sys,json; print(json.load(sys.stdin)['test_id'])")

echo "Test started: $TEST_ID"

# Watch the worker pod spin up
kubectl get jobs -w

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

## Inspect Redis directly

```bash
# Exec into the Redis pod
kubectl exec -it redis-master-0 -- redis-cli

# Subscribe to all metrics channels (run before firing a test)
PSUBSCRIBE metrics:*
```

## How it works

```
POST /tests
  → creates a K8s Job (loadblast-worker:latest)
    worker pod: main.py --url ... --concurrency ... --test-id <id>
    REDIS_HOST=redis-master (K8s internal service)

Worker pod
  → publishes JSON lines to Redis channel: metrics:<test_id>

api.py (local, connected via port-forward)
  → subscribes to metrics:<test_id>
  → fans each line into WebSocket subscriber queues
  → deletes the K8s Job when summary line arrives
```

**Why `imagePullPolicy: Never`?** The worker image is built directly into minikube's Docker daemon, so there's no registry to pull from. This flag tells Kubernetes to use what's already present locally.

## Teardown

```bash
minikube stop        # pause the cluster
minikube delete      # wipe it entirely
```
