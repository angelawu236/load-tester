#!/usr/bin/env python3
import asyncio
import json
import uuid
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from kubernetes import client as k8s, config as k8s_config
from pydantic import BaseModel

WORKER_IMAGE = "loadblast-worker:latest"
REDIS_HOST_IN_CLUSTER = "redis-master"   # K8s service name, used by worker pods
REDIS_HOST_LOCAL = "localhost"           # port-forwarded, used by api.py

# test_id → {queues: list[Queue], done: bool}
tests: dict[str, dict] = {}
batch_v1: k8s.BatchV1Api | None = None


class TestConfig(BaseModel):
    url: str
    concurrency: int
    duration: int
    ramp_up: int = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    global batch_v1
    k8s_config.load_kube_config()
    batch_v1 = k8s.BatchV1Api()
    yield
    for test_id, test in tests.items():
        if not test["done"]:
            _delete_job(test_id)


app = FastAPI(lifespan=lifespan)


def _create_job(test_id: str, config: TestConfig) -> None:
    batch_v1.create_namespaced_job(
        namespace="default",
        body=k8s.V1Job(
            metadata=k8s.V1ObjectMeta(name=f"worker-{test_id}"),
            spec=k8s.V1JobSpec(
                ttl_seconds_after_finished=60,
                template=k8s.V1PodTemplateSpec(
                    spec=k8s.V1PodSpec(
                        restart_policy="Never",
                        containers=[k8s.V1Container(
                            name="worker",
                            image=WORKER_IMAGE,
                            image_pull_policy="Never",
                            args=[
                                "--url", config.url,
                                "--concurrency", str(config.concurrency),
                                "--duration", str(config.duration),
                                "--ramp-up", str(config.ramp_up),
                                "--test-id", test_id,
                            ],
                            env=[k8s.V1EnvVar(name="REDIS_HOST", value=REDIS_HOST_IN_CLUSTER)],
                        )],
                    )
                ),
            ),
        ),
    )


def _delete_job(test_id: str) -> None:
    try:
        batch_v1.delete_namespaced_job(
            name=f"worker-{test_id}",
            namespace="default",
            body=k8s.V1DeleteOptions(propagation_policy="Foreground"),
        )
    except Exception:
        pass  # already deleted or ttl'd


async def _pipe_from_redis(test_id: str) -> None:
    """Subscribe to worker metrics on Redis and fan out to all WebSocket queues."""
    r = aioredis.Redis(host=REDIS_HOST_LOCAL, port=6379, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(f"metrics:{test_id}")

    test = tests[test_id]
    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        line = message["data"]
        for q in list(test["queues"]):
            await q.put(line)
        if json.loads(line).get("summary"):
            break

    await pubsub.unsubscribe(f"metrics:{test_id}")
    await r.aclose()
    test["done"] = True
    for q in list(test["queues"]):
        await q.put(None)  # sentinel: stream finished
    _delete_job(test_id)


@app.post("/tests")
async def start_test(config: TestConfig):
    test_id = str(uuid.uuid4())
    _create_job(test_id, config)
    tests[test_id] = {"queues": [], "done": False}
    asyncio.create_task(_pipe_from_redis(test_id))
    return {"test_id": test_id}


@app.websocket("/tests/{test_id}/metrics")
async def metrics_ws(websocket: WebSocket, test_id: str):
    await websocket.accept()

    if test_id not in tests:
        await websocket.close(code=4004, reason="unknown test_id")
        return

    test = tests[test_id]
    queue: asyncio.Queue[str | None] = asyncio.Queue()
    test["queues"].append(queue)

    # Guard: test finished between POST and WS connect — no sentinel is coming
    if test["done"]:
        test["queues"].remove(queue)
        await websocket.close(code=4000, reason="test already finished")
        return

    try:
        while True:
            line = await queue.get()
            if line is None:
                await websocket.close()
                break
            await websocket.send_text(line)
    except WebSocketDisconnect:
        pass
    finally:
        if queue in test["queues"]:
            test["queues"].remove(queue)
