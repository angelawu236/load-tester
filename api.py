#!/usr/bin/env python3
import asyncio
import sys
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

# test_id → {process, queues: list[Queue], done: bool}
tests: dict[str, dict] = {}


class TestConfig(BaseModel):
    url: str
    concurrency: int
    duration: int
    ramp_up: int = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    for t in tests.values():
        if t["process"].returncode is None:
            t["process"].terminate()


app = FastAPI(lifespan=lifespan)


async def _pipe_to_queues(test_id: str, process: asyncio.subprocess.Process) -> None:
    """Read worker stdout and broadcast each JSON line to all subscribed WebSocket queues."""
    test = tests[test_id]
    async for raw in process.stdout:
        line = raw.decode().strip()
        if line:
            for q in list(test["queues"]):
                await q.put(line)
    await process.wait()
    test["done"] = True
    for q in list(test["queues"]):
        await q.put(None)  # sentinel: stream finished


@app.post("/tests")
async def start_test(config: TestConfig):
    test_id = str(uuid.uuid4())
    process = await asyncio.create_subprocess_exec(
        sys.executable, "main.py",
        "--url", config.url,
        "--concurrency", str(config.concurrency),
        "--duration", str(config.duration),
        "--ramp-up", str(config.ramp_up),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    tests[test_id] = {"process": process, "queues": [], "done": False}
    asyncio.create_task(_pipe_to_queues(test_id, process))
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

    # Guard: if test finished between POST and WS connect, no sentinel is coming
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
