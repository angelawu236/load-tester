#!/usr/bin/env python3
"""
LoadBlast worker: async HTTP load generator.
Usage: python main.py --url URL --concurrency N --duration S [--ramp-up S] --test-id ID
"""
import argparse
import asyncio
import json
import os
import time
from collections import Counter

import aiohttp
import redis.asyncio as aioredis


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Async HTTP load tester")
    p.add_argument("--url", required=True, help="Target URL")
    p.add_argument("--concurrency", type=int, required=True, help="Number of concurrent requests in-flight at a time")
    p.add_argument("--duration", type=int, required=True, help="Test duration in seconds")
    p.add_argument("--ramp-up", type=int, default=0, dest="ramp_up",
                   help="Seconds to ramp up to full concurrency (default: 0)")
    p.add_argument("--test-id", required=True, dest="test_id",
                   help="Unique test ID; metrics published to Redis channel metrics:<test-id>")
    return p.parse_args()


def percentile(sorted_samples: list[float], pct: float) -> float:
    """Return the Pth percentile from a pre-sorted list via linear interpolation."""
    if not sorted_samples:
        return 0.0
    k = (len(sorted_samples) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(sorted_samples) - 1)
    return sorted_samples[lo] + (k - lo) * (sorted_samples[hi] - sorted_samples[lo])


def build_metrics_line(snapshot: list, active_vus: int) -> str:
    latencies = sorted(s[0] for s in snapshot)
    errors = sum(1 for s in snapshot if s[2])
    code_counter = Counter(str(s[1]) for s in snapshot if s[1] != 0)
    return json.dumps({
        "ts": int(time.time()),
        "rps": len(snapshot),
        "p50": round(percentile(latencies, 50)),
        "p95": round(percentile(latencies, 95)),
        "p99": round(percentile(latencies, 99)),
        "errors": errors,
        "concurrency": active_vus,
        "status_codes": dict(code_counter),
    })


async def publish_final_summary(state: dict) -> None:
    all_results = state["all_results"]
    if not all_results:
        return
    latencies = sorted(s[0] for s in all_results)
    errors = sum(1 for s in all_results if s[2])
    code_counter = Counter(str(s[1]) for s in all_results if s[1] != 0)
    await state["redis"].publish(state["channel"], json.dumps({
        "summary": True,
        "total_requests": len(all_results),
        "total_errors": errors,
        "p50": round(percentile(latencies, 50)),
        "p95": round(percentile(latencies, 95)),
        "p99": round(percentile(latencies, 99)),
        "status_codes": dict(code_counter),
    }))


async def make_request(state: dict, session: aiohttp.ClientSession) -> None:
    t0 = asyncio.get_event_loop().time()
    status = 0
    is_error = False
    try:
        async with session.get(state["url"]) as resp:
            await resp.read()
            latency_ms = (asyncio.get_event_loop().time() - t0) * 1000
            status = resp.status
            is_error = not (200 <= status < 300)
    except aiohttp.ClientError:
        latency_ms = (asyncio.get_event_loop().time() - t0) * 1000
        is_error = True
    record = (latency_ms, status, is_error)
    state["current_window"].append(record)
    state["all_results"].append(record)


async def vu_worker(state: dict, session: aiohttp.ClientSession) -> None:
    state["active_vus"] += 1
    try:
        while not state["done"]:
            await make_request(state, session)
    except asyncio.CancelledError:
        pass
    finally:
        state["active_vus"] -= 1


async def reporter_loop(state: dict) -> None:
    while not state["done"]:
        await asyncio.sleep(1)
        snapshot = state["current_window"]
        state["current_window"] = []
        await state["redis"].publish(state["channel"], build_metrics_line(snapshot, state["active_vus"]))


async def orchestrate(args: argparse.Namespace, state: dict,
                      session: aiohttp.ClientSession) -> None:
    tasks = []
    interval = args.ramp_up / args.concurrency if args.ramp_up > 0 else 0
    deadline = asyncio.get_event_loop().time() + args.duration

    for _ in range(args.concurrency):
        if interval > 0:
            await asyncio.sleep(interval)
        tasks.append(asyncio.create_task(vu_worker(state, session)))

    remaining = deadline - asyncio.get_event_loop().time()
    if remaining > 0:
        await asyncio.sleep(remaining)

    state["done"] = True
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    args = parse_args()
    state: dict = {
        "url": args.url,
        "channel": f"metrics:{args.test_id}",
        "redis": None,
        "current_window": [],
        "all_results": [],
        "active_vus": 0,
        "done": False,
    }

    async def run() -> None:
        redis = aioredis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379)
        state["redis"] = redis
        connector = aiohttp.TCPConnector(limit=args.concurrency + 10, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(connect=5, sock_read=10)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            reporter = asyncio.create_task(reporter_loop(state))
            try:
                await orchestrate(args, state, session)
            except asyncio.CancelledError:
                state["done"] = True
            finally:
                reporter.cancel()
                await asyncio.gather(reporter, return_exceptions=True)
                await publish_final_summary(state)
                await redis.aclose()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
