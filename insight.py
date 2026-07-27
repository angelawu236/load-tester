"""Post-test analysis: detect the degradation point in a metrics time series and
generate a plain-language summary.

Degradation heuristic: compare each tick's p95 latency and error rate against a
baseline (median of the first BASELINE_TICKS ticks). The degradation point is the
first tick where p95 latency exceeds LATENCY_MULTIPLIER x baseline, or the error
rate exceeds ERROR_RATE_THRESHOLD, sustained for SUSTAIN_TICKS consecutive ticks.
SUSTAIN_TICKS=1 flags a spike immediately — load tests are short enough (seconds
to minutes) that even a one-tick blip usually reflects a real event worth
surfacing, not sensor noise.
"""
import statistics

BASELINE_TICKS = 5
LATENCY_MULTIPLIER = 2.0
ERROR_RATE_THRESHOLD = 0.05
SUSTAIN_TICKS = 1


def _error_rate(tick: dict) -> float:
    return tick["errors"] / tick["rps"] if tick["rps"] else 0.0


def _is_degraded(tick: dict, baseline_p95: float) -> bool:
    if baseline_p95 > 0 and tick["p95"] > baseline_p95 * LATENCY_MULTIPLIER:
        return True
    return _error_rate(tick) > ERROR_RATE_THRESHOLD


def find_degradation_point(metrics: list[dict]) -> dict | None:
    """Return the first sustained degraded tick, or None if the test stayed stable."""
    if len(metrics) <= BASELINE_TICKS:
        return None

    baseline_ticks = metrics[:BASELINE_TICKS]
    baseline_p95 = statistics.median(t["p95"] for t in baseline_ticks)

    streak = 0
    for i in range(BASELINE_TICKS, len(metrics)):
        if _is_degraded(metrics[i], baseline_p95):
            streak += 1
            if streak >= SUSTAIN_TICKS:
                onset = metrics[i - SUSTAIN_TICKS + 1]
                return {"tick": onset, "baseline_p95": baseline_p95}
        else:
            streak = 0
    return None


def compute_stats(metrics: list[dict]) -> dict:
    return {
        "peak_rps": max((t["rps"] for t in metrics), default=0),
        "avg_rps": round(statistics.mean(t["rps"] for t in metrics)) if metrics else 0,
        "peak_concurrency": max((t["concurrency"] for t in metrics), default=0),
        "overall_error_rate": (
            sum(t["errors"] for t in metrics) / sum(t["rps"] for t in metrics)
            if metrics and sum(t["rps"] for t in metrics) else 0.0
        ),
    }


def render_summary(metrics: list[dict], summary: dict) -> str:
    """Build a plain-language description from a template, filled with the
    computed stats and (if any) degradation point."""
    if not metrics:
        return "No metrics were recorded for this test."

    stats = compute_stats(metrics)
    degradation = find_degradation_point(metrics)
    start_ts = metrics[0]["ts"]

    if degradation is None:
        return (
            f"The system stayed stable for the full test, handling up to "
            f"{stats['peak_rps']:,} req/s at {stats['peak_concurrency']} concurrent "
            f"users with p95 latency around {round(statistics.median(t['p95'] for t in metrics))}ms "
            f"and an overall error rate of {stats['overall_error_rate']:.1%}."
        )

    tick = degradation["tick"]
    baseline_p95 = degradation["baseline_p95"]
    elapsed = tick["ts"] - start_ts
    multiplier = round(tick["p95"] / baseline_p95, 1) if baseline_p95 else None
    error_rate = _error_rate(tick)

    reason = (
        f"p95 latency jumped to {tick['p95']}ms ({multiplier}x the {round(baseline_p95)}ms baseline)"
        if baseline_p95 > 0 and tick["p95"] > baseline_p95 * LATENCY_MULTIPLIER
        else f"the error rate rose to {error_rate:.1%}"
    )

    return (
        f"The system handled up to {stats['peak_rps']:,} req/s cleanly, then began "
        f"degrading about {elapsed}s into the test at {tick['concurrency']} concurrent "
        f"users — {reason}. Peak concurrency reached {stats['peak_concurrency']} "
        f"with an overall error rate of {stats['overall_error_rate']:.1%}."
    )


def compute_insight(metrics: list[dict], summary: dict) -> dict:
    """Full insight record for persistence: summary text + the raw numbers behind it."""
    stats = compute_stats(metrics)
    degradation = find_degradation_point(metrics)
    return {
        "summary_text": render_summary(metrics, summary),
        "degradation_ts": degradation["tick"]["ts"] if degradation else None,
        "degradation_sec": (
            degradation["tick"]["ts"] - metrics[0]["ts"] if degradation and metrics else None
        ),
        "baseline_p95": degradation["baseline_p95"] if degradation else None,
        "peak_rps": stats["peak_rps"],
        "error_rate": stats["overall_error_rate"],
    }
