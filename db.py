"""SQLite persistence for test configs, per-second metrics, and computed insights."""
import json
import sqlite3
from contextlib import contextmanager

DB_PATH = "loadblast.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS tests (
    test_id    TEXT PRIMARY KEY,
    config     TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    done       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS metrics (
    test_id      TEXT NOT NULL,
    ts           INTEGER NOT NULL,
    rps          INTEGER NOT NULL,
    p50          REAL NOT NULL,
    p95          REAL NOT NULL,
    p99          REAL NOT NULL,
    errors       INTEGER NOT NULL,
    concurrency  INTEGER NOT NULL,
    status_codes TEXT NOT NULL,
    FOREIGN KEY (test_id) REFERENCES tests(test_id)
);
CREATE INDEX IF NOT EXISTS idx_metrics_test_id ON metrics(test_id);

CREATE TABLE IF NOT EXISTS summaries (
    test_id       TEXT PRIMARY KEY,
    total_requests INTEGER NOT NULL,
    total_errors   INTEGER NOT NULL,
    p50            REAL NOT NULL,
    p95            REAL NOT NULL,
    p99            REAL NOT NULL,
    status_codes   TEXT NOT NULL,
    FOREIGN KEY (test_id) REFERENCES tests(test_id)
);

CREATE TABLE IF NOT EXISTS insights (
    test_id         TEXT PRIMARY KEY,
    summary_text    TEXT NOT NULL,
    degradation_ts  INTEGER,
    degradation_sec INTEGER,
    baseline_p95    REAL,
    peak_rps        INTEGER,
    error_rate      REAL,
    FOREIGN KEY (test_id) REFERENCES tests(test_id)
);
"""


def init_db() -> None:
    with _connect() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def save_test(test_id: str, config: dict, created_at: int) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT INTO tests (test_id, config, created_at, done) VALUES (?, ?, ?, 0)",
            (test_id, json.dumps(config), created_at),
        )


def save_metric(test_id: str, metric: dict) -> None:
    with _connect() as conn:
        conn.execute(
            """INSERT INTO metrics
               (test_id, ts, rps, p50, p95, p99, errors, concurrency, status_codes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (test_id, metric["ts"], metric["rps"], metric["p50"], metric["p95"],
             metric["p99"], metric["errors"], metric["concurrency"],
             json.dumps(metric["status_codes"])),
        )


def save_summary(test_id: str, summary: dict) -> None:
    with _connect() as conn:
        conn.execute(
            """INSERT INTO summaries
               (test_id, total_requests, total_errors, p50, p95, p99, status_codes)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (test_id, summary["total_requests"], summary["total_errors"],
             summary["p50"], summary["p95"], summary["p99"],
             json.dumps(summary["status_codes"])),
        )
        conn.execute("UPDATE tests SET done = 1 WHERE test_id = ?", (test_id,))


def save_insight(test_id: str, insight: dict) -> None:
    with _connect() as conn:
        conn.execute(
            """INSERT INTO insights
               (test_id, summary_text, degradation_ts, degradation_sec,
                baseline_p95, peak_rps, error_rate)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (test_id, insight["summary_text"], insight.get("degradation_ts"),
             insight.get("degradation_sec"), insight.get("baseline_p95"),
             insight.get("peak_rps"), insight.get("error_rate")),
        )


def get_test(test_id: str) -> dict | None:
    with _connect() as conn:
        test_row = conn.execute(
            "SELECT * FROM tests WHERE test_id = ?", (test_id,)
        ).fetchone()
        if not test_row:
            return None

        metric_rows = conn.execute(
            "SELECT * FROM metrics WHERE test_id = ? ORDER BY ts ASC", (test_id,)
        ).fetchall()
        summary_row = conn.execute(
            "SELECT * FROM summaries WHERE test_id = ?", (test_id,)
        ).fetchone()
        insight_row = conn.execute(
            "SELECT * FROM insights WHERE test_id = ?", (test_id,)
        ).fetchone()

        return {
            "test_id": test_row["test_id"],
            "config": json.loads(test_row["config"]),
            "done": bool(test_row["done"]),
            "metrics": [
                {
                    "ts": r["ts"], "rps": r["rps"], "p50": r["p50"], "p95": r["p95"],
                    "p99": r["p99"], "errors": r["errors"], "concurrency": r["concurrency"],
                    "status_codes": json.loads(r["status_codes"]),
                }
                for r in metric_rows
            ],
            "summary": (
                {
                    "total_requests": summary_row["total_requests"],
                    "total_errors": summary_row["total_errors"],
                    "p50": summary_row["p50"], "p95": summary_row["p95"],
                    "p99": summary_row["p99"],
                    "status_codes": json.loads(summary_row["status_codes"]),
                }
                if summary_row else None
            ),
            "insight": (
                {
                    "summary_text": insight_row["summary_text"],
                    "degradation_ts": insight_row["degradation_ts"],
                    "degradation_sec": insight_row["degradation_sec"],
                    "baseline_p95": insight_row["baseline_p95"],
                    "peak_rps": insight_row["peak_rps"],
                    "error_rate": insight_row["error_rate"],
                }
                if insight_row else None
            ),
        }
