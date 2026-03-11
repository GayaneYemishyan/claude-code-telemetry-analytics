#!/usr/bin/env python3
"""
ETL pipeline: Ingest Claude Code telemetry JSONL + employee CSV into SQLite.

Reads the nested JSONL batches, flattens events into structured tables,
joins with employee metadata, and stores everything in a normalized SQLite DB.

Usage:
    python ingest.py [--input-dir output] [--db telemetry.db]
"""

import argparse
import csv
import json 
import sqlite3
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
-- Employee dimension table
CREATE TABLE IF NOT EXISTS employees (
    email           TEXT PRIMARY KEY,
    full_name       TEXT NOT NULL,
    practice        TEXT NOT NULL,
    level           TEXT NOT NULL,
    location        TEXT NOT NULL
);

-- Session dimension (derived from events)
CREATE TABLE IF NOT EXISTS sessions (
    session_id      TEXT PRIMARY KEY,
    user_email      TEXT NOT NULL,
    terminal_type   TEXT,
    started_at      TEXT,
    ended_at        TEXT,
    event_count     INTEGER DEFAULT 0,
    FOREIGN KEY (user_email) REFERENCES employees(email)
);

-- Unified events fact table (common fields for all event types)
CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type      TEXT NOT NULL,       -- e.g. 'api_request', 'tool_decision'
    event_timestamp TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    user_email      TEXT NOT NULL,
    user_id         TEXT,
    organization_id TEXT,
    terminal_type   TEXT,
    -- resource / host info
    host_arch       TEXT,
    host_name       TEXT,
    os_type         TEXT,
    os_version      TEXT,
    service_version TEXT,
    user_practice   TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id),
    FOREIGN KEY (user_email) REFERENCES employees(email)
);

-- API request details
CREATE TABLE IF NOT EXISTS api_requests (
    event_id                INTEGER PRIMARY KEY,
    model                   TEXT NOT NULL,
    input_tokens            INTEGER,
    output_tokens           INTEGER,
    cache_read_tokens       INTEGER,
    cache_creation_tokens   INTEGER,
    cost_usd                REAL,
    duration_ms             INTEGER,
    FOREIGN KEY (event_id) REFERENCES events(id)
);

-- Tool decision details
CREATE TABLE IF NOT EXISTS tool_decisions (
    event_id    INTEGER PRIMARY KEY,
    tool_name   TEXT NOT NULL,
    decision    TEXT NOT NULL,       -- 'accept' or 'reject'
    source      TEXT,               -- 'config', 'user_temporary', etc.
    FOREIGN KEY (event_id) REFERENCES events(id)
);

-- Tool result details
CREATE TABLE IF NOT EXISTS tool_results (
    event_id            INTEGER PRIMARY KEY,
    tool_name           TEXT NOT NULL,
    success             INTEGER NOT NULL,   -- 1=true, 0=false
    duration_ms         INTEGER,
    decision_type       TEXT,
    decision_source     TEXT,
    result_size_bytes   INTEGER,
    FOREIGN KEY (event_id) REFERENCES events(id)
);

-- User prompt details
CREATE TABLE IF NOT EXISTS user_prompts (
    event_id        INTEGER PRIMARY KEY,
    prompt_length   INTEGER,
    FOREIGN KEY (event_id) REFERENCES events(id)
);

-- API error details
CREATE TABLE IF NOT EXISTS api_errors (
    event_id    INTEGER PRIMARY KEY,
    model       TEXT,
    error       TEXT,
    status_code TEXT,
    duration_ms INTEGER,
    attempt     INTEGER,
    FOREIGN KEY (event_id) REFERENCES events(id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_events_type        ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_timestamp   ON events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_session     ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_user        ON events(user_email);
CREATE INDEX IF NOT EXISTS idx_events_practice    ON events(user_practice);
CREATE INDEX IF NOT EXISTS idx_api_requests_model ON api_requests(model);
CREATE INDEX IF NOT EXISTS idx_tool_results_name  ON tool_results(tool_name);
CREATE INDEX IF NOT EXISTS idx_sessions_user      ON sessions(user_email);

-- Handy view: api_requests joined with event + employee info
CREATE VIEW IF NOT EXISTS v_api_requests AS
SELECT
    e.event_timestamp,
    e.session_id,
    e.user_email,
    emp.full_name,
    emp.practice,
    emp.level,
    emp.location,
    e.terminal_type,
    e.os_type,
    e.host_arch,
    e.service_version,
    ar.model,
    ar.input_tokens,
    ar.output_tokens,
    ar.cache_read_tokens,
    ar.cache_creation_tokens,
    ar.cost_usd,
    ar.duration_ms
FROM api_requests ar
JOIN events e ON ar.event_id = e.id
LEFT JOIN employees emp ON e.user_email = emp.email;

-- Handy view: tool_results joined with event + employee info
CREATE VIEW IF NOT EXISTS v_tool_results AS
SELECT
    e.event_timestamp,
    e.session_id,
    e.user_email,
    emp.full_name,
    emp.practice,
    emp.level,
    emp.location,
    e.terminal_type,
    tr.tool_name,
    tr.success,
    tr.duration_ms,
    tr.decision_type,
    tr.decision_source,
    tr.result_size_bytes
FROM tool_results tr
JOIN events e ON tr.event_id = e.id
LEFT JOIN employees emp ON e.user_email = emp.email;

-- Handy view: session summary
CREATE VIEW IF NOT EXISTS v_session_summary AS
SELECT
    s.session_id,
    s.user_email,
    emp.full_name,
    emp.practice,
    emp.level,
    emp.location,
    s.terminal_type,
    s.started_at,
    s.ended_at,
    s.event_count,
    -- Compute duration in seconds from timestamps
    CAST(
        (julianday(s.ended_at) - julianday(s.started_at)) * 86400 AS INTEGER
    ) AS duration_seconds,
    -- Aggregated costs
    COALESCE(cost_agg.total_cost, 0) AS total_cost_usd,
    COALESCE(cost_agg.total_input_tokens, 0) AS total_input_tokens,
    COALESCE(cost_agg.total_output_tokens, 0) AS total_output_tokens,
    COALESCE(cost_agg.api_call_count, 0) AS api_call_count
FROM sessions s
LEFT JOIN employees emp ON s.user_email = emp.email
LEFT JOIN (
    SELECT
        e.session_id,
        SUM(ar.cost_usd) AS total_cost,
        SUM(ar.input_tokens) AS total_input_tokens,
        SUM(ar.output_tokens) AS total_output_tokens,
        COUNT(*) AS api_call_count
    FROM api_requests ar
    JOIN events e ON ar.event_id = e.id
    GROUP BY e.session_id
) cost_agg ON s.session_id = cost_agg.session_id;
"""

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_int(val, default=0):
    """Safely parse an integer from a string."""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def parse_float(val, default=0.0):
    """Safely parse a float from a string."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def parse_bool(val, default=False):
    """Parse 'true'/'false' string to int 1/0."""
    if val is None:
        return int(default)
    return 1 if str(val).lower() == "true" else 0


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def load_employees(db: sqlite3.Connection, csv_path: Path):
    """Load employees CSV into the employees table."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append((
                row["email"].strip(),
                row["full_name"].strip(),
                row["practice"].strip(),
                row["level"].strip(),
                row["location"].strip(),
            ))
    db.executemany(
        "INSERT OR IGNORE INTO employees (email, full_name, practice, level, location) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    db.commit()
    print(f"  Loaded {len(rows)} employees")
    return {r[0] for r in rows}  # set of emails


def ingest_telemetry(db: sqlite3.Connection, jsonl_path: Path):
    """Parse JSONL file and insert events into all tables."""
    # Accumulators for batch inserts
    events_buf = []
    api_req_buf = []
    tool_dec_buf = []
    tool_res_buf = []
    prompt_buf = []
    api_err_buf = []

    # Session tracking: session_id -> {min_ts, max_ts, user_email, terminal, count}
    sessions = {}

    event_id_counter = 0
    batch_count = 0

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            batch = json.loads(line)
            batch_count += 1

            for log_event in batch.get("logEvents", []):
                msg = json.loads(log_event["message"])
                attrs = msg.get("attributes", {})
                resource = msg.get("resource", {})
                scope = msg.get("scope", {})

                event_type = attrs.get("event.name", "")
                timestamp = attrs.get("event.timestamp", "")
                session_id = attrs.get("session.id", "")
                user_email = attrs.get("user.email", "")
                user_id = attrs.get("user.id", "")
                org_id = attrs.get("organization.id", "")
                terminal = attrs.get("terminal.type", "")

                host_arch = resource.get("host.arch", "")
                host_name = resource.get("host.name", "")
                os_type = resource.get("os.type", "")
                os_version = resource.get("os.version", "")
                service_version = resource.get("service.version", "")
                user_practice = resource.get("user.practice", "")

                event_id_counter += 1
                eid = event_id_counter

                # Base event row
                events_buf.append((
                    eid, event_type, timestamp, session_id, user_email,
                    user_id, org_id, terminal,
                    host_arch, host_name, os_type, os_version,
                    service_version, user_practice,
                ))

                # Event-type-specific detail tables
                if event_type == "api_request":
                    api_req_buf.append((
                        eid,
                        attrs.get("model", ""),
                        parse_int(attrs.get("input_tokens")),
                        parse_int(attrs.get("output_tokens")),
                        parse_int(attrs.get("cache_read_tokens")),
                        parse_int(attrs.get("cache_creation_tokens")),
                        parse_float(attrs.get("cost_usd")),
                        parse_int(attrs.get("duration_ms")),
                    ))
                elif event_type == "tool_decision":
                    tool_dec_buf.append((
                        eid,
                        attrs.get("tool_name", ""),
                        attrs.get("decision", ""),
                        attrs.get("source", ""),
                    ))
                elif event_type == "tool_result":
                    tool_res_buf.append((
                        eid,
                        attrs.get("tool_name", ""),
                        parse_bool(attrs.get("success")),
                        parse_int(attrs.get("duration_ms")),
                        attrs.get("decision_type", ""),
                        attrs.get("decision_source", ""),
                        parse_int(attrs.get("tool_result_size_bytes")) if attrs.get("tool_result_size_bytes") else None,
                    ))
                elif event_type == "user_prompt":
                    prompt_buf.append((
                        eid,
                        parse_int(attrs.get("prompt_length")),
                    ))
                elif event_type == "api_error":
                    api_err_buf.append((
                        eid,
                        attrs.get("model", ""),
                        attrs.get("error", ""),
                        attrs.get("status_code", ""),
                        parse_int(attrs.get("duration_ms")),
                        parse_int(attrs.get("attempt")),
                    ))

                # Track sessions
                if session_id:
                    if session_id not in sessions:
                        sessions[session_id] = {
                            "user_email": user_email,
                            "terminal": terminal,
                            "min_ts": timestamp,
                            "max_ts": timestamp,
                            "count": 0,
                        }
                    s = sessions[session_id]
                    if timestamp < s["min_ts"]:
                        s["min_ts"] = timestamp
                    if timestamp > s["max_ts"]:
                        s["max_ts"] = timestamp
                    s["count"] += 1

            # Flush in batches every 1000 JSONL lines
            if batch_count % 1000 == 0:
                _flush_buffers(db, events_buf, api_req_buf, tool_dec_buf,
                               tool_res_buf, prompt_buf, api_err_buf)
                events_buf.clear()
                api_req_buf.clear()
                tool_dec_buf.clear()
                tool_res_buf.clear()
                prompt_buf.clear()
                api_err_buf.clear()
                print(f"    ... processed {batch_count} batches ({event_id_counter} events)")

    # Final flush
    _flush_buffers(db, events_buf, api_req_buf, tool_dec_buf,
                   tool_res_buf, prompt_buf, api_err_buf)

    # Insert sessions
    session_rows = [
        (sid, s["user_email"], s["terminal"], s["min_ts"], s["max_ts"], s["count"])
        for sid, s in sessions.items()
    ]
    db.executemany(
        "INSERT OR IGNORE INTO sessions (session_id, user_email, terminal_type, started_at, ended_at, event_count) VALUES (?, ?, ?, ?, ?, ?)",
        session_rows,
    )
    db.commit()

    print(f"  Ingested {event_id_counter} events from {batch_count} batches")
    print(f"  Sessions: {len(sessions)}")


def _flush_buffers(db, events_buf, api_req_buf, tool_dec_buf,
                   tool_res_buf, prompt_buf, api_err_buf):
    """Bulk-insert buffered rows into the database."""
    if events_buf:
        db.executemany(
            "INSERT INTO events (id, event_type, event_timestamp, session_id, user_email, "
            "user_id, organization_id, terminal_type, host_arch, host_name, os_type, "
            "os_version, service_version, user_practice) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            events_buf,
        )
    if api_req_buf:
        db.executemany(
            "INSERT INTO api_requests (event_id, model, input_tokens, output_tokens, "
            "cache_read_tokens, cache_creation_tokens, cost_usd, duration_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            api_req_buf,
        )
    if tool_dec_buf:
        db.executemany(
            "INSERT INTO tool_decisions (event_id, tool_name, decision, source) VALUES (?, ?, ?, ?)",
            tool_dec_buf,
        )
    if tool_res_buf:
        db.executemany(
            "INSERT INTO tool_results (event_id, tool_name, success, duration_ms, "
            "decision_type, decision_source, result_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?)",
            tool_res_buf,
        )
    if prompt_buf:
        db.executemany(
            "INSERT INTO user_prompts (event_id, prompt_length) VALUES (?, ?)",
            prompt_buf,
        )
    if api_err_buf:
        db.executemany(
            "INSERT INTO api_errors (event_id, model, error, status_code, duration_ms, attempt) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            api_err_buf,
        )
    db.commit()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(db: sqlite3.Connection):
    """Print summary stats to verify data integrity."""
    print("\n=== Validation ===")

    tables = [
        "employees", "sessions", "events",
        "api_requests", "tool_decisions", "tool_results",
        "user_prompts", "api_errors",
    ]
    for table in tables:
        count = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608 – table names are hardcoded
        print(f"  {table:20s}: {count:>8,} rows")

    # Event type breakdown
    print("\n  Event type breakdown:")
    for row in db.execute(
        "SELECT event_type, COUNT(*) AS cnt FROM events GROUP BY event_type ORDER BY cnt DESC"
    ):
        print(f"    {row[0]:25s}: {row[1]:>8,}")

    # Cost summary
    row = db.execute(
        "SELECT SUM(cost_usd), AVG(cost_usd), MIN(cost_usd), MAX(cost_usd) FROM api_requests"
    ).fetchone()
    print(f"\n  API cost: total=${row[0]:.2f}  avg=${row[1]:.4f}  min=${row[2]:.4f}  max=${row[3]:.4f}")

    # Check join coverage (how many events match an employee)
    matched = db.execute(
        "SELECT COUNT(*) FROM events e JOIN employees emp ON e.user_email = emp.email"
    ).fetchone()[0]
    total = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    print(f"\n  Employee join coverage: {matched}/{total} events ({100*matched/total:.1f}%)")

    # Sessions per user
    print("\n  Top 5 users by session count:")
    for row in db.execute(
        "SELECT s.user_email, emp.practice, emp.level, COUNT(*) AS sess_count "
        "FROM sessions s LEFT JOIN employees emp ON s.user_email = emp.email "
        "GROUP BY s.user_email ORDER BY sess_count DESC LIMIT 5"
    ):
        print(f"    {row[0]:35s}  {row[1]:25s}  {row[2]:4s}  sessions={row[3]}")

    print("\n  Database is ready!")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest Claude Code telemetry into SQLite")
    parser.add_argument("--input-dir", default="output", help="Directory with telemetry_logs.jsonl and employees.csv")
    parser.add_argument("--db", default="telemetry.db", help="Output SQLite database path")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    jsonl_path = input_dir / "telemetry_logs.jsonl"
    csv_path = input_dir / "employees.csv"

    if not jsonl_path.exists():
        print(f"ERROR: {jsonl_path} not found. Run generate_fake_data.py first.", file=sys.stderr)
        sys.exit(1)
    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found. Run generate_fake_data.py first.", file=sys.stderr)
        sys.exit(1)

    db_path = Path(args.db)
    # Remove existing DB for a clean run
    if db_path.exists():
        db_path.unlink()
        print(f"Removed existing {db_path}")

    print(f"Creating database: {db_path}")
    db = sqlite3.connect(str(db_path))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")

    print("Creating schema...")
    db.executescript(SCHEMA_SQL)

    print("Loading employees...")
    load_employees(db, csv_path)

    print("Ingesting telemetry events...")
    ingest_telemetry(db, jsonl_path)

    validate(db)

    db.close()
    print(f"\nDone. Database saved to: {db_path}")


if __name__ == "__main__":
    main()
