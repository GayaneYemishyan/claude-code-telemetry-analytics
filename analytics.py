#!/usr/bin/env python3
"""
Analytics module: Extract insights from the Claude Code telemetry database.

Provides reusable query functions for each insight area, plus a CLI that
prints a full analytics report.

Usage:
    python analytics.py [--db telemetry.db]
"""

import argparse
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

def get_db(db_path: str = "telemetry.db") -> sqlite3.Connection:
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    return db


def query(db: sqlite3.Connection, sql: str, params=()) -> list[dict]:
    """Execute SQL and return list of dicts."""
    rows = db.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


# ===================================================================
# 1. TOKEN CONSUMPTION
# ===================================================================

def tokens_by_model(db):
    """Token usage aggregated by model."""
    return query(db, """
        SELECT
            model,
            COUNT(*) AS request_count,
            SUM(input_tokens) AS total_input,
            SUM(output_tokens) AS total_output,
            SUM(cache_read_tokens) AS total_cache_read,
            SUM(cache_creation_tokens) AS total_cache_create,
            SUM(input_tokens + output_tokens) AS total_tokens,
            ROUND(AVG(input_tokens), 1) AS avg_input,
            ROUND(AVG(output_tokens), 1) AS avg_output,
            ROUND(SUM(cost_usd), 2) AS total_cost
        FROM api_requests
        GROUP BY model
        ORDER BY total_tokens DESC
    """)


def tokens_by_practice(db):
    """Token usage by engineering practice."""
    return query(db, """
        SELECT
            emp.practice,
            COUNT(*) AS request_count,
            SUM(ar.input_tokens + ar.output_tokens) AS total_tokens,
            ROUND(AVG(ar.input_tokens + ar.output_tokens), 1) AS avg_tokens_per_req,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.practice
        ORDER BY total_cost DESC
    """)


def tokens_by_level(db):
    """Token usage by seniority level."""
    return query(db, """
        SELECT
            emp.level,
            COUNT(DISTINCT e.user_email) AS user_count,
            COUNT(*) AS request_count,
            SUM(ar.input_tokens + ar.output_tokens) AS total_tokens,
            ROUND(AVG(ar.input_tokens + ar.output_tokens), 1) AS avg_tokens_per_req,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost,
            ROUND(SUM(ar.cost_usd) / COUNT(DISTINCT e.user_email), 2) AS cost_per_user
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.level
        ORDER BY emp.level
    """)


def tokens_by_practice_and_model(db):
    """Token usage cross-tabulated by practice and model."""
    return query(db, """
        SELECT
            emp.practice,
            ar.model,
            COUNT(*) AS request_count,
            SUM(ar.input_tokens + ar.output_tokens) AS total_tokens,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.practice, ar.model
        ORDER BY emp.practice, total_cost DESC
    """)


# ===================================================================
# 2. COST ANALYSIS
# ===================================================================

def cost_by_user(db):
    """Total cost per user, ranked."""
    return query(db, """
        SELECT
            e.user_email,
            emp.full_name,
            emp.practice,
            emp.level,
            emp.location,
            COUNT(*) AS api_calls,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost,
            ROUND(AVG(ar.cost_usd), 4) AS avg_cost_per_call
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY e.user_email
        ORDER BY total_cost DESC
    """)


def cost_per_session(db):
    """Cost distribution per session."""
    return query(db, """
        SELECT
            ROUND(AVG(total_cost_usd), 2) AS avg_session_cost,
            ROUND(MIN(total_cost_usd), 4) AS min_session_cost,
            ROUND(MAX(total_cost_usd), 2) AS max_session_cost,
            ROUND(SUM(total_cost_usd), 2) AS grand_total,
            COUNT(*) AS session_count
        FROM v_session_summary
    """)


def cost_per_session_by_practice(db):
    """Average session cost by practice."""
    return query(db, """
        SELECT
            practice,
            COUNT(*) AS session_count,
            ROUND(AVG(total_cost_usd), 2) AS avg_session_cost,
            ROUND(SUM(total_cost_usd), 2) AS total_cost
        FROM v_session_summary
        GROUP BY practice
        ORDER BY avg_session_cost DESC
    """)


def cost_trend_daily(db):
    """Daily cost trend."""
    return query(db, """
        SELECT
            DATE(e.event_timestamp) AS date,
            COUNT(*) AS api_calls,
            ROUND(SUM(ar.cost_usd), 2) AS daily_cost,
            SUM(ar.input_tokens + ar.output_tokens) AS daily_tokens
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        GROUP BY DATE(e.event_timestamp)
        ORDER BY date
    """)


# ===================================================================
# 3. PEAK USAGE TIMES
# ===================================================================

def usage_by_hour(db):
    """Event counts by hour of day."""
    return query(db, """
        SELECT
            CAST(strftime('%H', event_timestamp) AS INTEGER) AS hour,
            COUNT(*) AS event_count,
            COUNT(DISTINCT session_id) AS active_sessions,
            COUNT(DISTINCT user_email) AS active_users
        FROM events
        GROUP BY hour
        ORDER BY hour
    """)


def usage_by_day_of_week(db):
    """Event counts by day of week (0=Sunday)."""
    return query(db, """
        SELECT
            CAST(strftime('%w', event_timestamp) AS INTEGER) AS dow,
            CASE CAST(strftime('%w', event_timestamp) AS INTEGER)
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END AS day_name,
            COUNT(*) AS event_count,
            COUNT(DISTINCT session_id) AS active_sessions
        FROM events
        GROUP BY dow
        ORDER BY dow
    """)


def business_vs_offhours(db):
    """Compare business hours (9-18 Mon-Fri) vs off-hours."""
    return query(db, """
        SELECT
            CASE
                WHEN CAST(strftime('%w', event_timestamp) AS INTEGER) IN (0, 6) THEN 'Weekend'
                WHEN CAST(strftime('%H', event_timestamp) AS INTEGER) BETWEEN 9 AND 17 THEN 'Business Hours'
                ELSE 'Off-Hours (Weekday)'
            END AS time_category,
            COUNT(*) AS event_count,
            COUNT(DISTINCT session_id) AS sessions,
            COUNT(DISTINCT user_email) AS users
        FROM events
        GROUP BY time_category
        ORDER BY event_count DESC
    """)


def hourly_cost_heatmap(db):
    """Cost by hour and day-of-week for heatmap visualization."""
    return query(db, """
        SELECT
            CAST(strftime('%w', e.event_timestamp) AS INTEGER) AS dow,
            CAST(strftime('%H', e.event_timestamp) AS INTEGER) AS hour,
            COUNT(*) AS api_calls,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        GROUP BY dow, hour
        ORDER BY dow, hour
    """)


# ===================================================================
# 4. TOOL USAGE PATTERNS
# ===================================================================

def tool_usage_summary(db):
    """Overall tool usage: decision counts and results."""
    return query(db, """
        SELECT
            tr.tool_name,
            COUNT(*) AS total_uses,
            SUM(tr.success) AS successes,
            COUNT(*) - SUM(tr.success) AS failures,
            ROUND(100.0 * SUM(tr.success) / COUNT(*), 1) AS success_rate,
            ROUND(AVG(tr.duration_ms), 0) AS avg_duration_ms
        FROM tool_results tr
        GROUP BY tr.tool_name
        ORDER BY total_uses DESC
    """)


def tool_usage_by_practice(db):
    """Which practices use which tools most."""
    return query(db, """
        SELECT
            emp.practice,
            tr.tool_name,
            COUNT(*) AS uses,
            ROUND(100.0 * SUM(tr.success) / COUNT(*), 1) AS success_rate
        FROM tool_results tr
        JOIN events e ON tr.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.practice, tr.tool_name
        ORDER BY emp.practice, uses DESC
    """)


def tool_acceptance_rates(db):
    """Tool decision accept/reject rates by source."""
    return query(db, """
        SELECT
            tool_name,
            decision,
            source,
            COUNT(*) AS count
        FROM tool_decisions
        GROUP BY tool_name, decision, source
        ORDER BY tool_name, count DESC
    """)


def tool_usage_by_level(db):
    """Tool preferences by seniority level."""
    return query(db, """
        SELECT
            emp.level,
            tr.tool_name,
            COUNT(*) AS uses
        FROM tool_results tr
        JOIN events e ON tr.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.level, tr.tool_name
        ORDER BY emp.level, uses DESC
    """)


# ===================================================================
# 5. ERROR ANALYSIS
# ===================================================================

def errors_by_type(db):
    """Error breakdown by error message and status code."""
    return query(db, """
        SELECT
            error,
            status_code,
            COUNT(*) AS count,
            ROUND(AVG(attempt), 1) AS avg_attempt,
            ROUND(AVG(duration_ms), 0) AS avg_duration_ms
        FROM api_errors
        GROUP BY error, status_code
        ORDER BY count DESC
    """)


def errors_by_model(db):
    """Error rates by model."""
    return query(db, """
        SELECT
            ae.model,
            COUNT(*) AS error_count,
            req_counts.total_requests,
            ROUND(100.0 * COUNT(*) / req_counts.total_requests, 2) AS error_rate_pct
        FROM api_errors ae
        JOIN events e ON ae.event_id = e.id
        JOIN (
            SELECT ar.model, COUNT(*) AS total_requests
            FROM api_requests ar
            GROUP BY ar.model
        ) req_counts ON ae.model = req_counts.model
        GROUP BY ae.model
        ORDER BY error_rate_pct DESC
    """)


def errors_by_practice(db):
    """Error counts by engineering practice."""
    return query(db, """
        SELECT
            emp.practice,
            COUNT(*) AS error_count,
            ae.error,
            ae.status_code
        FROM api_errors ae
        JOIN events e ON ae.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.practice, ae.error
        ORDER BY emp.practice, error_count DESC
    """)


def error_trend_daily(db):
    """Daily error counts."""
    return query(db, """
        SELECT
            DATE(e.event_timestamp) AS date,
            COUNT(*) AS error_count
        FROM api_errors ae
        JOIN events e ON ae.event_id = e.id
        GROUP BY DATE(e.event_timestamp)
        ORDER BY date
    """)


def retry_distribution(db):
    """Distribution of retry attempts."""
    return query(db, """
        SELECT
            attempt,
            COUNT(*) AS count
        FROM api_errors
        GROUP BY attempt
        ORDER BY attempt
    """)


# ===================================================================
# 6. SESSION BEHAVIOR
# ===================================================================

def session_overview(db):
    """High-level session statistics."""
    return query(db, """
        SELECT
            COUNT(*) AS total_sessions,
            ROUND(AVG(event_count), 1) AS avg_events_per_session,
            ROUND(AVG(duration_seconds), 0) AS avg_duration_seconds,
            ROUND(AVG(api_call_count), 1) AS avg_api_calls_per_session,
            ROUND(AVG(total_cost_usd), 2) AS avg_cost_per_session,
            ROUND(AVG(total_input_tokens), 0) AS avg_input_tokens_per_session,
            ROUND(AVG(total_output_tokens), 0) AS avg_output_tokens_per_session
        FROM v_session_summary
    """)


def turns_per_session(db):
    """Distribution of user prompts (turns) per session."""
    return query(db, """
        SELECT
            e.session_id,
            COUNT(*) AS turn_count
        FROM events e
        WHERE e.event_type = 'user_prompt'
        GROUP BY e.session_id
    """)


def turns_distribution(db):
    """Histogram buckets for turns per session."""
    return query(db, """
        WITH turn_counts AS (
            SELECT session_id, COUNT(*) AS turns
            FROM events WHERE event_type = 'user_prompt'
            GROUP BY session_id
        )
        SELECT
            CASE
                WHEN turns = 1 THEN '1'
                WHEN turns BETWEEN 2 AND 3 THEN '2-3'
                WHEN turns BETWEEN 4 AND 7 THEN '4-7'
                WHEN turns BETWEEN 8 AND 15 THEN '8-15'
                WHEN turns BETWEEN 16 AND 30 THEN '16-30'
                ELSE '31+'
            END AS turn_bucket,
            COUNT(*) AS session_count,
            ROUND(AVG(turns), 1) AS avg_turns
        FROM turn_counts
        GROUP BY turn_bucket
        ORDER BY MIN(turns)
    """)


def prompt_length_stats(db):
    """Prompt length statistics."""
    return query(db, """
        SELECT
            COUNT(*) AS total_prompts,
            ROUND(AVG(prompt_length), 0) AS avg_length,
            MIN(prompt_length) AS min_length,
            MAX(prompt_length) AS max_length
        FROM user_prompts
    """)


def prompt_length_by_practice(db):
    """Average prompt length by practice."""
    return query(db, """
        SELECT
            emp.practice,
            COUNT(*) AS prompt_count,
            ROUND(AVG(up.prompt_length), 0) AS avg_length,
            MIN(up.prompt_length) AS min_length,
            MAX(up.prompt_length) AS max_length
        FROM user_prompts up
        JOIN events e ON up.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.practice
        ORDER BY avg_length DESC
    """)


def prompt_length_by_level(db):
    """Average prompt length by seniority level."""
    return query(db, """
        SELECT
            emp.level,
            COUNT(*) AS prompt_count,
            ROUND(AVG(up.prompt_length), 0) AS avg_length
        FROM user_prompts up
        JOIN events e ON up.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.level
        ORDER BY emp.level
    """)


def session_duration_by_practice(db):
    """Session duration stats by practice."""
    return query(db, """
        SELECT
            practice,
            COUNT(*) AS sessions,
            ROUND(AVG(duration_seconds) / 60.0, 1) AS avg_duration_min,
            ROUND(MIN(duration_seconds) / 60.0, 1) AS min_duration_min,
            ROUND(MAX(duration_seconds) / 60.0, 1) AS max_duration_min
        FROM v_session_summary
        GROUP BY practice
        ORDER BY avg_duration_min DESC
    """)


def top_sessions_by_cost(db, limit=10):
    """Most expensive sessions."""
    return query(db, """
        SELECT
            session_id,
            user_email,
            full_name,
            practice,
            level,
            api_call_count,
            total_cost_usd,
            total_input_tokens,
            total_output_tokens,
            ROUND(duration_seconds / 60.0, 1) AS duration_min
        FROM v_session_summary
        ORDER BY total_cost_usd DESC
        LIMIT ?
    """, (limit,))


# ===================================================================
# CLI Report
# ===================================================================

def print_table(rows, title=None, max_rows=20):
    """Pretty-print a list of dicts as a table."""
    if not rows:
        print("  (no data)")
        return
    if title:
        print(f"\n{'='*70}")
        print(f"  {title}")
        print(f"{'='*70}")

    keys = list(rows[0].keys())
    # Compute column widths
    widths = {k: max(len(str(k)), max(len(str(r.get(k, ""))) for r in rows[:max_rows])) for k in keys}

    # Header
    header = " | ".join(str(k).ljust(widths[k]) for k in keys)
    print(f"  {header}")
    print(f"  {'-+-'.join('-' * widths[k] for k in keys)}")

    # Rows
    for i, row in enumerate(rows[:max_rows]):
        line = " | ".join(str(row.get(k, "")).ljust(widths[k]) for k in keys)
        print(f"  {line}")
    if len(rows) > max_rows:
        print(f"  ... and {len(rows) - max_rows} more rows")


def report(db_path: str = "telemetry.db"):
    """Generate full analytics report."""
    db = get_db(db_path)

    # --- 1. TOKEN CONSUMPTION ---
    print_table(tokens_by_model(db), "TOKEN CONSUMPTION BY MODEL")
    print_table(tokens_by_practice(db), "TOKEN CONSUMPTION BY PRACTICE")
    print_table(tokens_by_level(db), "TOKEN CONSUMPTION BY SENIORITY LEVEL")
    print_table(tokens_by_practice_and_model(db), "TOKEN CONSUMPTION BY PRACTICE × MODEL", max_rows=30)

    # --- 2. COST ANALYSIS ---
    print_table(cost_per_session(db), "COST PER SESSION (OVERVIEW)")
    print_table(cost_per_session_by_practice(db), "AVG SESSION COST BY PRACTICE")
    print_table(cost_by_user(db), "COST BY USER (TOP 20)")
    print_table(cost_trend_daily(db), "DAILY COST TREND", max_rows=60)

    # --- 3. PEAK USAGE ---
    print_table(usage_by_hour(db), "USAGE BY HOUR OF DAY")
    print_table(usage_by_day_of_week(db), "USAGE BY DAY OF WEEK")
    print_table(business_vs_offhours(db), "BUSINESS HOURS vs OFF-HOURS")
    print_table(hourly_cost_heatmap(db), "COST HEATMAP (DOW × HOUR)", max_rows=50)

    # --- 4. TOOL USAGE ---
    print_table(tool_usage_summary(db), "TOOL USAGE SUMMARY")
    print_table(tool_usage_by_practice(db), "TOOL USAGE BY PRACTICE", max_rows=50)

    # --- 5. ERROR ANALYSIS ---
    print_table(errors_by_type(db), "ERRORS BY TYPE")
    print_table(errors_by_model(db), "ERROR RATES BY MODEL")
    print_table(retry_distribution(db), "RETRY ATTEMPT DISTRIBUTION")
    print_table(error_trend_daily(db), "DAILY ERROR TREND", max_rows=60)

    # --- 6. SESSION BEHAVIOR ---
    print_table(session_overview(db), "SESSION OVERVIEW")
    print_table(turns_distribution(db), "TURNS PER SESSION DISTRIBUTION")
    print_table(prompt_length_stats(db), "PROMPT LENGTH STATISTICS")
    print_table(prompt_length_by_practice(db), "PROMPT LENGTH BY PRACTICE")
    print_table(prompt_length_by_level(db), "PROMPT LENGTH BY SENIORITY")
    print_table(session_duration_by_practice(db), "SESSION DURATION BY PRACTICE")
    print_table(top_sessions_by_cost(db), "TOP 10 MOST EXPENSIVE SESSIONS")

    db.close()
    print(f"\n{'='*70}")
    print("  Report complete.")
    print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser(description="Analytics report for Claude Code telemetry")
    parser.add_argument("--db", default="telemetry.db", help="SQLite database path")
    args = parser.parse_args()
    report(args.db)


if __name__ == "__main__":
    main()
