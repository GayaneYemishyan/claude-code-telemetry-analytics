#!/usr/bin/env python3
"""
FastAPI REST API for Claude Code telemetry data.

Provides programmatic access to processed telemetry analytics.

Usage:
    uvicorn api:app --reload --port 8000
"""

import sqlite3
from contextlib import contextmanager
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

DB_PATH = "telemetry.db"

app = FastAPI(
    title="Claude Code Telemetry API",
    description="REST API for querying processed Claude Code telemetry data and analytics.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@contextmanager
def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()


def query(sql: str, params=()) -> list[dict]:
    with get_db() as db:
        rows = db.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def build_where(practice=None, level=None, location=None, model=None,
                date_start=None, date_end=None, emp_alias="emp", event_alias="e",
                ar_alias=None):
    """Build parameterized WHERE clause from optional filters."""
    clauses = []
    params = []
    if practice:
        clauses.append(f"{emp_alias}.practice = ?")
        params.append(practice)
    if level:
        clauses.append(f"{emp_alias}.level = ?")
        params.append(level)
    if location:
        clauses.append(f"{emp_alias}.location = ?")
        params.append(location)
    if model and ar_alias:
        clauses.append(f"{ar_alias}.model = ?")
        params.append(model)
    if date_start:
        clauses.append(f"DATE({event_alias}.event_timestamp) >= ?")
        params.append(date_start)
    if date_end:
        clauses.append(f"DATE({event_alias}.event_timestamp) <= ?")
        params.append(date_end)
    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


# ---------- Health ----------

@app.get("/health")
def health():
    return {"status": "ok", "db": DB_PATH}


# ---------- Employees ----------

@app.get("/employees")
def list_employees(
    practice: Optional[str] = None,
    level: Optional[str] = None,
    location: Optional[str] = None,
):
    clauses, params = [], []
    if practice:
        clauses.append("practice = ?"); params.append(practice)
    if level:
        clauses.append("level = ?"); params.append(level)
    if location:
        clauses.append("location = ?"); params.append(location)
    where = " AND ".join(clauses) if clauses else "1=1"
    return query(f"SELECT * FROM employees WHERE {where} ORDER BY email", params)


# ---------- Token Consumption ----------

@app.get("/tokens/by-model")
def tokens_by_model(
    practice: Optional[str] = None, level: Optional[str] = None,
    location: Optional[str] = None, date_start: Optional[str] = None,
    date_end: Optional[str] = None,
):
    where, params = build_where(practice, level, location, date_start=date_start, date_end=date_end)
    return query(f"""
        SELECT ar.model, COUNT(*) AS requests,
            SUM(ar.input_tokens) AS total_input, SUM(ar.output_tokens) AS total_output,
            SUM(ar.input_tokens + ar.output_tokens) AS total_tokens,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY ar.model ORDER BY total_cost DESC
    """, params)


@app.get("/tokens/by-practice")
def tokens_by_practice(
    level: Optional[str] = None, location: Optional[str] = None,
    date_start: Optional[str] = None, date_end: Optional[str] = None,
):
    where, params = build_where(level=level, location=location, date_start=date_start, date_end=date_end)
    return query(f"""
        SELECT emp.practice, COUNT(*) AS requests,
            SUM(ar.input_tokens + ar.output_tokens) AS total_tokens,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY emp.practice ORDER BY total_cost DESC
    """, params)


@app.get("/tokens/by-level")
def tokens_by_level(
    practice: Optional[str] = None, location: Optional[str] = None,
    date_start: Optional[str] = None, date_end: Optional[str] = None,
):
    where, params = build_where(practice=practice, location=location, date_start=date_start, date_end=date_end)
    return query(f"""
        SELECT emp.level, COUNT(DISTINCT e.user_email) AS users,
            COUNT(*) AS requests,
            SUM(ar.input_tokens + ar.output_tokens) AS total_tokens,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost,
            ROUND(SUM(ar.cost_usd) / COUNT(DISTINCT e.user_email), 2) AS cost_per_user
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY emp.level ORDER BY emp.level
    """, params)


# ---------- Cost Analysis ----------

@app.get("/cost/by-user")
def cost_by_user(
    practice: Optional[str] = None, level: Optional[str] = None,
    location: Optional[str] = None, date_start: Optional[str] = None,
    date_end: Optional[str] = None, limit: int = Query(default=20, le=200),
):
    where, params = build_where(practice, level, location, date_start=date_start, date_end=date_end)
    return query(f"""
        SELECT e.user_email, emp.full_name, emp.practice, emp.level, emp.location,
            COUNT(*) AS api_calls, ROUND(SUM(ar.cost_usd), 2) AS total_cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY e.user_email ORDER BY total_cost DESC LIMIT ?
    """, params + [limit])


@app.get("/cost/daily")
def cost_daily(
    practice: Optional[str] = None, level: Optional[str] = None,
    date_start: Optional[str] = None, date_end: Optional[str] = None,
):
    where, params = build_where(practice, level, date_start=date_start, date_end=date_end)
    return query(f"""
        SELECT DATE(e.event_timestamp) AS date,
            COUNT(*) AS api_calls, ROUND(SUM(ar.cost_usd), 2) AS daily_cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY date ORDER BY date
    """, params)


@app.get("/cost/sessions")
def cost_sessions(
    practice: Optional[str] = None, level: Optional[str] = None,
    limit: int = Query(default=20, le=200),
):
    clauses, params = [], []
    if practice:
        clauses.append("practice = ?"); params.append(practice)
    if level:
        clauses.append("level = ?"); params.append(level)
    where = " AND ".join(clauses) if clauses else "1=1"
    return query(f"""
        SELECT session_id, user_email, full_name, practice, level,
            api_call_count, total_cost_usd, total_input_tokens, total_output_tokens,
            ROUND(duration_seconds / 60.0, 1) AS duration_min
        FROM v_session_summary WHERE {where}
        ORDER BY total_cost_usd DESC LIMIT ?
    """, params + [limit])


# ---------- Usage Times ----------

@app.get("/usage/hourly")
def usage_hourly(practice: Optional[str] = None, level: Optional[str] = None):
    where, params = build_where(practice, level)
    return query(f"""
        SELECT CAST(strftime('%H', e.event_timestamp) AS INTEGER) AS hour,
            COUNT(*) AS events, COUNT(DISTINCT e.session_id) AS sessions,
            COUNT(DISTINCT e.user_email) AS users
        FROM events e JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY hour ORDER BY hour
    """, params)


@app.get("/usage/daily")
def usage_daily(practice: Optional[str] = None, level: Optional[str] = None):
    where, params = build_where(practice, level)
    return query(f"""
        SELECT CAST(strftime('%w', e.event_timestamp) AS INTEGER) AS dow,
            CASE CAST(strftime('%w', e.event_timestamp) AS INTEGER)
                WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday' END AS day_name,
            COUNT(*) AS events
        FROM events e JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY dow ORDER BY dow
    """, params)


@app.get("/usage/business-hours")
def usage_business_hours(practice: Optional[str] = None):
    where, params = build_where(practice)
    return query(f"""
        SELECT
            CASE
                WHEN CAST(strftime('%w', e.event_timestamp) AS INTEGER) IN (0,6) THEN 'Weekend'
                WHEN CAST(strftime('%H', e.event_timestamp) AS INTEGER) BETWEEN 9 AND 17 THEN 'Business Hours'
                ELSE 'Off-Hours'
            END AS category,
            COUNT(*) AS events
        FROM events e JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY category
    """, params)


# ---------- Tools ----------

@app.get("/tools/summary")
def tools_summary(practice: Optional[str] = None, level: Optional[str] = None):
    where, params = build_where(practice, level)
    return query(f"""
        SELECT tr.tool_name, COUNT(*) AS total_uses,
            SUM(tr.success) AS successes,
            ROUND(100.0 * SUM(tr.success) / COUNT(*), 1) AS success_rate,
            ROUND(AVG(tr.duration_ms), 0) AS avg_duration_ms
        FROM tool_results tr
        JOIN events e ON tr.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY tr.tool_name ORDER BY total_uses DESC
    """, params)


@app.get("/tools/by-practice")
def tools_by_practice():
    return query("""
        SELECT emp.practice, tr.tool_name, COUNT(*) AS uses,
            ROUND(100.0 * SUM(tr.success) / COUNT(*), 1) AS success_rate
        FROM tool_results tr
        JOIN events e ON tr.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.practice, tr.tool_name
        ORDER BY emp.practice, uses DESC
    """)


# ---------- Errors ----------

@app.get("/errors/by-type")
def errors_by_type():
    return query("""
        SELECT error, status_code, COUNT(*) AS count,
            ROUND(AVG(attempt), 1) AS avg_attempt
        FROM api_errors GROUP BY error, status_code ORDER BY count DESC
    """)


@app.get("/errors/by-model")
def errors_by_model():
    return query("""
        SELECT ae.model, COUNT(*) AS errors,
            req.total_requests,
            ROUND(100.0 * COUNT(*) / req.total_requests, 2) AS error_rate
        FROM api_errors ae
        JOIN (SELECT model, COUNT(*) AS total_requests FROM api_requests GROUP BY model) req
            ON ae.model = req.model
        GROUP BY ae.model ORDER BY error_rate DESC
    """)


# ---------- Sessions ----------

@app.get("/sessions/overview")
def sessions_overview(practice: Optional[str] = None, level: Optional[str] = None):
    clauses, params = [], []
    if practice:
        clauses.append("practice = ?"); params.append(practice)
    if level:
        clauses.append("level = ?"); params.append(level)
    where = " AND ".join(clauses) if clauses else "1=1"
    return query(f"""
        SELECT COUNT(*) AS sessions,
            ROUND(AVG(event_count), 1) AS avg_events,
            ROUND(AVG(duration_seconds / 60.0), 1) AS avg_duration_min,
            ROUND(AVG(api_call_count), 1) AS avg_api_calls,
            ROUND(AVG(total_cost_usd), 2) AS avg_cost
        FROM v_session_summary WHERE {where}
    """, params)


@app.get("/sessions/{session_id}")
def session_detail(session_id: str):
    rows = query(
        "SELECT * FROM v_session_summary WHERE session_id = ?", (session_id,)
    )
    if not rows:
        return JSONResponse(status_code=404, content={"error": "Session not found"})
    return rows[0]


# ---------- Predictions ----------

@app.get("/predict/cost-forecast")
def cost_forecast(days: int = Query(default=14, le=60)):
    """Linear trend forecast for daily cost."""
    import numpy as np
    from sklearn.linear_model import LinearRegression

    data = query("""
        SELECT DATE(e.event_timestamp) AS date, SUM(ar.cost_usd) AS daily_cost
        FROM api_requests ar JOIN events e ON ar.event_id = e.id
        GROUP BY date ORDER BY date
    """)
    if len(data) < 5:
        return {"error": "Insufficient data for forecasting"}

    day_nums = list(range(len(data)))
    costs = [r["daily_cost"] for r in data]

    X = np.array(day_nums).reshape(-1, 1)
    y = np.array(costs)
    model = LinearRegression().fit(X, y)

    future_X = np.arange(len(data), len(data) + days).reshape(-1, 1)
    forecast = model.predict(future_X).tolist()

    return {
        "r_squared": round(model.score(X, y), 4),
        "slope_per_day": round(float(model.coef_[0]), 4),
        "intercept": round(float(model.intercept_), 4),
        "forecast_days": days,
        "forecast_total_cost": round(sum(forecast), 2),
        "daily_forecast": [round(v, 2) for v in forecast],
    }


@app.get("/predict/anomalies")
def detect_anomalies(contamination: float = Query(default=0.05, ge=0.01, le=0.2)):
    """Detect anomalous sessions using Isolation Forest."""
    from sklearn.ensemble import IsolationForest

    data = query("""
        SELECT session_id, user_email, practice, level,
            total_cost_usd, api_call_count, total_input_tokens,
            total_output_tokens, event_count, duration_seconds
        FROM v_session_summary WHERE total_cost_usd > 0
    """)
    if len(data) < 20:
        return {"error": "Insufficient data"}

    import numpy as np
    feature_keys = ["total_cost_usd", "api_call_count", "total_input_tokens",
                    "total_output_tokens", "event_count", "duration_seconds"]
    X = np.array([[r.get(k, 0) or 0 for k in feature_keys] for r in data])

    iso = IsolationForest(contamination=contamination, random_state=42)
    labels = iso.fit_predict(X)

    anomalies = [
        {**data[i], "anomaly_score": round(float(iso.score_samples(X[i:i+1])[0]), 4)}
        for i in range(len(data)) if labels[i] == -1
    ]
    anomalies.sort(key=lambda x: x["anomaly_score"])

    return {
        "total_sessions": len(data),
        "anomalies_found": len(anomalies),
        "contamination": contamination,
        "anomalous_sessions": anomalies,
    }


# ---------- Token Forecast ----------

@app.get("/predict/token-forecast")
def token_forecast(days: int = Query(default=14, le=60)):
    """Linear trend forecast for daily token usage."""
    import numpy as np
    from sklearn.linear_model import LinearRegression

    data = query("""
        SELECT DATE(e.event_timestamp) AS date,
            SUM(ar.input_tokens + ar.output_tokens) AS daily_tokens
        FROM api_requests ar JOIN events e ON ar.event_id = e.id
        GROUP BY date ORDER BY date
    """)
    if len(data) < 5:
        return {"error": "Insufficient data for forecasting"}

    day_nums = list(range(len(data)))
    tokens = [r["daily_tokens"] for r in data]

    X = np.array(day_nums).reshape(-1, 1)
    y = np.array(tokens)
    model = LinearRegression().fit(X, y)

    future_X = np.arange(len(data), len(data) + days).reshape(-1, 1)
    forecast = model.predict(future_X).tolist()

    return {
        "r_squared": round(model.score(X, y), 4),
        "slope_per_day": round(float(model.coef_[0]), 1),
        "forecast_days": days,
        "forecast_total_tokens": round(sum(forecast), 0),
        "daily_forecast": [round(v, 0) for v in forecast],
    }


# ---------- Statistical Analysis ----------

@app.get("/stats/seniority-cost-correlation")
def seniority_cost_correlation():
    """Pearson correlation between seniority level and cost per user."""
    from scipy import stats

    data = query("""
        SELECT
            CAST(REPLACE(emp.level, 'L', '') AS INTEGER) AS level_num,
            ROUND(SUM(ar.cost_usd) / COUNT(DISTINCT e.user_email), 2) AS cost_per_user
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.level ORDER BY level_num
    """)
    if len(data) < 3:
        return {"error": "Insufficient data"}

    levels = [r["level_num"] for r in data]
    costs = [r["cost_per_user"] for r in data]
    r, p_val = stats.pearsonr(levels, costs)
    return {
        "pearson_r": round(r, 4),
        "p_value": round(p_val, 4),
        "significant": p_val < 0.05,
        "data": data,
    }


@app.get("/stats/practice-cost-comparison")
def practice_cost_comparison():
    """Kruskal-Wallis test for cost differences across practices."""
    from scipy import stats
    import numpy as np

    data = query("""
        SELECT emp.practice, ar.cost_usd
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
    """)
    if not data:
        return {"error": "No data"}

    groups = {}
    for r in data:
        groups.setdefault(r["practice"], []).append(r["cost_usd"])

    if len(groups) < 2:
        return {"error": "Need at least 2 practices"}

    stat, p_val = stats.kruskal(*groups.values())
    summaries = []
    for prac, costs in sorted(groups.items()):
        arr = np.array(costs)
        summaries.append({
            "practice": prac,
            "count": len(costs),
            "mean_cost": round(float(arr.mean()), 4),
            "median_cost": round(float(np.median(arr)), 4),
            "std_cost": round(float(arr.std()), 4),
        })

    return {
        "kruskal_wallis_H": round(float(stat), 4),
        "p_value": round(float(p_val), 4),
        "significant": p_val < 0.05,
        "practices": summaries,
    }


@app.get("/stats/practice-patterns")
def practice_patterns():
    """Practice-specific model and tool usage patterns."""
    model_prefs = query("""
        SELECT emp.practice, ar.model,
            COUNT(*) AS requests,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost,
            ROUND(AVG(ar.input_tokens + ar.output_tokens), 0) AS avg_tokens
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.practice, ar.model
        ORDER BY emp.practice, requests DESC
    """)
    tool_prefs = query("""
        SELECT emp.practice, tr.tool_name,
            COUNT(*) AS uses,
            ROUND(100.0 * SUM(tr.success) / COUNT(*), 1) AS success_rate
        FROM tool_results tr
        JOIN events e ON tr.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.practice, tr.tool_name
        ORDER BY emp.practice, uses DESC
    """)
    return {"model_preferences": model_prefs, "tool_preferences": tool_prefs}
