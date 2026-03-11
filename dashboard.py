#!/usr/bin/env python3
"""
Streamlit dashboard for Claude Code Telemetry analytics.

Usage:
    streamlit run dashboard.py
"""

import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

DB_PATH = "telemetry.db"


def _db_has_tables():
    """Return True if the DB file exists and contains the expected tables."""
    if not Path(DB_PATH).exists():
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        conn.close()
        return "employees" in tables and "events" in tables
    except Exception:
        return False


def _ensure_database():
    """Generate data and build the DB if it doesn't exist yet (e.g. on Streamlit Cloud)."""
    if _db_has_tables():
        return

    # Remove incomplete / empty DB so ingest.py can start fresh
    if Path(DB_PATH).exists():
        Path(DB_PATH).unlink()

    st.info("First run detected — generating data and building database…")
    python = sys.executable

    # Only generate data if output files are missing (they are gitignored)
    if not Path("output/telemetry_logs.jsonl").exists():
        result = subprocess.run(
            [python, "generate_fake_data.py"], capture_output=True, text=True
        )
        if result.returncode != 0:
            st.error(f"Data generation failed:\n```\n{result.stderr}\n```")
            st.stop()

    result = subprocess.run(
        [python, "ingest.py"], capture_output=True, text=True
    )
    if result.returncode != 0:
        st.error(f"Database ingestion failed:\n```\n{result.stderr}\n```")
        st.stop()

    # Clear cached DB connection so the new database is picked up
    get_db.clear()
    st.rerun()


@st.cache_resource
def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


_ensure_database()

st.set_page_config(
    page_title="Claude Code Telemetry",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(ttl=60)
def load_df(sql, params=()):
    db = get_db()
    return pd.read_sql_query(sql, db, params=params)


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.title("🔍 Filters")

# Load filter options
employees = load_df("SELECT DISTINCT practice, level, location FROM employees")
practices = ["All"] + sorted(employees["practice"].unique().tolist())
levels = ["All"] + sorted(employees["level"].unique().tolist())
locations = ["All"] + sorted(employees["location"].unique().tolist())

models_df = load_df("SELECT DISTINCT model FROM api_requests ORDER BY model")
models = ["All"] + models_df["model"].tolist()

date_range_df = load_df(
    "SELECT MIN(event_timestamp) AS min_ts, MAX(event_timestamp) AS max_ts FROM events"
)
min_date = datetime.strptime(date_range_df["min_ts"][0][:10], "%Y-%m-%d").date()
max_date = datetime.strptime(date_range_df["max_ts"][0][:10], "%Y-%m-%d").date()

sel_practice = st.sidebar.selectbox("Practice", practices)
sel_level = st.sidebar.selectbox("Seniority Level", levels)
sel_location = st.sidebar.selectbox("Location", locations)
sel_model = st.sidebar.selectbox("Model", models)
sel_dates = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(sel_dates, tuple) and len(sel_dates) == 2:
    date_start, date_end = sel_dates
else:
    date_start, date_end = min_date, max_date


def build_filter_clause(table_prefix="e", include_model=False):
    """Build SQL WHERE conditions from sidebar selections."""
    clauses = []
    params = []
    if sel_practice != "All":
        clauses.append(f"emp.practice = ?")
        params.append(sel_practice)
    if sel_level != "All":
        clauses.append(f"emp.level = ?")
        params.append(sel_level)
    if sel_location != "All":
        clauses.append(f"emp.location = ?")
        params.append(sel_location)
    if include_model and sel_model != "All":
        clauses.append(f"ar.model = ?")
        params.append(sel_model)
    clauses.append(f"DATE({table_prefix}.event_timestamp) BETWEEN ? AND ?")
    params.extend([str(date_start), str(date_end)])
    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------

st.title("📊 Claude Code Telemetry Dashboard")
st.caption(f"Data range: {min_date} → {max_date} | Filters active: "
           f"Practice={sel_practice}, Level={sel_level}, Location={sel_location}, Model={sel_model}")

# ---------------------------------------------------------------------------
# Tab layout
# ---------------------------------------------------------------------------

tab_overview, tab_tokens, tab_cost, tab_time, tab_tools, tab_errors, tab_sessions, tab_predict = st.tabs([
    "📋 Overview", "🪙 Tokens", "💰 Cost", "⏰ Usage Times",
    "🔧 Tools", "⚠️ Errors", "📈 Sessions", "🔮 Predictions"
])

# ===================================================================
# TAB 1: Overview KPIs
# ===================================================================

with tab_overview:
    where, params = build_filter_clause()
    overview = load_df(f"""
        SELECT
            COUNT(DISTINCT e.session_id) AS sessions,
            COUNT(DISTINCT e.user_email) AS users,
            COUNT(*) AS total_events
        FROM events e
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
    """, params)

    where_ar, params_ar = build_filter_clause(include_model=True)
    cost_ov = load_df(f"""
        SELECT
            ROUND(SUM(ar.cost_usd), 2) AS total_cost,
            SUM(ar.input_tokens + ar.output_tokens) AS total_tokens,
            COUNT(*) AS api_calls
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where_ar}
    """, params_ar)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Active Users", f"{overview['users'][0]}")
    c2.metric("Sessions", f"{overview['sessions'][0]:,}")
    c3.metric("Total Events", f"{overview['total_events'][0]:,}")
    c4.metric("API Calls", f"{cost_ov['api_calls'][0]:,}")
    c5.metric("Total Cost", f"${cost_ov['total_cost'][0]:,.2f}")

    st.divider()

    # Cost by practice donut
    col1, col2 = st.columns(2)

    with col1:
        cost_prac = load_df(f"""
            SELECT emp.practice, ROUND(SUM(ar.cost_usd), 2) AS total_cost
            FROM api_requests ar
            JOIN events e ON ar.event_id = e.id
            JOIN employees emp ON e.user_email = emp.email
            WHERE {where_ar}
            GROUP BY emp.practice ORDER BY total_cost DESC
        """, params_ar)
        if not cost_prac.empty:
            fig = px.pie(cost_prac, values="total_cost", names="practice",
                         title="Cost Distribution by Practice", hole=0.4)
            st.plotly_chart(fig, width='stretch')

    with col2:
        cost_model = load_df(f"""
            SELECT ar.model, ROUND(SUM(ar.cost_usd), 2) AS total_cost
            FROM api_requests ar
            JOIN events e ON ar.event_id = e.id
            JOIN employees emp ON e.user_email = emp.email
            WHERE {where_ar}
            GROUP BY ar.model ORDER BY total_cost DESC
        """, params_ar)
        if not cost_model.empty:
            fig = px.pie(cost_model, values="total_cost", names="model",
                         title="Cost Distribution by Model", hole=0.4)
            st.plotly_chart(fig, width='stretch')

    # Daily events trend
    daily_trend = load_df(f"""
        SELECT DATE(e.event_timestamp) AS date, e.event_type, COUNT(*) AS cnt
        FROM events e
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY date, e.event_type ORDER BY date
    """, params)
    if not daily_trend.empty:
        fig = px.area(daily_trend, x="date", y="cnt", color="event_type",
                      title="Daily Event Volume by Type")
        fig.update_layout(xaxis_title="Date", yaxis_title="Events")
        st.plotly_chart(fig, width='stretch')

# ===================================================================
# TAB 2: Token Consumption
# ===================================================================

with tab_tokens:
    where_ar, params_ar = build_filter_clause(include_model=True)

    st.subheader("Token Usage by Model")
    tok_model = load_df(f"""
        SELECT ar.model,
            COUNT(*) AS requests,
            SUM(ar.input_tokens) AS total_input,
            SUM(ar.output_tokens) AS total_output,
            SUM(ar.cache_read_tokens) AS cache_read,
            SUM(ar.cache_creation_tokens) AS cache_create,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where_ar}
        GROUP BY ar.model ORDER BY total_cost DESC
    """, params_ar)

    if not tok_model.empty:
        fig = px.bar(tok_model, x="model", y=["total_input", "total_output"],
                     barmode="group", title="Input vs Output Tokens by Model")
        st.plotly_chart(fig, width='stretch')
        st.dataframe(tok_model, width='stretch', hide_index=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Tokens by Practice")
        tok_prac = load_df(f"""
            SELECT emp.practice,
                COUNT(*) AS requests,
                SUM(ar.input_tokens + ar.output_tokens) AS total_tokens,
                ROUND(AVG(ar.input_tokens + ar.output_tokens), 0) AS avg_per_req,
                ROUND(SUM(ar.cost_usd), 2) AS total_cost
            FROM api_requests ar
            JOIN events e ON ar.event_id = e.id
            JOIN employees emp ON e.user_email = emp.email
            WHERE {where_ar}
            GROUP BY emp.practice ORDER BY total_cost DESC
        """, params_ar)
        if not tok_prac.empty:
            fig = px.bar(tok_prac, x="practice", y="total_tokens", color="practice",
                         title="Total Tokens by Practice")
            st.plotly_chart(fig, width='stretch')

    with col2:
        st.subheader("Tokens by Seniority")
        tok_level = load_df(f"""
            SELECT emp.level,
                COUNT(DISTINCT e.user_email) AS users,
                SUM(ar.input_tokens + ar.output_tokens) AS total_tokens,
                ROUND(SUM(ar.cost_usd) / COUNT(DISTINCT e.user_email), 2) AS cost_per_user
            FROM api_requests ar
            JOIN events e ON ar.event_id = e.id
            JOIN employees emp ON e.user_email = emp.email
            WHERE {where_ar}
            GROUP BY emp.level ORDER BY emp.level
        """, params_ar)
        if not tok_level.empty:
            fig = px.bar(tok_level, x="level", y="cost_per_user",
                         title="Cost per User by Seniority Level", color="level")
            st.plotly_chart(fig, width='stretch')

    # Daily token trend
    st.subheader("Daily Token Trend")
    tok_daily = load_df(f"""
        SELECT DATE(e.event_timestamp) AS date,
            SUM(ar.input_tokens) AS input_tokens,
            SUM(ar.output_tokens) AS output_tokens,
            ROUND(SUM(ar.cost_usd), 2) AS cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where_ar}
        GROUP BY date ORDER BY date
    """, params_ar)
    if not tok_daily.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=tok_daily["date"], y=tok_daily["input_tokens"],
                                 name="Input Tokens", fill="tozeroy"))
        fig.add_trace(go.Scatter(x=tok_daily["date"], y=tok_daily["output_tokens"],
                                 name="Output Tokens", fill="tozeroy"))
        fig.update_layout(title="Daily Token Volume", xaxis_title="Date", yaxis_title="Tokens")
        st.plotly_chart(fig, width='stretch')

# ===================================================================
# TAB 3: Cost Analysis
# ===================================================================

with tab_cost:
    where_ar, params_ar = build_filter_clause(include_model=True)

    # Top spenders
    st.subheader("Top Users by Cost")
    top_users = load_df(f"""
        SELECT e.user_email, emp.full_name, emp.practice, emp.level, emp.location,
            COUNT(*) AS api_calls,
            ROUND(SUM(ar.cost_usd), 2) AS total_cost,
            ROUND(AVG(ar.cost_usd), 4) AS avg_cost_per_call
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where_ar}
        GROUP BY e.user_email ORDER BY total_cost DESC
    """, params_ar)

    if not top_users.empty:
        fig = px.bar(top_users, x="full_name", y="total_cost", color="practice",
                     title="Total Cost by User", hover_data=["level", "location", "api_calls"])
        fig.update_layout(xaxis_title="User", yaxis_title="Cost (USD)")
        st.plotly_chart(fig, width='stretch')
        st.dataframe(top_users, width='stretch', hide_index=True)

    # Session cost distribution
    st.subheader("Session Cost Distribution")
    sess_cost = load_df(f"""
        SELECT vs.session_id, vs.user_email, vs.practice, vs.level,
            vs.total_cost_usd, vs.api_call_count,
            ROUND(vs.duration_seconds / 60.0, 1) AS duration_min
        FROM v_session_summary vs
        JOIN employees emp ON vs.user_email = emp.email
        WHERE {" AND ".join([
            "emp.practice = ?" if sel_practice != "All" else "1=1",
            "emp.level = ?" if sel_level != "All" else "1=1",
            "emp.location = ?" if sel_location != "All" else "1=1",
            "DATE(vs.started_at) BETWEEN ? AND ?"
        ])}
    """, [p for p in [
        sel_practice if sel_practice != "All" else None,
        sel_level if sel_level != "All" else None,
        sel_location if sel_location != "All" else None,
    ] if p is not None] + [str(date_start), str(date_end)])

    if not sess_cost.empty:
        fig = px.histogram(sess_cost, x="total_cost_usd", nbins=40,
                           title="Session Cost Distribution",
                           color="practice", barmode="overlay", opacity=0.7)
        fig.update_layout(xaxis_title="Session Cost (USD)", yaxis_title="Count")
        st.plotly_chart(fig, width='stretch')

    # Daily cost trend
    st.subheader("Daily Cost Trend")
    daily_cost = load_df(f"""
        SELECT DATE(e.event_timestamp) AS date,
            ROUND(SUM(ar.cost_usd), 2) AS daily_cost,
            COUNT(*) AS api_calls
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where_ar}
        GROUP BY date ORDER BY date
    """, params_ar)
    if not daily_cost.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=daily_cost["date"], y=daily_cost["daily_cost"], name="Daily Cost"))
        fig.add_trace(go.Scatter(x=daily_cost["date"], y=daily_cost["daily_cost"].rolling(5).mean(),
                                 name="5-day Moving Avg", line=dict(color="red", width=2)))
        fig.update_layout(title="Daily Cost with Moving Average",
                          xaxis_title="Date", yaxis_title="Cost (USD)")
        st.plotly_chart(fig, width='stretch')

# ===================================================================
# TAB 4: Peak Usage Times
# ===================================================================

with tab_time:
    where, params = build_filter_clause()

    # Hourly pattern
    st.subheader("Activity by Hour of Day")
    hourly = load_df(f"""
        SELECT CAST(strftime('%H', e.event_timestamp) AS INTEGER) AS hour,
            COUNT(*) AS events,
            COUNT(DISTINCT e.session_id) AS sessions,
            COUNT(DISTINCT e.user_email) AS users
        FROM events e
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY hour ORDER BY hour
    """, params)
    if not hourly.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=hourly["hour"], y=hourly["events"], name="Events",
                             marker_color="steelblue"))
        fig.add_trace(go.Scatter(x=hourly["hour"], y=hourly["users"], name="Unique Users",
                                 yaxis="y2", line=dict(color="orange", width=2)))
        fig.update_layout(
            title="Hourly Activity Pattern",
            xaxis_title="Hour of Day",
            yaxis=dict(title="Event Count"),
            yaxis2=dict(title="Unique Users", overlaying="y", side="right"),
        )
        st.plotly_chart(fig, width='stretch')

    col1, col2 = st.columns(2)

    with col1:
        # Day of week
        st.subheader("Activity by Day of Week")
        dow = load_df(f"""
            SELECT
                CASE CAST(strftime('%w', e.event_timestamp) AS INTEGER)
                    WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue'
                    WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat'
                END AS day,
                CAST(strftime('%w', e.event_timestamp) AS INTEGER) AS dow_num,
                COUNT(*) AS events
            FROM events e
            JOIN employees emp ON e.user_email = emp.email
            WHERE {where}
            GROUP BY dow_num ORDER BY dow_num
        """, params)
        if not dow.empty:
            fig = px.bar(dow, x="day", y="events", title="Events by Day of Week",
                         color="events", color_continuous_scale="Blues")
            st.plotly_chart(fig, width='stretch')

    with col2:
        # Business hours vs off-hours
        st.subheader("Business vs Off-Hours")
        biz = load_df(f"""
            SELECT
                CASE
                    WHEN CAST(strftime('%w', e.event_timestamp) AS INTEGER) IN (0, 6) THEN 'Weekend'
                    WHEN CAST(strftime('%H', e.event_timestamp) AS INTEGER) BETWEEN 9 AND 17 THEN 'Business Hours'
                    ELSE 'Off-Hours'
                END AS category,
                COUNT(*) AS events
            FROM events e
            JOIN employees emp ON e.user_email = emp.email
            WHERE {where}
            GROUP BY category
        """, params)
        if not biz.empty:
            fig = px.pie(biz, values="events", names="category",
                         title="Business Hours vs Off-Hours", hole=0.4,
                         color_discrete_map={"Business Hours": "#2196F3",
                                             "Off-Hours": "#FF9800",
                                             "Weekend": "#9C27B0"})
            st.plotly_chart(fig, width='stretch')

    # Heatmap: hour × day of week
    st.subheader("Activity Heatmap (Hour × Day of Week)")
    where_ar, params_ar = build_filter_clause(include_model=True)
    heatmap = load_df(f"""
        SELECT
            CAST(strftime('%w', e.event_timestamp) AS INTEGER) AS dow,
            CAST(strftime('%H', e.event_timestamp) AS INTEGER) AS hour,
            ROUND(SUM(ar.cost_usd), 2) AS cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where_ar}
        GROUP BY dow, hour
    """, params_ar)
    if not heatmap.empty:
        import numpy as np
        days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        matrix = np.zeros((7, 24))
        for _, row in heatmap.iterrows():
            matrix[int(row["dow"]), int(row["hour"])] = row["cost"]
        fig = go.Figure(data=go.Heatmap(
            z=matrix, x=list(range(24)), y=days,
            colorscale="YlOrRd", colorbar_title="Cost ($)"
        ))
        fig.update_layout(title="Cost Heatmap by Day × Hour",
                          xaxis_title="Hour", yaxis_title="Day of Week")
        st.plotly_chart(fig, width='stretch')

# ===================================================================
# TAB 5: Tool Usage
# ===================================================================

with tab_tools:
    where, params = build_filter_clause()

    st.subheader("Tool Usage Summary")
    tools_df = load_df(f"""
        SELECT tr.tool_name,
            COUNT(*) AS total_uses,
            SUM(tr.success) AS successes,
            ROUND(100.0 * SUM(tr.success) / COUNT(*), 1) AS success_rate,
            ROUND(AVG(tr.duration_ms), 0) AS avg_duration_ms
        FROM tool_results tr
        JOIN events e ON tr.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY tr.tool_name ORDER BY total_uses DESC
    """, params)

    if not tools_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(tools_df, x="tool_name", y="total_uses",
                         title="Tool Usage Count", color="success_rate",
                         color_continuous_scale="RdYlGn")
            st.plotly_chart(fig, width='stretch')
        with col2:
            fig = px.bar(tools_df, x="tool_name", y="success_rate",
                         title="Tool Success Rate (%)", color="success_rate",
                         color_continuous_scale="RdYlGn", range_y=[85, 101])
            st.plotly_chart(fig, width='stretch')
        st.dataframe(tools_df, width='stretch', hide_index=True)

    # Tool usage by practice
    st.subheader("Tool Preferences by Practice")
    tools_prac = load_df(f"""
        SELECT emp.practice, tr.tool_name, COUNT(*) AS uses
        FROM tool_results tr
        JOIN events e ON tr.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY emp.practice, tr.tool_name
        ORDER BY emp.practice, uses DESC
    """, params)
    if not tools_prac.empty:
        # Show top 8 tools per practice
        top_tools = tools_df.nlargest(8, "total_uses")["tool_name"].tolist()
        filtered = tools_prac[tools_prac["tool_name"].isin(top_tools)]
        fig = px.bar(filtered, x="practice", y="uses", color="tool_name",
                     title="Tool Usage by Practice (Top 8 Tools)", barmode="group")
        st.plotly_chart(fig, width='stretch')

# ===================================================================
# TAB 6: Error Analysis
# ===================================================================

with tab_errors:
    where, params = build_filter_clause()

    st.subheader("Errors by Type")
    err_type = load_df(f"""
        SELECT ae.error, ae.status_code, COUNT(*) AS count,
            ROUND(AVG(ae.attempt), 1) AS avg_attempt
        FROM api_errors ae
        JOIN events e ON ae.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY ae.error, ae.status_code ORDER BY count DESC
    """, params)

    if not err_type.empty:
        fig = px.bar(err_type, x="error", y="count", color="status_code",
                     title="Error Counts by Type")
        fig.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig, width='stretch')
        st.dataframe(err_type, width='stretch', hide_index=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Error Rate by Model")
        err_model = load_df(f"""
            SELECT ae.model,
                COUNT(*) AS errors,
                req.total_requests,
                ROUND(100.0 * COUNT(*) / req.total_requests, 2) AS error_rate
            FROM api_errors ae
            JOIN events e ON ae.event_id = e.id
            JOIN employees emp ON e.user_email = emp.email
            JOIN (SELECT model, COUNT(*) AS total_requests FROM api_requests GROUP BY model) req
                ON ae.model = req.model
            WHERE {where}
            GROUP BY ae.model ORDER BY error_rate DESC
        """, params)
        if not err_model.empty:
            fig = px.bar(err_model, x="model", y="error_rate",
                         title="Error Rate by Model (%)", color="error_rate",
                         color_continuous_scale="OrRd")
            st.plotly_chart(fig, width='stretch')

    with col2:
        st.subheader("Retry Distribution")
        retry = load_df(f"""
            SELECT ae.attempt, COUNT(*) AS count
            FROM api_errors ae
            JOIN events e ON ae.event_id = e.id
            JOIN employees emp ON e.user_email = emp.email
            WHERE {where}
            GROUP BY ae.attempt ORDER BY ae.attempt
        """, params)
        if not retry.empty:
            fig = px.pie(retry, values="count", names="attempt",
                         title="Retry Attempts Distribution")
            st.plotly_chart(fig, width='stretch')

    # Error trend
    st.subheader("Daily Error Trend")
    err_daily = load_df(f"""
        SELECT DATE(e.event_timestamp) AS date, COUNT(*) AS errors
        FROM api_errors ae
        JOIN events e ON ae.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY date ORDER BY date
    """, params)
    if not err_daily.empty:
        fig = px.bar(err_daily, x="date", y="errors", title="Daily Error Count")
        st.plotly_chart(fig, width='stretch')

# ===================================================================
# TAB 7: Session Behavior
# ===================================================================

with tab_sessions:
    where, params = build_filter_clause()

    # Session overview KPIs
    sess_ov = load_df(f"""
        SELECT
            COUNT(*) AS sessions,
            ROUND(AVG(vs.event_count), 1) AS avg_events,
            ROUND(AVG(vs.duration_seconds / 60.0), 1) AS avg_duration_min,
            ROUND(AVG(vs.api_call_count), 1) AS avg_api_calls,
            ROUND(AVG(vs.total_cost_usd), 2) AS avg_cost
        FROM v_session_summary vs
        JOIN employees emp ON vs.user_email = emp.email
        WHERE {" AND ".join([
            "emp.practice = ?" if sel_practice != "All" else "1=1",
            "emp.level = ?" if sel_level != "All" else "1=1",
            "emp.location = ?" if sel_location != "All" else "1=1",
            "DATE(vs.started_at) BETWEEN ? AND ?"
        ])}
    """, [p for p in [
        sel_practice if sel_practice != "All" else None,
        sel_level if sel_level != "All" else None,
        sel_location if sel_location != "All" else None,
    ] if p is not None] + [str(date_start), str(date_end)])

    if not sess_ov.empty:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Sessions", f"{sess_ov['sessions'][0]:,}")
        c2.metric("Avg Events/Session", f"{sess_ov['avg_events'][0]}")
        c3.metric("Avg Duration", f"{sess_ov['avg_duration_min'][0]} min")
        c4.metric("Avg API Calls", f"{sess_ov['avg_api_calls'][0]}")
        c5.metric("Avg Cost/Session", f"${sess_ov['avg_cost'][0]}")

    # Turns per session distribution
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Turns per Session")
        turns = load_df(f"""
            SELECT e.session_id, COUNT(*) AS turns
            FROM events e
            JOIN employees emp ON e.user_email = emp.email
            WHERE e.event_type = 'user_prompt' AND {where}
            GROUP BY e.session_id
        """, params)
        if not turns.empty:
            fig = px.histogram(turns, x="turns", nbins=30,
                               title="Distribution of Turns per Session")
            fig.update_layout(xaxis_title="Turns", yaxis_title="Sessions")
            st.plotly_chart(fig, width='stretch')

    with col2:
        st.subheader("Prompt Length Distribution")
        prompts = load_df(f"""
            SELECT up.prompt_length
            FROM user_prompts up
            JOIN events e ON up.event_id = e.id
            JOIN employees emp ON e.user_email = emp.email
            WHERE {where}
        """, params)
        if not prompts.empty:
            fig = px.histogram(prompts, x="prompt_length", nbins=50,
                               title="Prompt Length Distribution", log_y=True)
            fig.update_layout(xaxis_title="Prompt Length (chars)", yaxis_title="Count (log)")
            st.plotly_chart(fig, width='stretch')

    # Session duration by practice
    st.subheader("Session Duration by Practice")
    sess_dur = load_df(f"""
        SELECT vs.practice,
            ROUND(vs.duration_seconds / 60.0, 1) AS duration_min,
            vs.total_cost_usd AS cost
        FROM v_session_summary vs
        JOIN employees emp ON vs.user_email = emp.email
        WHERE {" AND ".join([
            "emp.practice = ?" if sel_practice != "All" else "1=1",
            "emp.level = ?" if sel_level != "All" else "1=1",
            "emp.location = ?" if sel_location != "All" else "1=1",
            "DATE(vs.started_at) BETWEEN ? AND ?"
        ])}
    """, [p for p in [
        sel_practice if sel_practice != "All" else None,
        sel_level if sel_level != "All" else None,
        sel_location if sel_location != "All" else None,
    ] if p is not None] + [str(date_start), str(date_end)])

    if not sess_dur.empty:
        fig = px.box(sess_dur, x="practice", y="duration_min", color="practice",
                     title="Session Duration by Practice")
        fig.update_layout(yaxis_title="Duration (min)")
        st.plotly_chart(fig, width='stretch')

    # Prompt length by practice and level
    st.subheader("Prompt Length by Practice & Level")
    prompt_prac = load_df(f"""
        SELECT emp.practice, emp.level, ROUND(AVG(up.prompt_length), 0) AS avg_prompt_len
        FROM user_prompts up
        JOIN events e ON up.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        WHERE {where}
        GROUP BY emp.practice, emp.level ORDER BY emp.practice, emp.level
    """, params)
    if not prompt_prac.empty:
        fig = px.bar(prompt_prac, x="level", y="avg_prompt_len", color="practice",
                     barmode="group", title="Average Prompt Length by Level & Practice")
        st.plotly_chart(fig, width='stretch')

# ===================================================================
# TAB 8: Predictions & Statistical Analysis
# ===================================================================

with tab_predict:
    st.subheader("🔮 Predictive Analytics & Statistical Analysis")

    # --- Cost forecasting with linear regression ---
    st.markdown("### Daily Cost Forecast (Linear Trend)")
    daily_cost = load_df("""
        SELECT DATE(e.event_timestamp) AS date,
            ROUND(SUM(ar.cost_usd), 2) AS daily_cost
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        GROUP BY date ORDER BY date
    """)

    if not daily_cost.empty and len(daily_cost) >= 5:
        import numpy as np
        from sklearn.linear_model import LinearRegression

        daily_cost["date_dt"] = pd.to_datetime(daily_cost["date"])
        daily_cost["day_num"] = (daily_cost["date_dt"] - daily_cost["date_dt"].min()).dt.days

        X = daily_cost[["day_num"]].values
        y = daily_cost["daily_cost"].values
        model = LinearRegression().fit(X, y)

        # Forecast next 14 days
        last_day = daily_cost["day_num"].max()
        future_days = np.arange(last_day + 1, last_day + 15).reshape(-1, 1)
        future_dates = [daily_cost["date_dt"].max() + timedelta(days=i) for i in range(1, 15)]
        forecast = model.predict(future_days)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=daily_cost["date_dt"], y=daily_cost["daily_cost"],
                                 name="Actual", mode="lines+markers"))
        # Trend line on historical data
        fig.add_trace(go.Scatter(x=daily_cost["date_dt"], y=model.predict(X),
                                 name="Trend", line=dict(dash="dash", color="gray")))
        fig.add_trace(go.Scatter(x=future_dates, y=forecast,
                                 name="Forecast (14 days)", mode="lines+markers",
                                 line=dict(dash="dot", color="red")))
        fig.update_layout(title=f"Cost Forecast (slope: ${model.coef_[0]:.3f}/day)",
                          xaxis_title="Date", yaxis_title="Daily Cost (USD)")
        st.plotly_chart(fig, width='stretch')

        r2 = model.score(X, y)
        st.info(f"**Linear model R²**: {r2:.3f} | **Daily trend**: ${model.coef_[0]:+.3f}/day | "
                f"**14-day forecast total**: ${forecast.sum():.2f}")

    # --- Token usage forecast ---
    st.markdown("### Daily Token Usage Forecast")
    tok_daily_pred = load_df("""
        SELECT DATE(e.event_timestamp) AS date,
            SUM(ar.input_tokens + ar.output_tokens) AS daily_tokens
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        GROUP BY date ORDER BY date
    """)

    if not tok_daily_pred.empty and len(tok_daily_pred) >= 5:
        import numpy as np
        from sklearn.linear_model import LinearRegression

        tok_daily_pred["date_dt"] = pd.to_datetime(tok_daily_pred["date"])
        tok_daily_pred["day_num"] = (tok_daily_pred["date_dt"] - tok_daily_pred["date_dt"].min()).dt.days

        X_t = tok_daily_pred[["day_num"]].values
        y_t = tok_daily_pred["daily_tokens"].values
        model_t = LinearRegression().fit(X_t, y_t)

        last_day_t = tok_daily_pred["day_num"].max()
        future_days_t = np.arange(last_day_t + 1, last_day_t + 15).reshape(-1, 1)
        future_dates_t = [tok_daily_pred["date_dt"].max() + timedelta(days=i) for i in range(1, 15)]
        forecast_t = model_t.predict(future_days_t)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=tok_daily_pred["date_dt"], y=tok_daily_pred["daily_tokens"],
                                 name="Actual", mode="lines+markers"))
        fig.add_trace(go.Scatter(x=tok_daily_pred["date_dt"], y=model_t.predict(X_t),
                                 name="Trend", line=dict(dash="dash", color="gray")))
        fig.add_trace(go.Scatter(x=future_dates_t, y=forecast_t,
                                 name="Forecast (14 days)", mode="lines+markers",
                                 line=dict(dash="dot", color="orange")))
        fig.update_layout(title=f"Token Usage Forecast (slope: {model_t.coef_[0]:+,.0f} tokens/day)",
                          xaxis_title="Date", yaxis_title="Daily Tokens")
        st.plotly_chart(fig, width='stretch')

        r2_t = model_t.score(X_t, y_t)
        st.info(f"**Linear model R²**: {r2_t:.3f} | **Daily trend**: {model_t.coef_[0]:+,.0f} tokens/day | "
                f"**14-day forecast total**: {forecast_t.sum():,.0f} tokens")

    # --- Anomaly detection on sessions ---
    st.markdown("### Session Anomaly Detection (Isolation Forest)")
    sess_features = load_df("""
        SELECT session_id, user_email, practice, level,
            total_cost_usd, api_call_count, total_input_tokens, total_output_tokens,
            event_count, duration_seconds
        FROM v_session_summary
        WHERE total_cost_usd > 0
    """)

    if not sess_features.empty and len(sess_features) >= 20:
        from sklearn.ensemble import IsolationForest

        feature_cols = ["total_cost_usd", "api_call_count", "total_input_tokens",
                        "total_output_tokens", "event_count", "duration_seconds"]
        X_feat = sess_features[feature_cols].fillna(0)

        iso = IsolationForest(contamination=0.05, random_state=42)
        sess_features["anomaly"] = iso.fit_predict(X_feat)
        sess_features["is_anomaly"] = sess_features["anomaly"] == -1

        anomalies = sess_features[sess_features["is_anomaly"]]
        normal = sess_features[~sess_features["is_anomaly"]]

        fig = px.scatter(sess_features, x="api_call_count", y="total_cost_usd",
                         color="is_anomaly", size="event_count",
                         hover_data=["session_id", "user_email", "practice"],
                         title=f"Session Anomalies ({len(anomalies)} flagged out of {len(sess_features)})",
                         color_discrete_map={False: "steelblue", True: "red"})
        fig.update_layout(xaxis_title="API Calls", yaxis_title="Cost (USD)")
        st.plotly_chart(fig, width='stretch')

        if not anomalies.empty:
            st.warning(f"**{len(anomalies)} anomalous sessions detected** (top by cost):")
            st.dataframe(anomalies.nlargest(10, "total_cost_usd")[
                ["session_id", "user_email", "practice", "level",
                 "total_cost_usd", "api_call_count", "event_count"]
            ], width='stretch', hide_index=True)

    # --- Statistical correlations ---
    st.markdown("### Statistical Analysis")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Seniority vs Cost Correlation")
        level_cost = load_df("""
            SELECT emp.level,
                CAST(REPLACE(emp.level, 'L', '') AS INTEGER) AS level_num,
                ROUND(SUM(ar.cost_usd) / COUNT(DISTINCT e.user_email), 2) AS cost_per_user
            FROM api_requests ar
            JOIN events e ON ar.event_id = e.id
            JOIN employees emp ON e.user_email = emp.email
            GROUP BY emp.level ORDER BY level_num
        """)
        if not level_cost.empty:
            from scipy import stats
            r, p_val = stats.pearsonr(level_cost["level_num"], level_cost["cost_per_user"])
            fig = px.scatter(level_cost, x="level_num", y="cost_per_user",
                             title=f"Seniority vs Cost/User (r={r:.3f}, p={p_val:.3f})",
                             labels={"level_num": "Level", "cost_per_user": "Cost/User ($)"})
            coeffs = np.polyfit(level_cost["level_num"], level_cost["cost_per_user"], 1)
            x_range = np.linspace(level_cost["level_num"].min(), level_cost["level_num"].max(), 50)
            fig.add_trace(go.Scatter(x=x_range, y=np.polyval(coeffs, x_range),
                                     mode="lines", name="OLS Trend", line=dict(dash="dash")))
            st.plotly_chart(fig, width='stretch')
            if p_val < 0.05:
                st.success(f"Statistically significant correlation (p={p_val:.3f})")
            else:
                st.info(f"No significant correlation (p={p_val:.3f})")

    with col2:
        st.markdown("#### Session Duration vs Cost")
        dur_cost = load_df("""
            SELECT duration_seconds / 60.0 AS duration_min,
                total_cost_usd, practice
            FROM v_session_summary
            WHERE total_cost_usd > 0 AND duration_seconds > 0
        """)
        if not dur_cost.empty:
            from scipy import stats
            r, p_val = stats.pearsonr(dur_cost["duration_min"], dur_cost["total_cost_usd"])
            fig = px.scatter(dur_cost, x="duration_min", y="total_cost_usd",
                             color="practice", opacity=0.6,
                             title=f"Duration vs Cost (r={r:.3f}, p={p_val:.4f})",
                             labels={"duration_min": "Duration (min)", "total_cost_usd": "Cost ($)"})
            coeffs = np.polyfit(dur_cost["duration_min"], dur_cost["total_cost_usd"], 1)
            x_range = np.linspace(dur_cost["duration_min"].min(), dur_cost["duration_min"].max(), 50)
            fig.add_trace(go.Scatter(x=x_range, y=np.polyval(coeffs, x_range),
                                     mode="lines", name="OLS Trend", line=dict(dash="dash")))
            st.plotly_chart(fig, width='stretch')

    # Practice comparison box plots
    st.markdown("#### Cost Distribution by Practice (Statistical)")
    prac_costs = load_df("""
        SELECT emp.practice, ar.cost_usd
        FROM api_requests ar
        JOIN events e ON ar.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
    """)
    if not prac_costs.empty:
        fig = px.box(prac_costs, x="practice", y="cost_usd", color="practice",
                     title="API Request Cost Distribution by Practice", log_y=True)
        fig.update_layout(yaxis_title="Cost per Request (USD, log scale)")
        st.plotly_chart(fig, width='stretch')

        # Kruskal-Wallis test across practices
        from scipy import stats
        groups = [g["cost_usd"].values for _, g in prac_costs.groupby("practice")]
        if len(groups) >= 2:
            stat, p_val = stats.kruskal(*groups)
            if p_val < 0.05:
                st.success(f"**Kruskal-Wallis test**: Significant difference in costs across practices "
                           f"(H={stat:.2f}, p={p_val:.4f})")
            else:
                st.info(f"**Kruskal-Wallis test**: No significant difference (H={stat:.2f}, p={p_val:.4f})")

    # Practice-specific patterns
    st.markdown("#### Practice-Specific Model Preferences")
    model_prefs = load_df("""
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
    if not model_prefs.empty:
        fig = px.bar(model_prefs, x="practice", y="requests", color="model",
                     title="Model Usage by Practice", barmode="group",
                     hover_data=["total_cost", "avg_tokens"])
        st.plotly_chart(fig, width='stretch')

    st.markdown("#### Practice-Specific Tool Usage Patterns")
    tool_prefs = load_df("""
        SELECT emp.practice, tr.tool_name,
            COUNT(*) AS uses,
            ROUND(100.0 * SUM(tr.success) / COUNT(*), 1) AS success_rate
        FROM tool_results tr
        JOIN events e ON tr.event_id = e.id
        JOIN employees emp ON e.user_email = emp.email
        GROUP BY emp.practice, tr.tool_name
        ORDER BY emp.practice, uses DESC
    """)
    if not tool_prefs.empty:
        # Top 6 tools overall for readability
        top_tool_names = tool_prefs.groupby("tool_name")["uses"].sum().nlargest(6).index.tolist()
        filtered_tp = tool_prefs[tool_prefs["tool_name"].isin(top_tool_names)]
        fig = px.bar(filtered_tp, x="practice", y="uses", color="tool_name",
                     title="Top Tool Usage by Practice", barmode="group",
                     hover_data=["success_rate"])
        st.plotly_chart(fig, width='stretch')

# ===================================================================
# Real-time simulation (sidebar toggle)
# ===================================================================

st.sidebar.divider()
st.sidebar.subheader("🔴 Real-time Simulation")
if st.sidebar.toggle("Enable live stream"):
    import time
    import random

    placeholder = st.empty()
    with placeholder.container():
        st.subheader("🔴 Live Event Stream")
        st.caption("Simulating real-time telemetry events...")

        # Pull recent events as seed data
        recent = load_df("""
            SELECT e.event_type, e.user_email, e.event_timestamp,
                emp.practice, emp.level
            FROM events e
            JOIN employees emp ON e.user_email = emp.email
            ORDER BY e.event_timestamp DESC LIMIT 50
        """)

        event_log = []
        chart_data = pd.DataFrame(columns=["time", "events"])

        for i in range(20):
            # Simulate a burst of events
            n_events = random.randint(1, 8)
            ts = datetime.now().strftime("%H:%M:%S")
            for _ in range(n_events):
                row = recent.sample(1).iloc[0]
                event_log.append({
                    "time": ts,
                    "type": row["event_type"],
                    "user": row["user_email"],
                    "practice": row["practice"],
                })

            new_row = pd.DataFrame([{"time": ts, "events": n_events}])
            chart_data = pd.concat([chart_data, new_row], ignore_index=True)

            with placeholder.container():
                st.subheader("🔴 Live Event Stream")
                st.line_chart(chart_data.set_index("time")["events"])
                st.dataframe(
                    pd.DataFrame(event_log[-10:][::-1]),
                    width='stretch', hide_index=True
                )
            time.sleep(0.5)

        st.success("Simulation complete (20 ticks)")
