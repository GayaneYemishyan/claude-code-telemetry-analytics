# Claude Code Telemetry Analytics Platform

An end-to-end analytics platform that ingests Claude Code telemetry data, stores it in a normalized SQLite database, surfaces insights through reusable analytics queries, and presents them via an interactive Streamlit dashboard and a REST API.

---

## Architecture

```
generate_fake_data.py       ← Synthetic data generator (JSONL + CSV)
        │
        ▼
   output/
   ├─ telemetry_logs.jsonl  ← Nested JSONL telemetry batches
   └─ employees.csv         ← Employee directory
        │
        ▼
    ingest.py               ← ETL: parse, flatten, join → SQLite
        │
        ▼
   telemetry.db             ← Normalized SQLite database
        │
   ┌────┼────────────┐
   ▼    ▼            ▼
analytics.py  dashboard.py  api.py
(CLI report)  (Streamlit)   (FastAPI)
```

### Components

| File | Purpose |
|------|---------|
| `generate_fake_data.py` | Generates synthetic telemetry JSONL and employee CSV (stdlib only) |
| `ingest.py` | ETL pipeline — parses nested JSONL, flattens events, joins with employees, loads into SQLite |
| `analytics.py` | Reusable analytics queries + CLI report (token usage, cost, peak hours, tools, errors, sessions, forecasting, anomaly detection, statistical tests) |
| `dashboard.py` | Interactive Streamlit dashboard with filters and 8 visualization tabs |
| `api.py` | FastAPI REST API for programmatic access to all analytics |
| `requirements.txt` | Pinned Python dependencies |

### Database Schema (SQLite)

| Table | Description |
|-------|-------------|
| `employees` | Employee dimension (email, name, practice, level, location) |
| `sessions` | Session dimension (session_id, user, terminal, timestamps) |
| `events` | Unified fact table (event_type, timestamp, session, user, host info) |
| `api_requests` | API call details (model, tokens, cost, duration) |
| `tool_decisions` | Tool accept/reject decisions |
| `tool_results` | Tool execution results (success, duration, size) |
| `user_prompts` | User prompt metadata |
| `api_errors` | API error details (model, error, status code) |
| `v_api_requests` | View: API requests + employee info |
| `v_tool_results` | View: Tool results + employee info |
| `v_session_summary` | View: Session-level aggregates |

---

## Setup Instructions

### Prerequisites

- **Python 3.11+**

### 1. Clone the repository

```bash
git clone <git@github.com:GayaneYemishyan/claude-code-telemetry-analytics.git>
cd claude-code-telemetry-analytics
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Generate synthetic data

```bash
python generate_fake_data.py
```

For a larger dataset:

```bash
python generate_fake_data.py --num-users 100 --num-sessions 5000 --days 60
```

| Flag | Default | Description |
|------|---------|-------------|
| `--num-users` | 30 | Number of engineers |
| `--num-sessions` | 500 | Total coding sessions |
| `--days` | 30 | Time span in days |
| `--output-dir` | `output` | Output directory |
| `--seed` | 42 | Random seed for reproducibility |

### 5. Ingest data into SQLite

```bash
python ingest.py
```

Options: `--input-dir output` (default) and `--db telemetry.db` (default).

### 6. Run the analytics report (CLI)

```bash
python analytics.py
```

### 7. Launch the Streamlit dashboard

```bash
streamlit run dashboard.py
```

Opens at `http://localhost:8501`. Use the sidebar to filter by practice, seniority level, location, model, and date range.

### 8. Start the REST API

```bash
uvicorn api:app --reload --port 8000
```

Interactive docs at `http://localhost:8000/docs`.

---

## Dashboard Tabs

| Tab | Content |
|-----|---------|
| **Overview** | KPI cards (users, sessions, events, cost), cost by practice/model donuts, daily event trend |
| **Tokens** | Token consumption by model, practice, seniority; cache efficiency |
| **Cost** | Top spenders, daily cost trend, cost per session by practice |
| **Usage Times** | Hourly distribution, day-of-week patterns, business vs. off-hours split |
| **Tools** | Tool usage ranking, success rates, practice-specific tool preferences |
| **Errors** | Error types, error rates by model, error trends |
| **Sessions** | Session duration distribution, sessions per user, cost per session |
| **Predictions** | Cost forecast, token usage forecast, session anomaly detection (Isolation Forest), seniority vs cost correlation, duration vs cost correlation, Kruskal-Wallis practice comparison, practice-specific model & tool patterns |

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/employees` | List employees (filterable) |
| `GET` | `/tokens/by-model` | Token usage by model |
| `GET` | `/tokens/by-practice` | Token usage by practice |
| `GET` | `/tokens/by-level` | Token usage by seniority |
| `GET` | `/cost/by-user` | Cost per user |
| `GET` | `/cost/daily` | Daily cost trend |
| `GET` | `/cost/sessions` | Cost per session |
| `GET` | `/usage/hourly` | Hourly usage distribution |
| `GET` | `/usage/daily` | Daily usage patterns |
| `GET` | `/usage/business-hours` | Business vs. off-hours |
| `GET` | `/tools/summary` | Tool usage summary |
| `GET` | `/tools/by-practice` | Tools by practice |
| `GET` | `/errors/by-type` | Errors by type |
| `GET` | `/errors/by-model` | Errors by model |
| `GET` | `/sessions/overview` | Session overview |
| `GET` | `/sessions/{session_id}` | Session detail |
| `GET` | `/predict/cost-forecast` | Cost forecast (linear regression, configurable days) |
| `GET` | `/predict/token-forecast` | Token usage forecast (linear regression) |
| `GET` | `/predict/anomalies` | Anomalous session detection (Isolation Forest) |
| `GET` | `/stats/seniority-cost-correlation` | Pearson correlation: seniority vs cost |
| `GET` | `/stats/practice-cost-comparison` | Kruskal-Wallis test across practices |
| `GET` | `/stats/practice-patterns` | Practice-specific model & tool preferences |

All endpoints accept optional query parameters where applicable: `practice`, `level`, `location`, `model`, `date_start`, `date_end`.

---

## Dependencies

Core libraries (see `requirements.txt` for pinned versions):

| Package | Purpose |
|---------|---------|
| `streamlit` | Interactive dashboard UI |
| `plotly` | Interactive charts |
| `pandas` | Data manipulation |
| `numpy` | Numerical computation |
| `scikit-learn` | Machine learning (predictions) |
| `scipy` | Scientific computing |
| `statsmodels` | Statistical analysis / forecasting |
| `fastapi` | REST API framework |
| `starlette` | ASGI framework (FastAPI dependency) |
| `uvicorn` | ASGI server (install separately: `pip install uvicorn`) |

---

## Bonus Features (Step 5)

### Predictive Analytics
- **Cost forecast** — Linear regression on daily cost with 14-day projection (configurable)
- **Token usage forecast** — Linear regression on daily token consumption
- **Anomaly detection** — Isolation Forest on session features (cost, tokens, duration, API calls) to flag outlier sessions

### Real-time Simulation
- Sidebar toggle in the dashboard streams simulated telemetry events with a live-updating chart and event log

### API Layer
- FastAPI REST API with 24 endpoints covering all analytics, predictions, and statistical tests
- Auto-generated Swagger docs at `/docs`
- CORS-enabled for frontend integration

### Statistical Analysis
- **Seniority vs cost** — Pearson correlation with significance test
- **Session duration vs cost** — Scatter plot with OLS trendline
- **Practice cost comparison** — Kruskal-Wallis H-test across engineering practices
- **Practice-specific patterns** — Model preferences and tool usage breakdown per practice

---

## Data Notes

- All user identifiers are synthetic
- Prompt contents are redacted
- Employee emails match telemetry data
- Telemetry structure: batches → `logEvents` → JSON `message` → `{body, attributes, scope, resource}`

---

## LLM Usage Log

### Tools Used

- **GitHub Copilot (Claude Opus 4.6)** in VS Code — used for all code generation, architecture decisions, and documentation throughout the project.

### Key Prompts & What They Produced

| Step | Prompt (summarized) | Output |
|------|---------------------|--------|
| Data Ingestion | *"Implement data ingestion & storage — parse the nested JSONL, flatten events, join with employee CSV, store in SQLite with a normalized schema"* | `ingest.py` — full ETL pipeline with schema design, JSONL parsing, event flattening, and employee joins |
| Analytics | *"Implement analytics & insights — token consumption, cost analysis, peak usage times, tool usage patterns, error analysis, session behavior"* | `analytics.py` — 20+ reusable query functions covering all insight areas, plus a CLI report |
| Dashboard | *"Build a Streamlit app that visualizes the insights with filters by practice, level, location, model, date range"* | `dashboard.py` — 8-tab interactive dashboard with sidebar filters and Plotly charts |
| Bonus Features | *"Add predictive analytics, real-time simulation, API layer, statistical analysis"* | `api.py` — FastAPI REST API with 24 endpoints; Predictions tab with cost/token forecasting, anomaly detection, and statistical analysis (correlations, hypothesis tests, practice patterns); real-time simulation in dashboard |


### Validation Approach

1. **Execution testing** — Every generated script was run immediately after creation. `ingest.py` was verified by confirming `telemetry.db` was created with correct table row counts. `analytics.py` output was reviewed for plausible aggregations.
2. **Schema review** — The SQLite schema was manually inspected to ensure proper foreign keys, indexes, and normalized table design.
3. **Query spot-checks** — Analytics queries were run independently against the database to verify results matched expected patterns (e.g., higher-tier models cost more, senior engineers have different usage patterns).
4. **Dashboard visual inspection** — Each dashboard tab was loaded in the browser to confirm charts rendered correctly, filters propagated, and KPI numbers were consistent across tabs.
5. **API testing** — FastAPI endpoints were tested via the auto-generated `/docs` Swagger UI, verifying response structure and filter parameters.
6. **Code review** — All generated code was reviewed for correctness, SQL injection safety (parameterized queries throughout), and adherence to project requirements before committing.
