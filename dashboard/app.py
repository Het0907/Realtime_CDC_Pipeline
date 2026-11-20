"""
dashboard/app.py
=================
Next-Gen Modern UI Dashboard for the Real-Time CDC Lakehouse Platform.
Architecture: Postgres (WAL) → Debezium → Kafka → Delta Lake → dbt (DuckDB) → Great Expectations

Pages:
  🏠 Overview        — Real-time health, Medallion flow widget, KPI metrics, operation analytics
  📡 Live CDC Feed   — Event stream with search, operation filtering, & before/after state diff
  👥 Customers       — Silver current state, Gold customer intelligence mart, CDC operations
  📦 Orders          — Dual-axis sales velocity, enriched Gold order facts, & raw transactions
  🏷️ Products       — Gold product leaderboard, category weights & catalog inventory
  ✅ Data Quality    — Great Expectations automated quality gate, pass/fail audit cards
  🔍 Time Travel     — Delta Lake historical version query, version diffing, & transaction log
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Path setup ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
BRONZE_DIR   = PROJECT_ROOT / "bronze_layer"
SILVER_DIR   = PROJECT_ROOT / "silver"
GOLD_DB      = PROJECT_ROOT / "gold" / "cdc_lakehouse.duckdb"
GQ_RESULTS   = PROJECT_ROOT / "data_quality" / "results" / "gx_results_latest.json"

sys.path.insert(0, str(PROJECT_ROOT))

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CDC Lakehouse | Real-Time Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom Modern CSS ──────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

  /* Global typography & background */
  html, body, [class*="css"] {
    font-family: 'Plus Jakarta Sans', sans-serif;
  }
  
  code, pre, .mono-font {
    font-family: 'JetBrains Mono', monospace !important;
  }

  .stApp {
    background: radial-gradient(circle at 15% 15%, #0f172a 0%, #090d16 50%, #030712 100%);
    color: #f1f5f9;
  }

  /* Glassmorphism sidebar */
  [data-testid="stSidebar"] {
    background: linear-gradient(180deg, rgba(15, 23, 42, 0.95) 0%, rgba(3, 7, 18, 0.98) 100%);
    border-right: 1px solid rgba(255, 255, 255, 0.08);
    backdrop-filter: blur(16px);
  }

  /* Stat & KPI Cards */
  .stat-card {
    background: rgba(30, 41, 59, 0.5);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 16px;
    padding: 20px 22px;
    backdrop-filter: blur(12px);
    box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
    position: relative;
    overflow: hidden;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  }
  .stat-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 12px 40px 0 rgba(56, 189, 248, 0.15);
    border-color: rgba(56, 189, 248, 0.3);
  }
  .stat-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 3px;
    background: linear-gradient(90deg, #38bdf8, #818cf8, #c084fc);
  }
  .stat-title {
    font-size: 0.85rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #94a3b8;
    margin-bottom: 8px;
  }
  .stat-value {
    font-size: 2.2rem;
    font-weight: 800;
    color: #ffffff;
    line-height: 1.1;
    letter-spacing: -0.02em;
  }
  .stat-subtext {
    font-size: 0.8rem;
    color: #64748b;
    margin-top: 8px;
    display: flex;
    align-items: center;
    gap: 6px;
  }

  /* Live pulse badge */
  .pulse-badge {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: rgba(16, 185, 129, 0.12);
    border: 1px solid rgba(16, 185, 129, 0.3);
    color: #34d399;
    padding: 4px 12px;
    border-radius: 9999px;
    font-size: 0.78rem;
    font-weight: 600;
  }
  .pulse-dot {
    width: 8px;
    height: 8px;
    background-color: #10b981;
    border-radius: 50%;
    box-shadow: 0 0 10px #10b981;
    animation: pulse 1.8s infinite;
  }
  @keyframes pulse {
    0% { transform: scale(0.95); opacity: 0.7; }
    50% { transform: scale(1.3); opacity: 1; }
    100% { transform: scale(0.95); opacity: 0.7; }
  }

  /* Section headers */
  .section-header-modern {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: linear-gradient(90deg, rgba(30, 58, 138, 0.25) 0%, rgba(15, 23, 42, 0) 100%);
    border-left: 4px solid #38bdf8;
    border-radius: 0 12px 12px 0;
    padding: 12px 20px;
    margin: 24px 0 16px 0;
    font-weight: 700;
    font-size: 1.15rem;
    color: #f8fafc;
  }

  /* Operation Pills */
  .pill-op {
    padding: 3px 10px;
    border-radius: 8px;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.03em;
    display: inline-block;
  }
  .pill-c { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }
  .pill-u { background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }
  .pill-d { background: rgba(244, 63, 94, 0.2); color: #fb7185; border: 1px solid rgba(244, 63, 94, 0.4); }
  .pill-r { background: rgba(99, 102, 241, 0.2); color: #818cf8; border: 1px solid rgba(99, 102, 241, 0.4); }

  /* Event feed cards */
  .event-card {
    background: rgba(30, 41, 59, 0.4);
    border: 1px solid rgba(255, 255, 255, 0.06);
    border-radius: 12px;
    padding: 14px 18px;
    margin-bottom: 10px;
    transition: all 0.2s ease;
  }
  .event-card:hover {
    background: rgba(30, 41, 59, 0.7);
    border-color: rgba(56, 189, 248, 0.4);
  }

  /* Modern Tab styling */
  .stTabs [data-baseweb="tab-list"] {
    background: rgba(15, 23, 42, 0.7);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 12px;
    padding: 6px;
    gap: 8px;
  }
  .stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    color: #94a3b8;
    font-weight: 600;
    padding: 8px 16px;
  }
  .stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #0284c7 0%, #4f46e5 100%) !important;
    color: #ffffff !important;
    box-shadow: 0 4px 14px rgba(2, 132, 199, 0.4);
  }

  /* Medallion Pipeline Graphic Box */
  .pipeline-grid {
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 12px;
    background: rgba(15, 23, 42, 0.6);
    border: 1px solid rgba(56, 189, 248, 0.2);
    border-radius: 18px;
    padding: 22px 18px;
    backdrop-filter: blur(12px);
    box-shadow: 0 10px 30px rgba(0,0,0,0.4);
    margin-bottom: 24px;
  }
  .stage-box {
    background: rgba(30, 41, 59, 0.6);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 14px;
    padding: 16px 12px;
    text-align: center;
    position: relative;
    transition: transform 0.2s ease;
  }
  .stage-box:hover {
    transform: translateY(-2px);
    border-color: #38bdf8;
  }
  .stage-icon { font-size: 1.8rem; margin-bottom: 6px; }
  .stage-title { font-weight: 700; font-size: 0.88rem; color: #f8fafc; }
  .stage-sub { font-size: 0.72rem; color: #94a3b8; margin-top: 4px; }
  .stage-badge {
    margin-top: 8px;
    font-size: 0.68rem;
    padding: 2px 8px;
    border-radius: 6px;
    font-weight: 600;
  }
</style>
""", unsafe_allow_html=True)


# ── Data Loaders & Caching ─────────────────────────────────────────────────────

@st.cache_data(ttl=20)
def load_bronze_events(table: str) -> pd.DataFrame:
    table_dir = BRONZE_DIR / table
    rows = []
    if not table_dir.exists():
        return pd.DataFrame()
    for f in sorted(table_dir.glob("*.json")):
        try:
            with open(f, encoding="utf-8") as fp:
                ev = json.load(fp)
            row_data = ev.get("after") or ev.get("before") or {}
            rows.append({
                "table":        table,
                "operation":    ev.get("operation", "?"),
                "timestamp_ms": ev.get("timestamp_ms", 0),
                "ingested_at":  ev.get("ingested_at", ""),
                "id":           row_data.get("id"),
                "before":       ev.get("before"),
                "after":        ev.get("after"),
                "filename":     f.name,
            })
        except Exception:
            continue
    df = pd.DataFrame(rows)
    if not df.empty:
        df["event_time"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    return df


@st.cache_data(ttl=20)
def load_all_bronze_events() -> pd.DataFrame:
    frames = [load_bronze_events(t) for t in ["customers", "orders", "products"]]
    valid = [f for f in frames if not f.empty]
    return pd.concat(valid, ignore_index=True) if valid else pd.DataFrame()


@st.cache_data(ttl=20)
def load_silver_table(table: str) -> pd.DataFrame:
    try:
        from deltalake import DeltaTable
        return DeltaTable(str(SILVER_DIR / table)).to_pandas()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=20)
def load_gold_mart(table_name: str) -> pd.DataFrame:
    if not GOLD_DB.exists():
        return pd.DataFrame()
    try:
        con = duckdb.connect(str(GOLD_DB), read_only=True)
        df = con.execute(f"SELECT * FROM main_marts.{table_name}").df()
        con.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=20)
def load_gq_results() -> dict:
    if not GQ_RESULTS.exists():
        return {}
    try:
        with open(GQ_RESULTS, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def load_delta_versions(table: str) -> list[dict]:
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(str(SILVER_DIR / table))
        return dt.history()
    except Exception:
        return []


def load_silver_as_of_version(table: str, version: int) -> pd.DataFrame:
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(str(SILVER_DIR / table), version=version)
        return dt.to_pandas()
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


# ── Sidebar Navigation ─────────────────────────────────────────────────────────

def render_sidebar():
    with st.sidebar:
        st.markdown("""
        <div style="text-align:center; padding: 16px 0 12px 0;">
            <div style="font-size: 2.4rem; filter: drop-shadow(0 0 16px #38bdf8);">⚡</div>
            <div style="font-size: 1.35rem; font-weight: 800; background: linear-gradient(90deg, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">CDC Lakehouse</div>
            <div style="font-size: 0.78rem; color: #94a3b8; margin-top: 4px; font-weight: 500;">Real-Time Data Platform</div>
            <div style="margin-top: 10px;">
                <span class="pulse-badge"><span class="pulse-dot"></span> Pipeline Active</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

        page = st.radio(
            "Navigation",
            options=[
                "🏠 Overview",
                "📡 Live CDC Feed",
                "👥 Customers",
                "📦 Orders",
                "🏷️ Products",
                "✅ Data Quality",
                "🔍 Time Travel",
            ],
            label_visibility="collapsed",
        )

        st.markdown("---")

        # System Health Card
        gq = load_gq_results()
        if gq:
            all_pass = all(v.get("failed", 0) == 0 for v in gq.get("tables", {}).values())
            color = "#10b981" if all_pass else "#f43f5e"
            status_text = "All Contracts Passing" if all_pass else "Violations Detected"
            ts = gq.get("run_timestamp", "")[:19].replace("T", " ")
            st.markdown(f"""
            <div style="background: rgba(30, 41, 59, 0.6); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px; padding: 12px 14px; border-left: 4px solid {color};">
                <div style="font-size: 0.72rem; color: #94a3b8; text-transform: uppercase; font-weight: 700;">Data Quality Gate</div>
                <div style="color: {color}; font-size: 0.88rem; font-weight: 700; margin-top: 2px;">{status_text}</div>
                <div style="font-size: 0.72rem; color: #64748b; margin-top: 4px;">Audited: {ts} UTC</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

        # Refresh Action
        if st.button("🔄 Refresh Cache", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown("""
        <div style="margin-top: 24px; font-size: 0.72rem; color: #475569; text-align: center; line-height: 1.6;">
            <strong>Stack:</strong> Postgres WAL • Debezium<br/>
            Kafka • Delta Lake • dbt • DuckDB
        </div>
        """, unsafe_allow_html=True)

    return page


# ── Page: Overview ─────────────────────────────────────────────────────────────

def page_overview():
    # Hero Title with Quick Badges
    st.markdown("""
    <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-bottom: 20px;">
        <div>
            <h1 style="margin:0; font-size: 2.2rem; font-weight: 800; letter-spacing: -0.02em;">⚡ CDC Lakehouse Platform</h1>
            <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
                Continuous row-level replication from PostgreSQL WAL to Delta Lake ACID tables & dbt Gold Marts
            </p>
        </div>
        <div>
            <span class="pulse-badge"><span class="pulse-dot"></span> Streaming Ingestion</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Medallion Architecture Pipeline Flow Widget
    st.markdown("""
    <div class="pipeline-grid">
        <div class="stage-box">
            <div class="stage-icon">🐘</div>
            <div class="stage-title">Postgres</div>
            <div class="stage-sub">OLTP WAL Log</div>
            <div class="stage-badge" style="background:rgba(56,189,248,0.2); color:#38bdf8;">Logical Repl</div>
        </div>
        <div class="stage-box">
            <div class="stage-icon">⚡</div>
            <div class="stage-title">Debezium</div>
            <div class="stage-sub">Kafka Connect</div>
            <div class="stage-badge" style="background:rgba(129,140,248,0.2); color:#818cf8;">Sub-sec CDC</div>
        </div>
        <div class="stage-box">
            <div class="stage-icon">📨</div>
            <div class="stage-title">Kafka</div>
            <div class="stage-sub">Event Backbone</div>
            <div class="stage-badge" style="background:rgba(192,132,252,0.2); color:#c084fc;">3 Topics</div>
        </div>
        <div class="stage-box">
            <div class="stage-icon">🥉</div>
            <div class="stage-title">Bronze</div>
            <div class="stage-sub">Raw Ingestion</div>
            <div class="stage-badge" style="background:rgba(251,146,60,0.2); color:#fb923c;">Immutable</div>
        </div>
        <div class="stage-box">
            <div class="stage-icon">🥈</div>
            <div class="stage-title">Silver</div>
            <div class="stage-sub">Delta Lake ACID</div>
            <div class="stage-badge" style="background:rgba(52,211,153,0.2); color:#34d399;">MERGE / Upsert</div>
        </div>
        <div class="stage-box">
            <div class="stage-icon">🥇</div>
            <div class="stage-title">Gold</div>
            <div class="stage-sub">dbt + DuckDB</div>
            <div class="stage-badge" style="background:rgba(250,204,21,0.2); color:#facc15;">Marts & Views</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Live Data Load
    all_events = load_all_bronze_events()
    c_events   = load_bronze_events("customers")
    o_events   = load_bronze_events("orders")
    p_events   = load_bronze_events("products")

    df_cust = load_silver_table("customers")
    df_ord  = load_silver_table("orders")
    df_prod = load_silver_table("products")
    gq      = load_gq_results()

    active_customers = len(df_cust[df_cust["_is_deleted"] == False]) if not df_cust.empty and "_is_deleted" in df_cust.columns else len(df_cust)
    total_orders = len(df_ord[df_ord["_is_deleted"] == False]) if not df_ord.empty and "_is_deleted" in df_ord.columns else len(df_ord)

    # 4 Stat Cards
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Captured CDC Events</div>
            <div class="stat-value">{len(all_events):,}</div>
            <div class="stat-subtext">📥 Append-only Bronze events</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Active Silver Customers</div>
            <div class="stat-value">{active_customers:,}</div>
            <div class="stat-subtext">👤 Deduplicated via MERGE</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Processed Orders</div>
            <div class="stat-value">{total_orders:,}</div>
            <div class="stat-subtext">📦 Real-time transaction state</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        if gq:
            failed = sum(v.get("failed", 0) for v in gq.get("tables", {}).values())
            passed = sum(v.get("passed", 0) for v in gq.get("tables", {}).values())
            pass_rate = int((passed / (passed + failed)) * 100) if (passed + failed) > 0 else 100
            val_text = f"{pass_rate}%"
            sub_text = "🛡️ 0 violations detected" if failed == 0 else f"⚠️ {failed} violations"
        else:
            val_text = "100%"
            sub_text = "🛡️ Initializing gate"
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Data Quality Gate</div>
            <div class="stat-value" style="color: #34d399;">{val_text}</div>
            <div class="stat-subtext">{sub_text}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)

    # Visual Analytics Row
    if not all_events.empty:
        col_left, col_right = st.columns([1, 1])

        with col_left:
            st.markdown('<div class="section-header-modern"><span>📊 Operations Distribution</span><span style="font-size:0.8rem; color:#94a3b8;">INSERT • UPDATE • DELETE</span></div>', unsafe_allow_html=True)
            op_map = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}
            all_events["op_name"] = all_events["operation"].map(op_map).fillna("OTHER")
            op_df = all_events["op_name"].value_counts().reset_index()
            op_df.columns = ["Operation", "Count"]

            fig_donut = px.pie(
                op_df, values="Count", names="Operation",
                hole=0.6,
                color="Operation",
                color_discrete_map={
                    "INSERT": "#10b981",
                    "UPDATE": "#f59e0b",
                    "DELETE": "#f43f5e",
                    "SNAPSHOT": "#6366f1",
                    "OTHER": "#94a3b8"
                },
                template="plotly_dark"
            )
            fig_donut.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Plus Jakarta Sans", color="#94a3b8"),
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_donut, use_container_width=True)

        with col_right:
            st.markdown('<div class="section-header-modern"><span>📈 Events by Entity Table</span><span style="font-size:0.8rem; color:#94a3b8;">Customers • Orders • Products</span></div>', unsafe_allow_html=True)
            tbl_df = all_events.groupby(["table", "op_name"]).size().reset_index(name="Count")
            fig_bar = px.bar(
                tbl_df, x="table", y="Count", color="op_name",
                barmode="stack",
                color_discrete_map={
                    "INSERT": "#10b981",
                    "UPDATE": "#f59e0b",
                    "DELETE": "#f43f5e",
                    "SNAPSHOT": "#6366f1"
                },
                template="plotly_dark"
            )
            fig_bar.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Plus Jakarta Sans", color="#94a3b8"),
                margin=dict(l=10, r=10, t=10, b=10),
                legend_title_text="",
                xaxis_title="",
                yaxis_title="Total Events",
                legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    # Lakehouse Storage & Integrity Summary
    st.markdown('<div class="section-header-modern"><span>🗄️ Medallion Storage Breakdown</span><span style="font-size:0.8rem; color:#38bdf8;">Delta Lake ACID + DuckDB</span></div>', unsafe_allow_html=True)
    m1, m2, m3 = st.columns(3)
    
    with m1:
        st.markdown("""
        <div style="background: rgba(30,41,59,0.4); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px; padding: 16px;">
            <div style="font-weight:700; color:#fb923c; margin-bottom:8px;">🥉 Bronze Ingestion Layer</div>
            <div style="font-size:0.85rem; color:#cbd5e1; line-height:1.7;">
                • Format: <strong>Raw JSON & Spark Delta</strong><br/>
                • Storage: <code>bronze_layer/</code><br/>
                • Strategy: Append-Only Immutable<br/>
                • Ordering: Partition + Kafka Offset
            </div>
        </div>
        """, unsafe_allow_html=True)
    with m2:
        st.markdown("""
        <div style="background: rgba(30,41,59,0.4); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px; padding: 16px;">
            <div style="font-weight:700; color:#34d399; margin-bottom:8px;">🥈 Silver Conformed Layer</div>
            <div style="font-size:0.85rem; color:#cbd5e1; line-height:1.7;">
                • Format: <strong>Delta Lake Parquet</strong><br/>
                • Storage: <code>silver/</code><br/>
                • Upsert: Idempotent <code>MERGE</code> on PK<br/>
                • Deletes: Soft delete flag (<code>_is_deleted</code>)
            </div>
        </div>
        """, unsafe_allow_html=True)
    with m3:
        st.markdown("""
        <div style="background: rgba(30,41,59,0.4); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px; padding: 16px;">
            <div style="font-weight:700; color:#facc15; margin-bottom:8px;">🥇 Gold Dimensional Layer</div>
            <div style="font-size:0.85rem; color:#cbd5e1; line-height:1.7;">
                • Engine: <strong>dbt Core + DuckDB</strong><br/>
                • Storage: <code>gold/cdc_lakehouse.duckdb</code><br/>
                • Marts: Wide Enriched Facts & Summaries<br/>
                • Governance: Tested with Great Expectations
            </div>
        </div>
        """, unsafe_allow_html=True)


# ── Page: Live CDC Feed ────────────────────────────────────────────────────────

def page_cdc_feed():
    st.markdown("""
    <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-bottom: 20px;">
        <div>
            <h1 style="margin:0; font-size: 2.1rem; font-weight: 800;">📡 Live CDC Event Stream</h1>
            <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
                Streaming audit feed captured directly from PostgreSQL logical replication
            </p>
        </div>
        <div>
            <span class="pulse-badge"><span class="pulse-dot"></span> Live Log Tail</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    all_events = load_all_bronze_events()
    if all_events.empty:
        st.info("No bronze events captured yet. Run `python simulate_event.py --insert` or `consumer.py`.")
        return

    # Filter Controls
    c1, c2, c3 = st.columns([1.5, 2, 2.5])
    with c1:
        tables = st.multiselect("Table Filter", ["customers", "orders", "products"], default=["customers", "orders", "products"])
    with c2:
        ops = st.multiselect("Operation Filter", ["c (INSERT)", "u (UPDATE)", "d (DELETE)", "r (SNAPSHOT)"],
                            default=["c (INSERT)", "u (UPDATE)", "d (DELETE)", "r (SNAPSHOT)"])
    with c3:
        search = st.text_input("🔍 Search Record ID or Content", placeholder="e.g. 1001, alice, gadget...")

    op_map_rev = {"c (INSERT)": "c", "u (UPDATE)": "u", "d (DELETE)": "d", "r (SNAPSHOT)": "r"}
    selected_ops = [op_map_rev[o] for o in ops]

    filtered = all_events[
        all_events["table"].isin(tables) & all_events["operation"].isin(selected_ops)
    ].sort_values("timestamp_ms", ascending=False)

    if search.strip():
        s = search.strip().lower()
        filtered = filtered[
            filtered["id"].astype(str).str.lower().str.contains(s) |
            filtered["before"].astype(str).str.lower().str.contains(s) |
            filtered["after"].astype(str).str.lower().str.contains(s)
        ]

    st.markdown(f"<div style='color:#94a3b8; font-size:0.85rem; margin: 12px 0;'>Showing <strong>{len(filtered)}</strong> matching CDC events</div>", unsafe_allow_html=True)

    pill_styles = {
        "c": '<span class="pill-op pill-c">🟢 INSERT</span>',
        "u": '<span class="pill-op pill-u">🟡 UPDATE</span>',
        "d": '<span class="pill-op pill-d">🔴 DELETE</span>',
        "r": '<span class="pill-op pill-r">🟣 SNAPSHOT</span>',
    }

    # Render events with expandable diffs
    for _, row in filtered.head(50).iterrows():
        op = row["operation"]
        op_pill = pill_styles.get(op, f'<span class="pill-op">{op}</span>')
        ts = str(row.get("event_time", ""))[:19]
        rec_id = row.get("id", "?")
        tbl = row["table"]

        with st.container():
            st.markdown(f"""
            <div class="event-card">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <div style="display:flex; align-items:center; gap: 12px;">
                        {op_pill}
                        <span style="font-weight:700; color:#f8fafc; font-size:1rem;">{tbl}</span>
                        <span style="color:#64748b; font-size:0.85rem; font-family:'JetBrains Mono';">ID: {rec_id}</span>
                    </div>
                    <div style="color:#64748b; font-size:0.8rem; font-family:'JetBrains Mono';">
                        ⏱️ {ts} UTC
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            with st.expander(f"Inspect Payload & State Diff (ID: {rec_id})"):
                d1, d2 = st.columns(2)
                with d1:
                    st.caption("State Before Change (`before`):")
                    st.json(row["before"] if row["before"] is not None else {"status": "None (New Record)"})
                with d2:
                    st.caption("State After Change (`after`):")
                    st.json(row["after"] if row["after"] is not None else {"status": "None (Deleted Record)"})


# ── Page: Customers ────────────────────────────────────────────────────────────

def page_customers():
    st.markdown("""
    <div style="margin-bottom: 20px;">
        <h1 style="margin:0; font-size: 2.1rem; font-weight: 800;">👥 Customer Analytics</h1>
        <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
            Silver Conformed Delta Table combined with Gold Dimensional Customer Mart
        </p>
    </div>
    """, unsafe_allow_html=True)

    df_silver = load_silver_table("customers")
    df_gold   = load_gold_mart("customer_order_summary")
    bronze    = load_bronze_events("customers")

    if df_silver.empty:
        st.warning("Silver customers table not found. Run `python silver_processor.py`.")
        return

    active = df_silver[df_silver["_is_deleted"] == False].copy() if "_is_deleted" in df_silver.columns else df_silver.copy()
    deleted = len(df_silver) - len(active)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Active Customers</div>
            <div class="stat-value">{len(active):,}</div>
            <div class="stat-subtext">👤 In Silver Delta Lake</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Soft-Deleted</div>
            <div class="stat-value" style="color:#f43f5e;">{deleted}</div>
            <div class="stat-subtext">🗑️ Preserved for audit</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Total CDC Events</div>
            <div class="stat-value">{len(bronze):,}</div>
            <div class="stat-subtext">📥 Processed row revisions</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        vip_count = len(df_gold[df_gold["customer_segment"] == "VIP"]) if not df_gold.empty and "customer_segment" in df_gold.columns else 0
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Gold VIP Customers</div>
            <div class="stat-value" style="color:#facc15;">{vip_count}</div>
            <div class="stat-subtext">⭐ Segmented via dbt mart</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["🥇 Gold Mart (Customer Segments)", "🥈 Silver Conformed Table", "📜 Full Event Audit Log"])

    with tab1:
        if not df_gold.empty:
            st.markdown('<div class="section-header-modern"><span>💎 Customer Order Intelligence (dbt Mart)</span></div>', unsafe_allow_html=True)
            st.dataframe(
                df_gold,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "customer_id": st.column_config.NumberColumn("Customer ID", format="%d"),
                    "customer_name": "Full Name",
                    "customer_email": "Email",
                    "total_orders": "Orders Placed",
                    "total_units_purchased": "Units Bought",
                    "customer_segment": st.column_config.TextColumn("Segment 🏷️"),
                    "favourite_product": "Top Purchased Item",
                }
            )
        else:
            st.info("Gold customer mart not built yet. Run `dbt run` or `python pipeline/run_pipeline.py`.")

    with tab2:
        st.markdown('<div class="section-header-modern"><span>🥈 Silver Layer Parquet Data (Delta Lake)</span></div>', unsafe_allow_html=True)
        display_cols = [c for c in active.columns if not c.startswith("__")]
        st.dataframe(active[display_cols], use_container_width=True, hide_index=True)

    with tab3:
        if not bronze.empty:
            st.markdown('<div class="section-header-modern"><span>📜 Historical Bronze Ingestion Feed</span></div>', unsafe_allow_html=True)
            hist = bronze.sort_values("timestamp_ms", ascending=False)
            st.dataframe(
                hist[["operation", "id", "event_time", "filename"]].rename(columns={
                    "operation": "Op", "id": "Customer ID", "event_time": "Time (UTC)", "filename": "Bronze Source File"
                }),
                use_container_width=True,
                hide_index=True
            )


# ── Page: Orders ───────────────────────────────────────────────────────────────

def page_orders():
    st.markdown("""
    <div style="margin-bottom: 20px;">
        <h1 style="margin:0; font-size: 2.1rem; font-weight: 800;">📦 Orders & Sales Velocity</h1>
        <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
            Real-time transaction log merged into Delta Lake and joined in dbt Gold Fact Marts
        </p>
    </div>
    """, unsafe_allow_html=True)

    df_orders = load_silver_table("orders")
    df_gold_orders = load_gold_mart("orders_with_customers")

    if df_orders.empty:
        st.warning("Silver orders table not found. Run `python silver_processor.py`.")
        return

    active_orders = df_orders[df_orders["_is_deleted"] == False].copy() if "_is_deleted" in df_orders.columns else df_orders.copy()
    total_units = int(pd.to_numeric(active_orders.get("quantity", pd.Series([0])), errors="coerce").sum())

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Total Orders</div>
            <div class="stat-value">{len(active_orders):,}</div>
            <div class="stat-subtext">📦 Deduplicated transactions</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Total Units Sold</div>
            <div class="stat-value" style="color:#38bdf8;">{total_units:,}</div>
            <div class="stat-subtext">📈 Cumulative sales volume</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        unique_dates = active_orders["order_date"].nunique() if "order_date" in active_orders.columns else 0
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Active Order Dates</div>
            <div class="stat-value">{unique_dates}</div>
            <div class="stat-subtext">📅 Daily transaction spread</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

    # Orders Velocity Chart
    if "order_date" in active_orders.columns and not active_orders.empty:
        st.markdown('<div class="section-header-modern"><span>📈 Sales Velocity Over Time</span><span style="font-size:0.8rem; color:#94a3b8;">Orders vs Units</span></div>', unsafe_allow_html=True)
        active_orders["order_date_dt"] = pd.to_datetime(active_orders["order_date"], errors="coerce")
        by_day = (
            active_orders.dropna(subset=["order_date_dt"])
            .groupby("order_date_dt")
            .agg(orders=("id", "count"), units=("quantity", "sum"))
            .reset_index()
        )
        by_day["units"] = pd.to_numeric(by_day["units"], errors="coerce")

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=by_day["order_date_dt"], y=by_day["orders"],
            name="Orders", marker_color="#0284c7"
        ))
        fig.add_trace(go.Scatter(
            x=by_day["order_date_dt"], y=by_day["units"],
            name="Units Sold", yaxis="y2",
            mode="lines+markers", line=dict(color="#34d399", width=3)
        ))
        fig.update_layout(
            yaxis=dict(title="# Orders", showgrid=True, gridcolor="rgba(255,255,255,0.06)"),
            yaxis2=dict(title="Units Sold", overlaying="y", side="right", showgrid=False),
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Plus Jakarta Sans", color="#94a3b8"),
            legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
            margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Details Tabs
    t1, t2 = st.tabs(["🥇 Enriched Gold Fact Table (dbt)", "🥈 Silver Raw Orders"])
    with t1:
        if not df_gold_orders.empty:
            st.dataframe(df_gold_orders, use_container_width=True, hide_index=True)
        else:
            st.info("Enriched Gold orders mart not built yet. Run `python pipeline/run_pipeline.py`.")
    with t2:
        st.dataframe(active_orders, use_container_width=True, hide_index=True)


# ── Page: Products ─────────────────────────────────────────────────────────────

def page_products():
    st.markdown("""
    <div style="margin-bottom: 20px;">
        <h1 style="margin:0; font-size: 2.1rem; font-weight: 800;">🏷️ Product Intelligence</h1>
        <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
            Product dimensions, Gold sales leaderboard, and weight logistics
        </p>
    </div>
    """, unsafe_allow_html=True)

    df_products = load_silver_table("products")
    df_gold_prod = load_gold_mart("product_sales_summary")

    if df_products.empty:
        st.warning("Silver products table not found. Run `python silver_processor.py`.")
        return

    active = df_products[df_products["_is_deleted"] == False].copy() if "_is_deleted" in df_products.columns else df_products.copy()
    avg_weight = pd.to_numeric(active.get("weight", pd.Series([0])), errors="coerce").mean()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Catalog Products</div>
            <div class="stat-value">{len(active):,}</div>
            <div class="stat-subtext">🏷️ Active SKUs</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Average Item Weight</div>
            <div class="stat-value" style="color:#c084fc;">{avg_weight:.2f} kg</div>
            <div class="stat-subtext">⚖️ Shipping logistics index</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        top_seller = df_gold_prod.iloc[0]["product_name"] if not df_gold_prod.empty and "product_name" in df_gold_prod.columns else "N/A"
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Top Selling SKU</div>
            <div class="stat-value" style="font-size: 1.4rem; color:#facc15;">{top_seller}</div>
            <div class="stat-subtext">🥇 Ranked #1 by dbt mart</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["🥇 Top Sellers Leaderboard (Gold)", "📋 Catalog & Weight Distribution"])

    with tab1:
        if not df_gold_prod.empty:
            st.markdown('<div class="section-header-modern"><span>🏆 Product Sales Leaderboard (dbt Mart)</span></div>', unsafe_allow_html=True)
            st.dataframe(
                df_gold_prod,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "sales_rank": st.column_config.NumberColumn("Rank 🏆", format="#%d"),
                    "product_name": "Product SKU",
                    "total_units_sold": "Units Sold",
                    "unique_customers": "Buyers Count",
                    "weight_kg": "Weight (kg)"
                }
            )
        else:
            st.info("Product sales summary mart not built yet. Run `python pipeline/run_pipeline.py`.")

    with tab2:
        c_left, c_right = st.columns([1.2, 1])
        with c_left:
            st.markdown('<div class="section-header-modern"><span>📋 Master Catalog (Silver Layer)</span></div>', unsafe_allow_html=True)
            st.dataframe(active, use_container_width=True, hide_index=True)
        with c_right:
            st.markdown('<div class="section-header-modern"><span>⚖️ Weight Distribution</span></div>', unsafe_allow_html=True)
            if "weight" in active.columns and "name" in active.columns:
                active["weight_num"] = pd.to_numeric(active["weight"], errors="coerce")
                fig_bar = px.bar(
                    active.dropna(subset=["weight_num"]).sort_values("weight_num", ascending=True),
                    x="weight_num", y="name", orientation="h",
                    color="weight_num",
                    color_continuous_scale="Purples",
                    template="plotly_dark",
                    labels={"weight_num": "Weight (kg)", "name": "Product"},
                )
                fig_bar.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="Plus Jakarta Sans", color="#94a3b8"),
                    margin=dict(l=0, r=0, t=10, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig_bar, use_container_width=True)


# ── Page: Data Quality ─────────────────────────────────────────────────────────

def page_data_quality():
    st.markdown("""
    <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-bottom: 20px;">
        <div>
            <h1 style="margin:0; font-size: 2.1rem; font-weight: 800;">✅ Automated Quality Gate</h1>
            <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
                Great Expectations assertion checks on Silver Delta tables. Pipeline halts on any contract violation.
            </p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    gq = load_gq_results()
    if not gq:
        st.info("No GX quality results found. Run `python data_quality/run_checks.py`.")
        return

    ts = gq.get("run_timestamp", "")[:19].replace("T", " ")
    all_pass = all(v.get("failed", 0) == 0 for v in gq.get("tables", {}).values())

    total_checks = sum(v.get("passed", 0) + v.get("failed", 0) for v in gq.get("tables", {}).values())
    total_passed = sum(v.get("passed", 0) for v in gq.get("tables", {}).values())
    total_failed = sum(v.get("failed", 0) for v in gq.get("tables", {}).values())

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Total Assertions</div>
            <div class="stat-value">{total_checks}</div>
            <div class="stat-subtext">🔍 Column & row contract rules</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Passed Checks</div>
            <div class="stat-value" style="color:#10b981;">{total_passed}</div>
            <div class="stat-subtext">🛡️ Clean conformed data</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        failed_color = "#f43f5e" if total_failed > 0 else "#10b981"
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Violations Detected</div>
            <div class="stat-value" style="color:{failed_color};">{total_failed}</div>
            <div class="stat-subtext">{"⚠️ Gate halted" if total_failed > 0 else "✓ Zero contract breaches"}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

    # Detailed Table Cards
    for table_name, result in gq.get("tables", {}).items():
        passed_ct = result.get("passed", 0)
        failed_ct = result.get("failed", 0)
        tbl_pass  = failed_ct == 0
        accent    = "#10b981" if tbl_pass else "#f43f5e"
        badge_html = f'<span style="background:rgba(16,185,129,0.2); color:#34d399; padding:4px 12px; border-radius:9999px; font-size:0.75rem; font-weight:700;">✓ 100% PASS</span>' if tbl_pass else f'<span style="background:rgba(244,63,94,0.2); color:#fb7185; padding:4px 12px; border-radius:9999px; font-size:0.75rem; font-weight:700;">✗ {failed_ct} FAIL</span>'

        st.markdown(f"""
        <div style="background: rgba(30,41,59,0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 14px; border-left: 4px solid {accent}; padding: 18px 22px; margin-bottom: 16px;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 12px;">
                <div style="display:flex; align-items:center; gap:10px;">
                    <span style="font-size:1.15rem; font-weight:700; color:#ffffff;">{table_name.capitalize()} Expectations Suite</span>
                    <span style="color:#64748b; font-size:0.85rem;">({passed_ct} / {passed_ct + failed_ct} checks passed)</span>
                </div>
                {badge_html}
            </div>
        """, unsafe_allow_html=True)

        for chk in result.get("checks", []):
            passed = chk.get("passed", False)
            icon = "✓" if passed else "✗"
            chk_color = "#34d399" if passed else "#fb7185"
            bg_badge = "rgba(16,185,129,0.15)" if passed else "rgba(244,63,94,0.15)"
            detail = chk.get("detail", "")
            st.markdown(f"""
            <div style="display:flex; justify-content:space-between; align-items:center; padding: 8px 12px; border-radius:8px; margin-bottom: 4px; background: rgba(15,23,42,0.4);">
                <div style="color: #cbd5e1; font-size: 0.9rem; font-weight: 500;">
                    <span style="color:{chk_color}; font-weight:800; margin-right:8px;">{icon}</span> {chk["name"]}
                </div>
                <div style="color: #64748b; font-size: 0.8rem; font-family:'JetBrains Mono';">
                    {detail}
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)


# ── Page: Time Travel ──────────────────────────────────────────────────────────

def page_time_travel():
    st.markdown("""
    <div style="margin-bottom: 20px;">
        <h1 style="margin:0; font-size: 2.1rem; font-weight: 800;">🔍 Delta Lake Time Travel</h1>
        <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
            Query historical table states AS OF any transaction version or timestamp via ACID commit logs
        </p>
    </div>
    """, unsafe_allow_html=True)

    c_left, c_right = st.columns([1, 2])

    with c_left:
        selected_table = st.selectbox("Select Target Table", ["customers", "orders", "products"])
        versions = load_delta_versions(selected_table)

        if not versions:
            st.warning(f"No Delta transaction history found for `{selected_table}`.")
            return

        version_options = {
            f"Version {v.get('version', i)} — {str(v.get('timestamp', ''))[:19]}": v.get('version', i)
            for i, v in enumerate(reversed(versions))
        }
        selected_label = st.selectbox("Time Travel Snapshot Version", list(version_options.keys()))
        selected_version = version_options[selected_label]

        st.markdown(f"""
        <div style="background: rgba(30,41,59,0.5); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px; padding: 16px; margin-top: 14px;">
            <div style="font-size:0.75rem; text-transform:uppercase; color:#94a3b8; font-weight:700;">Python Query API</div>
            <pre style="margin-top:8px; padding:10px; background:rgba(15,23,42,0.8); border-radius:8px; color:#38bdf8; font-size:0.82rem; overflow-x:auto;">
from deltalake import DeltaTable

dt = DeltaTable("silver/{selected_table}",
                version={selected_version})
df = dt.to_pandas()</pre>
        </div>
        """, unsafe_allow_html=True)

    with c_right:
        st.markdown(f'<div class="section-header-modern"><span>📋 Table Snapshot AS OF Version {selected_version}</span></div>', unsafe_allow_html=True)
        hist_df = load_silver_as_of_version(selected_table, selected_version)
        if "error" in hist_df.columns:
            st.error(hist_df["error"].iloc[0])
        else:
            st.caption(f"Snapshot contains **{len(hist_df)}** records at this specific commit point.")
            display_cols = [c for c in hist_df.columns if not c.startswith("__")]
            st.dataframe(hist_df[display_cols], use_container_width=True, hide_index=True)

    st.markdown('<div class="section-header-modern"><span>📜 Delta Lake ACID Transaction Log (`_delta_log/`)</span></div>', unsafe_allow_html=True)
    if versions:
        log_records = []
        for v in versions:
            log_records.append({
                "Version": v.get("version", ""),
                "Commit Timestamp (UTC)": str(v.get("timestamp", ""))[:19],
                "Delta Operation": v.get("operation", ""),
                "Engine Client": v.get("clientVersion", ""),
            })
        st.dataframe(pd.DataFrame(log_records), use_container_width=True, hide_index=True)


# ── Main Entrypoint ────────────────────────────────────────────────────────────

def main():
    selected_page = render_sidebar()

    if selected_page.startswith("🏠"):
        page_overview()
    elif selected_page.startswith("📡"):
        page_cdc_feed()
    elif selected_page.startswith("👥"):
        page_customers()
    elif selected_page.startswith("📦"):
        page_orders()
    elif selected_page.startswith("🏷️"):
        page_products()
    elif selected_page.startswith("✅"):
        page_data_quality()
    elif selected_page.startswith("🔍"):
        page_time_travel()


if __name__ == "__main__":
    main()
