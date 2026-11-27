"""Streamlit dashboard for CDC activity, lakehouse data, and validation."""

import json
import sys
from html import escape
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Path setup ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
SILVER_DIR   = PROJECT_ROOT / "silver"
GOLD_DB      = PROJECT_ROOT / "gold" / "cdc_lakehouse.duckdb"
GQ_RESULTS   = PROJECT_ROOT / "data_quality" / "runtime_results" / "gx_results_latest.json"

sys.path.insert(0, str(PROJECT_ROOT))
from silver_processor import TABLES, bronze_source, load_bronze_events as read_bronze_events

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CDC Lakehouse | Real-Time Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="auto",
)

st.markdown(f"<style>{Path(__file__).with_name('styles.css').read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


# ── Data Loaders & Caching ─────────────────────────────────────────────────────

@st.cache_data(ttl=20)
def load_bronze_events(table: str, source: str) -> pd.DataFrame:
    rows = []
    for event in read_bronze_events(table, source):
        row_data = event.get("after") or event.get("before") or {}
        rows.append({
            "table": table,
            "operation": event.get("operation", "?"),
            "timestamp_ms": event.get("timestamp_ms", 0),
            "partition": event.get("partition"),
            "offset": event.get("offset"),
            "ingested_at": event.get("ingested_at", ""),
            "id": row_data.get("id"),
            "before": event.get("before"),
            "after": event.get("after"),
            "location": event["location"],
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["event_time"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    return df


@st.cache_data(ttl=20)
def load_all_bronze_events(source: str) -> pd.DataFrame:
    frames = [load_bronze_events(table, source) for table in TABLES]
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


OPERATIONS = {
    "c": ("Insert", "#70e4c2"),
    "u": ("Update", "#efb86a"),
    "d": ("Delete", "#ff8d99"),
    "r": ("Snapshot", "#a9a4fa"),
}
OTHER_OPERATION = ("Other", "#98abc1")


def format_utc(value, fmt="%d %b %Y · %H:%M UTC"):
    unit = "ms" if isinstance(value, (int, float)) else None
    timestamp = pd.to_datetime(value, unit=unit, utc=True, errors="coerce")
    return timestamp.strftime(fmt) if pd.notna(timestamp) else "—"


def record_id(value):
    return "—" if pd.isna(value) else str(value).removesuffix(".0")


def page_heading(title, description):
    st.markdown(f"""
<div class="page-heading"><h1>{title}</h1><p>{description}</p></div>
""", unsafe_allow_html=True)


def panel_heading(title, description):
    st.markdown(f"""
<div class="panel-heading"><h2>{title}</h2><p>{description}</p></div>
""", unsafe_allow_html=True)


def stat_card_markup(title, value, detail, status=""):
    return f"""
<div class="stat-card">
    <div class="stat-title">{title}</div>
    <div class="stat-value {status}">{value}</div>
    <div class="stat-subtext">{detail}</div>
</div>
"""


def recent_first(events):
    return events.sort_values(["timestamp_ms", "partition", "offset"], ascending=False, kind="stable")


def render_sidebar():
    with st.sidebar:
        st.markdown("""
<div class="site-brand"><span class="site-mark" aria-hidden="true">≈</span><strong>CDC Lakehouse</strong></div>
""", unsafe_allow_html=True)
        page = st.radio(
            "Pages",
            ["Overview", "CDC Feed", "Customers", "Orders", "Products", "Data Quality", "Time Travel"],
        )
        source_label = "Spark Delta" if bronze_source() == "spark" else "Kafka JSON"
        st.markdown(f"""
<div class="sidebar-status">
    <span>Bronze source</span>
    <strong>{source_label}</strong>
</div>
""", unsafe_allow_html=True)
        if st.button("Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    return page


def page_overview():
    source = bronze_source()
    events = load_all_bronze_events(source)
    latest_capture = pd.to_datetime(events.get("ingested_at", pd.Series(dtype=str)), utc=True, errors="coerce", format="mixed").max()
    st.markdown(f"""
<div class="overview-hero">
    <div>
        <h1>CDC Lakehouse</h1>
        <p>Follow PostgreSQL changes from Kafka to Delta Lake and dbt.</p>
    </div>
    <div class="hero-freshness">
        <span>Latest Bronze capture</span>
        <strong>{format_utc(latest_capture)}</strong>
    </div>
</div>
""", unsafe_allow_html=True)

    audits = load_gq_results().get("tables", {})
    failed = sum(table.get("failed", 0) for table in audits.values())
    passed = sum(table.get("passed", 0) for table in audits.values())
    if passed + failed:
        quality_value = "Blocked" if failed else "Passed"
        quality_detail = f"{passed}/{passed + failed} checks passed"
        quality_status = "is-fail" if failed else "is-pass"
    else:
        quality_value, quality_detail, quality_status = "Pending", "No completed audit", ""

    metrics = [
        ("Bronze events", f"{len(events):,}", "Captured CDC records", ""),
        ("Active customers", f"{active_row_count(load_silver_table('customers')):,}", "Silver Delta", ""),
        ("Active orders", f"{active_row_count(load_silver_table('orders')):,}", "Silver Delta", ""),
        ("Quality gate", quality_value, quality_detail, quality_status),
    ]
    cards = "".join(stat_card_markup(*metric) for metric in metrics)
    st.markdown(f'<div class="overview-metrics">{cards}</div>', unsafe_allow_html=True)

    chart_column, recent_column = st.columns([1.35, 1])
    with chart_column, st.container(border=True):
        panel_heading("CDC activity", "Captured events by table and operation")
        if events.empty:
            st.info("Waiting for Bronze events.")
        else:
            render_activity_chart(events)

    with recent_column, st.container(border=True):
        panel_heading("Recent changes", "Most recent CDC events · UTC")
        if events.empty:
            st.info("New events will appear here as they are captured.")
        else:
            rows = []
            for event in recent_first(events).head(5).itertuples():
                op, _ = OPERATIONS.get(event.operation, OTHER_OPERATION)
                style = event.operation if event.operation in OPERATIONS else "other"
                rows.append(f"""
<div class="recent-event">
    <span class="pill-op pill-{style}">{op}</span>
    <strong>{escape(event.table.title())} · {escape(record_id(event.id))}</strong>
    <time>{format_utc(event.event_time, "%H:%M:%S")}</time>
</div>""")
            st.markdown("".join(rows), unsafe_allow_html=True)

    render_pipeline_flow(source)


def active_row_count(frame):
    if "_is_deleted" not in frame.columns:
        return len(frame)
    return int(frame["_is_deleted"].eq(False).sum())


def render_activity_chart(events):
    chart = go.Figure()
    names = [table.title() for table in TABLES]
    operations = events["operation"].where(events["operation"].isin(OPERATIONS), "other")
    counts = events.groupby(["table", operations.rename("op")]).size()
    for op, (label, color) in {**OPERATIONS, "other": OTHER_OPERATION}.items():
        values = [int(counts.get((table, op), 0)) for table in TABLES]
        if any(values):
            chart.add_bar(
                x=values, y=names, name=label, orientation="h",
                marker_color=color, hovertemplate="%{y}: %{x} events<extra>%{fullData.name}</extra>",
            )
    chart.update_layout(
        barmode="stack", height=218, bargap=.48,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#b2c4d6", "size": 12},
        margin={"l": 0, "r": 8, "t": 8, "b": 5},
        xaxis={"gridcolor": "#2b3b4e", "zeroline": False, "rangemode": "tozero", "dtick": 1 if counts.max() < 5 else None},
        yaxis={"autorange": "reversed", "showgrid": False, "automargin": True,
               "ticks": "outside", "ticklen": 7, "tickcolor": "#101721"},
        legend={"orientation": "h", "y": -0.2, "x": 0, "font": {"size": 11}, "traceorder": "normal"},
    )
    st.plotly_chart(chart, use_container_width=True, theme=None, config={"displayModeBar": False})


def render_pipeline_flow(source):
    st.subheader("Pipeline flow")
    steps = [
        ("PostgreSQL", "Write-ahead log", "#81b8f5"),
        ("Debezium → Kafka", "Change capture", "#a9a4fa"),
        ("Bronze", "Spark Structured Streaming" if source == "spark" else "Kafka JSON consumer", "#efb86a"),
        ("Silver", "Airflow · Delta upserts", "#a9bccd"),
        ("Quality gate", "Great Expectations", "#70e4c2"),
        ("Gold", "dbt · DuckDB marts", "#f2d789"),
    ]
    cards = "".join(
        f'<div class="pipeline-step" style="--step:{color}"><strong>{name}</strong><span>{detail}</span></div>'
        for name, detail, color in steps
    )
    st.markdown(f'<div class="pipeline-flow">{cards}</div>', unsafe_allow_html=True)


def page_cdc_feed():
    page_heading("CDC Feed", "Filter captured events and inspect the row before and after a change.")
    events = load_all_bronze_events(bronze_source())
    if events.empty:
        st.info("The selected Bronze source has no events yet.")
        return

    table_filter, op_filter, search_filter = st.columns([1, 1, 1.1])
    with table_filter:
        tables = st.multiselect("Tables", TABLES, format_func=str.title, placeholder="All tables")
    with op_filter:
        operation_labels = {code: label for code, (label, _) in OPERATIONS.items()}
        selected_ops = st.multiselect(
            "Operations", list(operation_labels), format_func=operation_labels.get,
            placeholder="All operations",
        )
    with search_filter:
        search = st.text_input("Search record or payload", placeholder="Record ID or field value").strip()

    filtered = events.loc[
        events["table"].isin(tables or TABLES) & events["operation"].isin(selected_ops or operation_labels)
    ]
    if search:
        matches = pd.Series(False, index=filtered.index)
        for field in ("id", "before", "after"):
            matches |= filtered[field].astype(str).str.contains(search, case=False, regex=False)
        filtered = filtered.loc[matches]

    filtered = recent_first(filtered).reset_index(drop=True)
    if filtered.empty:
        st.info("No events match the selected filters.")
        return

    list_column, inspector_column = st.columns([1.05, 1])
    with list_column, st.container(border=True):
        panel_heading("Matching events", f"{len(filtered):,} events · newest first")
        preview = pd.DataFrame({
            "Operation": filtered["operation"].map(operation_labels),
            "Table": filtered["table"].str.title(),
            "Record": filtered["id"].map(record_id),
            "Time (UTC)": filtered["event_time"].dt.strftime("%d %b %H:%M"),
        })
        st.dataframe(preview, hide_index=True, use_container_width=True, height=405)

    with inspector_column, st.container(border=True):
        panel_heading("Event inspector", "Choose an event to see the captured payload.")
        labels = [
            f"{operation_labels[row.operation]} · {row.table.title()} #{record_id(row.id)} · {format_utc(row.event_time, '%H:%M:%S')}"
            for row in filtered.itertuples()
        ]
        selected = st.selectbox("Event", range(len(filtered)), format_func=labels.__getitem__)
        event = filtered.iloc[selected]
        st.markdown(f'<div class="record-location">{escape(str(event["location"]))}</div>', unsafe_allow_html=True)
        before_tab, after_tab = st.tabs(["Before", "After"])
        with before_tab:
            render_event_state(event["before"], "No previous row in this event.")
        with after_tab:
            render_event_state(event["after"], "No resulting row in this event.")


def render_event_state(payload, empty_text):
    if payload is None:
        st.caption(empty_text)
    else:
        st.json(payload)


# ── Page: Customers ────────────────────────────────────────────────────────────

def page_customers():
    page_heading("Customers", "Active customer records in Silver and order segments from dbt.")

    df_silver = load_silver_table("customers")
    df_gold   = load_gold_mart("customer_order_summary")
    bronze    = load_bronze_events("customers", bronze_source())

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
                hist[["operation", "id", "event_time", "location"]].rename(columns={
                    "operation": "Op", "id": "Customer ID", "event_time": "Time (UTC)", "location": "Bronze Record"
                }),
                use_container_width=True,
                hide_index=True
            )


# ── Page: Orders ───────────────────────────────────────────────────────────────

def page_orders():
    page_heading("Orders", "Daily order volume, units sold, and enriched order records.")

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
            font=dict(family="sans-serif", color="#94a3b8"),
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
    page_heading("Products", "Catalog coverage, item weights, and product sales rankings.")

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
            <div class="stat-value" style="font-size: 1.4rem; color:#facc15;">{escape(str(top_seller))}</div>
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
                    font=dict(family="sans-serif", color="#94a3b8"),
                    margin=dict(l=0, r=0, t=10, b=0),
                    showlegend=False,
                )
                st.plotly_chart(fig_bar, use_container_width=True)


# ── Page: Data Quality ─────────────────────────────────────────────────────────

def page_data_quality():
    page_heading("Data Quality", "Great Expectations checks the Silver tables before dbt runs.")
    audit = load_gq_results()
    results = audit.get("tables", {})
    if not results:
        st.info("No quality audit has completed yet.")
        return

    passed = sum(result.get("passed", 0) for result in results.values())
    failed = sum(result.get("failed", 0) for result in results.values())
    if failed:
        gate = ("Quality gate", "Blocked", f"{failed:,} checks failed", "is-fail")
    else:
        gate = ("Quality gate", "Passed", "No violations detected", "is-pass")
    metrics = [
        gate,
        ("Checks passed", f"{passed:,} / {passed + failed:,}", f"Across {len(results)} Silver tables"),
        ("Last audit", format_utc(audit.get("run_timestamp"), "%H:%M UTC"), format_utc(audit.get("run_timestamp"), "%d %b %Y")),
    ]
    cards = "".join(stat_card_markup(*metric) for metric in metrics)
    st.markdown(f'<div class="overview-metrics snapshot-metrics">{cards}</div>', unsafe_allow_html=True)

    ordered_results = sorted(results, key=lambda table: (-results[table].get("failed", 0), table))
    with st.container(border=True):
        panel_heading("Check results", "Select a Silver table to review its assertions.")
        for tab, table in zip(st.tabs([table.title() for table in ordered_results]), ordered_results, strict=True):
            with tab:
                checks = sorted(results[table].get("checks", []), key=lambda check: bool(check.get("passed")))
                rows = [
                    {
                        "Check": check["name"],
                        "Result": "Passed" if check.get("passed") else "Failed",
                        "Detail": check.get("detail", ""),
                    }
                    for check in checks
                ]
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                else:
                    st.info("This table has no recorded assertions.")


# ── Page: Time Travel ──────────────────────────────────────────────────────────

def page_time_travel():
    page_heading("Time Travel", "Inspect older Silver snapshots and review the Delta commit history.")
    table_column, version_column = st.columns(2)
    with table_column:
        selected_table = st.selectbox("Silver table", TABLES, format_func=str.title)
    versions = sorted(load_delta_versions(selected_table), key=lambda entry: entry["version"], reverse=True)
    if not versions:
        st.info(f"No Delta commit history is available for {selected_table}.")
        return

    version_numbers = [entry["version"] for entry in versions]
    timestamps = {entry["version"]: format_utc(entry.get("timestamp")) for entry in versions}
    with version_column:
        selected_version = st.selectbox(
            "Snapshot version", version_numbers,
            index=1 if len(version_numbers) > 1 else 0,
            format_func=lambda version: f"v{version} · {timestamps[version]}",
            key=f"snapshot-{selected_table}",
        )

    snapshot = load_silver_as_of_version(selected_table, selected_version)
    if "error" in snapshot.columns:
        st.error(str(snapshot["error"].iloc[0]))
        return

    metrics = [
        ("Selected version", f"v{selected_version}", timestamps[selected_version]),
        ("Latest version", f"v{version_numbers[0]}", timestamps[version_numbers[0]]),
        ("Snapshot rows", f"{len(snapshot):,}", f"{active_row_count(snapshot):,} active"),
    ]
    cards = "".join(stat_card_markup(*metric) for metric in metrics)
    st.markdown(f'<div class="overview-metrics snapshot-metrics">{cards}</div>', unsafe_allow_html=True)

    snapshot_tab, history_tab = st.tabs(["Snapshot rows", "Commit history"])
    with snapshot_tab:
        st.dataframe(snapshot, use_container_width=True, hide_index=True)
        with st.expander("Python query"):
            st.code(
                f'from deltalake import DeltaTable\n\n'
                f'rows = DeltaTable("silver/{selected_table}", version={selected_version}).to_pandas()',
                language="python",
            )
    with history_tab:
        history = [
            {
                "Version": entry["version"],
                "Committed (UTC)": timestamps[entry["version"]],
                "Operation": entry.get("operation", ""),
                "Writer": entry.get("clientVersion", ""),
            }
            for entry in versions
        ]
        st.dataframe(pd.DataFrame(history), use_container_width=True, hide_index=True)


# ── Main Entrypoint ────────────────────────────────────────────────────────────

def main():
    pages = {
        "Overview": page_overview,
        "CDC Feed": page_cdc_feed,
        "Customers": page_customers,
        "Orders": page_orders,
        "Products": page_products,
        "Data Quality": page_data_quality,
        "Time Travel": page_time_travel,
    }
    pages[render_sidebar()]()


if __name__ == "__main__":
    main()
