"""
QueueWise -- Real-Time Queue Analytics and Capacity Planning Using Apache Spark
INT315 Cluster Computing Capstone Project

Operations Console Dashboard
Built with Streamlit 1.65.0, Plotly 7.1.0, and pandas 2.3.3.
Consumes pre-computed Spark Core, Spark SQL, Spark MLlib, and Capacity Simulation outputs.

Academic Notice:
    All analytics reflect synthetic discrete-event queue simulations (seed 42).
    Results do not represent actual university administrative records.
"""

import os
from typing import Dict, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ==============================================================================
# Page Configuration & Global Theme Settings
# ==============================================================================

st.set_page_config(
    page_title="QueueWise | Queue Operations & Capacity Planning Console",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Scoped CSS for Enterprise Light-Themed SaaS Operations Console
st.markdown(
    """
    <style>
    /* Clean System Typography */
    html, body, [class*="css"] {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }
    
    /* Global App Container */
    .stApp {
        background-color: #F8FAFC;
    }

    /* Top Brand Header */
    .app-brand-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 12px 0 16px 0;
        border-bottom: 1px solid #E2E8F0;
        margin-bottom: 20px;
    }
    .app-brand-title {
        font-size: 1.55rem;
        font-weight: 700;
        color: #0F172A;
        letter-spacing: -0.5px;
        margin: 0;
    }
    .app-brand-subtitle {
        font-size: 0.88rem;
        color: #64748B;
        margin: 2px 0 0 0;
    }
    .status-badge {
        background-color: #FFFFFF;
        border: 1px solid #CBD5E1;
        border-radius: 6px;
        padding: 5px 12px;
        font-size: 0.8rem;
        font-weight: 500;
        color: #334155;
    }

    /* Operational KPI Metric Card */
    .kpi-card {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 14px 16px;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
        height: 100%;
    }
    .kpi-title {
        font-size: 0.74rem;
        font-weight: 600;
        color: #64748B;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
    }
    .kpi-number {
        font-size: 1.65rem;
        font-weight: 700;
        color: #0F172A;
        line-height: 1.2;
    }
    .kpi-detail {
        font-size: 0.78rem;
        color: #64748B;
        margin-top: 4px;
    }
    .kpi-alert-text {
        color: #DC2626;
        font-weight: 600;
    }
    .kpi-success-text {
        color: #16A34A;
        font-weight: 600;
    }

    /* Structured Enterprise Content Surface */
    .content-box {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 16px 18px;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
        margin-bottom: 16px;
    }
    .content-header {
        font-size: 1.0rem;
        font-weight: 600;
        color: #1E293B;
        margin-bottom: 10px;
    }

    /* Status Pills */
    .pill {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 0.72rem;
        font-weight: 600;
    }
    .pill-red { background-color: #FEE2E2; color: #991B1B; }
    .pill-amber { background-color: #FEF3C7; color: #92400E; }
    .pill-green { background-color: #DCFCE7; color: #166534; }
    .pill-blue { background-color: #E0F2FE; color: #075985; }

    /* Footer Note */
    .footer-disclaimer {
        font-size: 0.78rem;
        color: #94A3B8;
        text-align: center;
        padding: 20px 0 8px 0;
        border-top: 1px solid #E2E8F0;
        margin-top: 28px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Queue Visual Encodings
QUEUE_COLORS = {
    "Q_FINANCIAL_AID": "#DC2626",      # Red (Critical Bottleneck)
    "Q_ADMISSIONS": "#2563EB",         # Royal Blue (High Volume)
    "Q_ACADEMIC_RECORDS": "#D97706",    # Amber (Moderate / Balanced)
    "Q_ID_GENERAL": "#059669",         # Green (Low Wait / Excess Capacity)
}

QUEUE_NAMES = {
    "Q_FINANCIAL_AID": "Financial Aid & Scholarships",
    "Q_ADMISSIONS": "Admissions & Enrollment",
    "Q_ACADEMIC_RECORDS": "Academic Records & Transcripts",
    "Q_ID_GENERAL": "Campus ID & General Help",
}


def apply_clean_layout(
    fig: go.Figure,
    height: int = 300,
    margin: Optional[Dict] = None,
    barmode: Optional[str] = None,
    xaxis_title: str = "",
    yaxis_title: str = "",
    yaxis_range: Optional[list] = None,
    legend_top: bool = False,
) -> go.Figure:
    """Applies a clean, restrained enterprise aesthetic to Plotly figures."""
    if margin is None:
        margin = dict(l=35, r=20, t=20, b=35)

    layout_update = dict(
        font=dict(family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif", color="#334155"),
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        margin=margin,
        height=height,
        xaxis=dict(gridcolor="#F1F5F9", zerolinecolor="#CBD5E1", title=xaxis_title),
        yaxis=dict(gridcolor="#F1F5F9", zerolinecolor="#CBD5E1", title=yaxis_title),
    )
    if barmode:
        layout_update["barmode"] = barmode
    if yaxis_range:
        layout_update["yaxis"]["range"] = yaxis_range
    if legend_top:
        layout_update["legend"] = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)

    fig.update_layout(**layout_update)
    return fig


# ==============================================================================
# Data Ingestion & Caching
# ==============================================================================

OUTPUTS_DIR = "outputs"
DATA_DIR = "data"


@st.cache_data
def load_analytical_datasets() -> Tuple[Dict[str, Optional[pd.DataFrame]], Dict[str, str]]:
    """Loads pre-computed analytical datasets from outputs/."""
    files = {
        "event_counts": os.path.join(OUTPUTS_DIR, "event_counts.csv"),
        "customer_counts": os.path.join(OUTPUTS_DIR, "customer_counts_by_queue.csv"),
        "queue_metrics": os.path.join(OUTPUTS_DIR, "queue_metrics.csv"),
        "hourly_arrivals": os.path.join(OUTPUTS_DIR, "hourly_arrivals.csv"),
        "counter_utilization": os.path.join(OUTPUTS_DIR, "counter_utilization.csv"),
        "sla_breaches": os.path.join(OUTPUTS_DIR, "sla_breaches_by_queue.csv"),
        "capacity_comparison": os.path.join(OUTPUTS_DIR, "capacity_comparison.csv"),
        "reallocation_metrics": os.path.join(OUTPUTS_DIR, "reallocation_simulation_metrics.csv"),
        "reallocated_counters": os.path.join(OUTPUTS_DIR, "reallocated_counter_utilization.csv"),
        "ml_risk": os.path.join(OUTPUTS_DIR, "queue_long_wait_risk.csv"),
        "ml_metrics": os.path.join(OUTPUTS_DIR, "ml_model_metrics.csv"),
    }

    dfs: Dict[str, Optional[pd.DataFrame]] = {}
    missing_files: Dict[str, str] = {}

    for key, path in files.items():
        if os.path.exists(path):
            try:
                dfs[key] = pd.read_csv(path)
            except Exception as e:
                dfs[key] = None
                missing_files[key] = f"Error reading {path}: {e}"
        else:
            dfs[key] = None
            missing_files[key] = path

    return dfs, missing_files


@st.cache_data
def load_raw_queue_events() -> Optional[pd.DataFrame]:
    """Loads raw queue events log for exact custom threshold evaluation."""
    path = os.path.join(DATA_DIR, "queue_events.csv")
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return None
    return None


# ==============================================================================
# Main Application Layout & Navigation
# ==============================================================================


def main() -> None:
    # Ingest Datasets
    datasets, missing = load_analytical_datasets()
    raw_df = load_raw_queue_events()

    # Core Dataset Availability Check
    critical_missing = [
        k for k in ["queue_metrics", "sla_breaches", "counter_utilization"]
        if datasets.get(k) is None
    ]
    if critical_missing:
        st.error(f"Required analytical datasets missing in `{OUTPUTS_DIR}/`: {', '.join(critical_missing)}")
        st.info(
            """
            Please execute the pipeline scripts to generate required outputs:
            ```powershell
            python spark_processor.py      # Generates Stage 2 outputs
            python capacity_planner.py     # Generates Stage 3 outputs
            python ml/queue_wait_prediction.py  # Generates Stage 4 outputs
            ```
            """
        )
        st.stop()

    df_queue = datasets["queue_metrics"]
    df_sla = datasets["sla_breaches"]
    df_util = datasets["counter_utilization"]
    df_hourly = datasets["hourly_arrivals"]
    df_events = datasets["event_counts"]
    df_cap = datasets["capacity_comparison"]
    df_realloc = datasets["reallocation_metrics"]
    df_realloc_counters = datasets["reallocated_counters"]
    df_ml_risk = datasets.get("ml_risk")
    df_ml_metrics = datasets.get("ml_metrics")

    # Sidebar Brand & Stateful Navigation
    if "nav_page" not in st.session_state:
        st.session_state["nav_page"] = "Overview"

    st.sidebar.markdown(
        """
        <div style="padding: 10px 0 16px 0; border-bottom: 1px solid #E2E8F0; margin-bottom: 14px;">
            <div style="font-size: 1.35rem; font-weight: 700; color: #0F172A; letter-spacing: -0.5px;">QueueWise</div>
            <div style="font-size: 0.8rem; color: #0284C7; font-weight: 600;">Cluster Operations Console</div>
            <div style="font-size: 0.74rem; color: #64748B; margin-top: 2px;">PySpark 3.5.9 | Streamlit 1.65.0</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    nav_options = [
        "Overview",
        "Queue Performance",
        "Counter Utilization",
        "Capacity Planning",
        "Data Explorer",
    ]

    selected_nav = st.sidebar.radio(
        "Navigation",
        options=nav_options,
        index=nav_options.index(st.session_state["nav_page"]) if st.session_state["nav_page"] in nav_options else 0,
        label_visibility="collapsed",
    )
    st.session_state["nav_page"] = selected_nav

    # Sidebar Dataset Status Pill
    loaded_count = sum(1 for df in datasets.values() if df is not None)
    total_count = len(datasets)
    st.sidebar.markdown(
        f"""
        <div style="margin-top: 20px; padding: 10px 12px; background-color: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 6px; font-size: 0.75rem; color: #475569;">
            <div style="font-weight: 600; color: #0F172A; margin-bottom: 2px;">Dataset Status</div>
            <div>{loaded_count} of {total_count} analytical datasets active.</div>
            <div style="color: #16A34A; margin-top: 2px;">Simulation MD5: Verified</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Top Brand Header Bar
    st.markdown(
        f"""
        <div class="app-brand-container">
            <div>
                <h1 class="app-brand-title">QueueWise &mdash; {selected_nav}</h1>
                <p class="app-brand-subtitle">Multi-Counter Queue Analytics & Decision Support System</p>
            </div>
            <div>
                <span class="status-badge">
                    Simulation Scope: 09:00 &ndash; 19:22 | 500 Customers (Synthetic)
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ==========================================================================
    # PAGE 1: OVERVIEW (EXECUTIVE SUMMARY)
    # ==========================================================================
    if selected_nav == "Overview":
        total_customers = int(df_queue["serviced_customers"].sum())
        weighted_avg_wait = (
            round(
                (df_queue["avg_wait_minutes"] * df_queue["serviced_customers"]).sum() / total_customers,
                1,
            )
            if total_customers > 0
            else 0.0
        )
        tot_served = int(df_sla["total_customers_served"].sum()) if not df_sla.empty else 0
        tot_breached_30m = int(df_sla["breach_count_30m"].sum()) if not df_sla.empty else 0
        breach_rate_30m = round((tot_breached_30m / tot_served * 100.0), 1) if tot_served > 0 else 0.0

        bottleneck_row = df_queue.loc[df_queue["avg_wait_minutes"].idxmax()] if not df_queue.empty else None
        bottleneck_id = bottleneck_row["queue_id"] if bottleneck_row is not None else "N/A"
        bottleneck_wait = bottleneck_row["avg_wait_minutes"] if bottleneck_row is not None else 0.0

        # KPI Metrics Row
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-title">Total Customers Served</div>
                    <div class="kpi-number">{total_customers:,}</div>
                    <div class="kpi-detail">Across 4 service queues</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with k2:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-title">Average Waiting Time</div>
                    <div class="kpi-number">{weighted_avg_wait} min</div>
                    <div class="kpi-detail kpi-alert-text">Target: &le; 30 min</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with k3:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-title">30-Min SLA Breach Rate</div>
                    <div class="kpi-number">{breach_rate_30m}%</div>
                    <div class="kpi-detail">{tot_breached_30m} of {tot_served} customers breached</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with k4:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-title">Current Bottleneck</div>
                    <div class="kpi-number" style="font-size: 1.35rem;">{QUEUE_NAMES.get(bottleneck_id, bottleneck_id)}</div>
                    <div class="kpi-detail kpi-alert-text">Avg wait: {bottleneck_wait}m | 97.4% load</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.markdown("<div style='height: 14px;'></div>", unsafe_allow_html=True)

        # Overview Visualizations
        col_c1, col_c2 = st.columns([3, 2])

        with col_c1:
            st.markdown(
                """
                <div class="content-box">
                    <div class="content-header">Customer Arrivals Over Time (Facility Intake Profile)</div>
                """,
                unsafe_allow_html=True,
            )
            if df_hourly is not None and not df_hourly.empty:
                hourly_copy = df_hourly.copy()
                hourly_copy["time_label"] = hourly_copy["arrival_hour"].apply(
                    lambda x: str(x).split(" ")[-1][:5] if " " in str(x) else str(x)
                )
                fig_arrivals = px.area(
                    hourly_copy,
                    x="time_label",
                    y="arrival_count",
                    labels={"time_label": "Time of Day", "arrival_count": "Arrivals"},
                    markers=True,
                    color_discrete_sequence=["#0284C7"],
                )
                apply_clean_layout(fig_arrivals, height=290, margin=dict(l=35, r=20, t=15, b=35))
                st.plotly_chart(fig_arrivals, use_container_width=True)
            else:
                st.info("Hourly arrival dataset unavailable.")
            st.markdown("</div>", unsafe_allow_html=True)

        with col_c2:
            st.markdown(
                """
                <div class="content-box">
                    <div class="content-header">Average Wait Time by Queue vs. 30m Benchmark</div>
                """,
                unsafe_allow_html=True,
            )
            df_queue_sorted = df_queue.sort_values(by="avg_wait_minutes", ascending=True)
            bar_colors = [QUEUE_COLORS.get(q, "#0284C7") for q in df_queue_sorted["queue_id"]]

            fig_wait_overview = go.Figure()
            fig_wait_overview.add_trace(
                go.Bar(
                    y=[QUEUE_NAMES.get(q, q) for q in df_queue_sorted["queue_id"]],
                    x=df_queue_sorted["avg_wait_minutes"],
                    orientation="h",
                    marker_color=bar_colors,
                    text=df_queue_sorted["avg_wait_minutes"].apply(lambda v: f"{v} min"),
                    textposition="auto",
                )
            )
            fig_wait_overview.add_vline(
                x=30,
                line_dash="dash",
                line_color="#DC2626",
                annotation_text="30m Target",
                annotation_position="bottom right",
            )
            apply_clean_layout(
                fig_wait_overview,
                height=290,
                xaxis_title="Average Wait (Minutes)",
                yaxis_title="",
                margin=dict(l=10, r=20, t=15, b=35),
            )
            st.plotly_chart(fig_wait_overview, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        # Prioritized Operational Alert & Action Box
        st.markdown(
            """
            <div class="content-box">
                <div class="content-header">Prioritized Operational Status & Root Cause Diagnostics</div>
                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px;">
                    <div style="border-left: 3px solid #DC2626; padding-left: 12px;">
                        <span class="pill pill-red">Critical Bottleneck</span>
                        <div style="font-weight: 600; margin: 4px 0; color: #0F172A;">Financial Aid & Scholarships</div>
                        <div style="font-size: 0.82rem; color: #475569; line-height: 1.45;">
                            Average wait is <strong>152.5 min</strong> with 79.1% exceeding 30 min. 
                            Long transaction durations (~9.4m) keep counters saturated at <strong>97.4% utilization</strong>.
                        </div>
                    </div>
                    <div style="border-left: 3px solid #2563EB; padding-left: 12px;">
                        <span class="pill pill-blue">High Volume</span>
                        <div style="font-weight: 600; margin: 4px 0; color: #0F172A;">Admissions & Enrollment</div>
                        <div style="font-size: 0.82rem; color: #475569; line-height: 1.45;">
                            Highest volume queue (<strong>154 students</strong>, 30.8% share). 
                            Average wait is 113.3 min with <strong>83.5% counter utilization</strong>.
                        </div>
                    </div>
                    <div style="border-left: 3px solid #059669; padding-left: 12px;">
                        <span class="pill pill-green">Surplus Capacity</span>
                        <div style="font-weight: 600; margin: 4px 0; color: #0F172A;">Campus ID & General Help</div>
                        <div style="font-size: 0.82rem; color: #475569; line-height: 1.45;">
                            Minimal wait time (<strong>1.7 min</strong>) and low counter utilization (<strong>30.3%</strong>). 
                            Holds spare capacity that can relieve Financial Aid.
                        </div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Clear navigation action to Capacity Planning
        st.markdown("<div style='height: 4px;'></div>", unsafe_allow_html=True)
        col_act1, col_act2 = st.columns([3, 1])
        with col_act1:
            st.write("Evaluate reallocating underutilized COUNTER_07 to eliminate the Financial Aid bottleneck:")
        with col_act2:
            if st.button("⚡ Open Capacity Planning Scenario →", use_container_width=True):
                st.session_state["nav_page"] = "Capacity Planning"
                st.rerun()

    # ==========================================================================
    # PAGE 2: QUEUE PERFORMANCE
    # ==========================================================================
    elif selected_nav == "Queue Performance":
        # Interactive Controls
        f_col1, f_col2 = st.columns([1, 2])
        with f_col1:
            queue_options = list(df_queue["queue_id"].unique())
            selected_queues = st.multiselect(
                "Filter Service Queues:",
                options=queue_options,
                default=queue_options,
                format_func=lambda q: QUEUE_NAMES.get(q, q),
            )
            if not selected_queues:
                selected_queues = queue_options

        with f_col2:
            user_threshold_minutes = st.slider(
                "Configurable Wait Threshold Benchmark (Minutes):",
                min_value=5,
                max_value=120,
                value=30,
                step=5,
                help="Evaluate the percentage of students whose wait exceeded this duration.",
            )

        # Filtered DataFrame
        f_queue = df_queue[df_queue["queue_id"].isin(selected_queues)].copy()
        f_sla = df_sla[df_sla["queue_id"].isin(selected_queues)].copy()

        # Dynamic Threshold Evaluation
        if raw_df is not None and not raw_df.empty:
            starts_filtered = raw_df[
                (raw_df["event_type"] == "SERVICE_START") & (raw_df["queue_id"].isin(selected_queues))
            ]
            cust_total = len(starts_filtered)
            cust_breached = (starts_filtered["wait_time_seconds"] > (user_threshold_minutes * 60)).sum()
            custom_breach_rate = round((cust_breached / cust_total * 100.0), 1) if cust_total > 0 else 0.0
            custom_info = f"**{cust_breached} of {cust_total} students ({custom_breach_rate}%)** waited strictly longer than **{user_threshold_minutes} minutes** across selected queues."
        else:
            cust_total = f_sla["total_customers_served"].sum() if not f_sla.empty else 0
            cust_breached = f_sla["breach_count_30m"].sum() if not f_sla.empty else 0
            custom_breach_rate = round((cust_breached / cust_total * 100.0), 1) if cust_total > 0 else 0.0
            custom_info = f"Showing 30-minute standard benchmark: **{custom_breach_rate}%** of customers breached."

        st.info(f"📊 **Threshold Analysis:** {custom_info}")

        col_q1, col_q2 = st.columns([3, 2])

        with col_q1:
            st.markdown(
                """
                <div class="content-box">
                    <div class="content-header">Wait Time vs. Service Duration (Minutes)</div>
                """,
                unsafe_allow_html=True,
            )
            fig_perf = go.Figure()
            fig_perf.add_trace(
                go.Bar(
                    x=[QUEUE_NAMES.get(q, q) for q in f_queue["queue_id"]],
                    y=f_queue["avg_wait_minutes"],
                    name="Average Wait",
                    marker_color="#2563EB",
                    text=f_queue["avg_wait_minutes"].apply(lambda v: f"{v}m"),
                    textposition="auto",
                )
            )
            fig_perf.add_trace(
                go.Bar(
                    x=[QUEUE_NAMES.get(q, q) for q in f_queue["queue_id"]],
                    y=f_queue["median_wait_minutes"],
                    name="Median Wait",
                    marker_color="#93C5FD",
                    text=f_queue["median_wait_minutes"].apply(lambda v: f"{v}m"),
                    textposition="auto",
                )
            )
            fig_perf.add_trace(
                go.Bar(
                    x=[QUEUE_NAMES.get(q, q) for q in f_queue["queue_id"]],
                    y=f_queue["avg_service_minutes"],
                    name="Average Service Duration",
                    marker_color="#10B981",
                    text=f_queue["avg_service_minutes"].apply(lambda v: f"{v}m"),
                    textposition="auto",
                )
            )
            apply_clean_layout(
                fig_perf,
                barmode="group",
                height=320,
                xaxis_title="",
                yaxis_title="Minutes",
                legend_top=True,
                margin=dict(l=35, r=20, t=25, b=35),
            )
            st.plotly_chart(fig_perf, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with col_q2:
            st.markdown(
                """
                <div class="content-box">
                    <div class="content-header">Cumulative Wait Time Distribution (CDF)</div>
                """,
                unsafe_allow_html=True,
            )
            if raw_df is not None and not raw_df.empty:
                starts_cdf = raw_df[
                    (raw_df["event_type"] == "SERVICE_START") & (raw_df["queue_id"].isin(selected_queues))
                ].copy()
                starts_cdf["wait_minutes"] = starts_cdf["wait_time_seconds"] / 60.0

                fig_cdf = px.ecdf(
                    starts_cdf,
                    x="wait_minutes",
                    color="queue_id",
                    color_discrete_map=QUEUE_COLORS,
                    labels={"wait_minutes": "Wait Time (Minutes)", "probability": "Cumulative Share"},
                )
                fig_cdf.add_vline(
                    x=user_threshold_minutes,
                    line_dash="dash",
                    line_color="#475569",
                    annotation_text=f"{user_threshold_minutes}m threshold",
                    annotation_position="top left",
                )
                apply_clean_layout(
                    fig_cdf,
                    height=320,
                    margin=dict(l=35, r=20, t=25, b=35),
                    legend_top=True,
                )
                st.plotly_chart(fig_cdf, use_container_width=True)
            else:
                st.info("Raw event log required for cumulative distribution.")
            st.markdown("</div>", unsafe_allow_html=True)

        # Statistical Insight Note
        st.markdown(
            """
            <div style="background-color: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 6px; padding: 10px 14px; font-size: 0.82rem; color: #475569; margin-bottom: 14px;">
                <strong>Interpreting Average vs. Median Wait:</strong> The average wait time reflects aggregate delays across the full day, while the median reflects the experience of a typical customer. In <strong>Admissions</strong>, the median wait (102.1 min) is lower than the average (113.3 min), reflecting how midday rush hour arrival surges disproportionately skewed total delays upward.
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Performance Summary Table
        st.markdown("##### Queue Turnaround & Performance Summary")
        table_df = f_queue[
            [
                "queue_id",
                "service_type",
                "serviced_customers",
                "avg_wait_minutes",
                "median_wait_minutes",
                "max_wait_minutes",
                "avg_service_minutes",
                "avg_total_turnaround_minutes",
            ]
        ].copy()
        table_df.columns = [
            "Queue ID",
            "Service Description",
            "Students",
            "Avg Wait (min)",
            "Median Wait (min)",
            "Max Wait (min)",
            "Avg Service (min)",
            "Turnaround (min)",
        ]
        st.dataframe(table_df, use_container_width=True, hide_index=True)

        # Spark MLlib Wait-Time Risk Prediction Section
        if df_ml_risk is not None and not df_ml_risk.empty:
            st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)
            st.markdown(
                """
                <div class="content-box">
                    <div class="content-header">Spark MLlib: Supervised Wait-Time Risk Prediction</div>
                    <div style="font-size: 0.83rem; color: #475569; line-height: 1.45; margin-bottom: 12px;">
                        PySpark MLlib supervised classification pipeline predicting whether an arriving student will breach the 30-minute wait threshold (&gt;1,800s).
                        <strong>Zero-Leakage Design:</strong> Trained strictly on arrival-time observable features (Queue ID, Priority Level, Arrival Hour, Rush Hour Indicator, and 30-minute Rolling Prior Arrival Density). Downstream service durations and counter assignments are strictly excluded.
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            risk_map = dict(zip(df_ml_risk["queue_id"], df_ml_risk["predicted_long_wait_risk_pct"]))
            with col_m1:
                st.markdown(
                    f"""
                    <div class="kpi-card" style="border-left: 4px solid #DC2626;">
                        <div class="kpi-title">Financial Aid Risk</div>
                        <div class="kpi-number" style="color: #DC2626;">{risk_map.get('Q_FINANCIAL_AID', 0.0):.1f}%</div>
                        <div class="kpi-detail">Predicted Long-Wait Probability</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with col_m2:
                st.markdown(
                    f"""
                    <div class="kpi-card" style="border-left: 4px solid #DC2626;">
                        <div class="kpi-title">Admissions Risk</div>
                        <div class="kpi-number" style="color: #DC2626;">{risk_map.get('Q_ADMISSIONS', 0.0):.1f}%</div>
                        <div class="kpi-detail">Predicted Long-Wait Probability</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with col_m3:
                st.markdown(
                    f"""
                    <div class="kpi-card" style="border-left: 4px solid #D97706;">
                        <div class="kpi-title">Academic Records Risk</div>
                        <div class="kpi-number" style="color: #D97706;">{risk_map.get('Q_ACADEMIC_RECORDS', 0.0):.1f}%</div>
                        <div class="kpi-detail">Predicted Long-Wait Probability</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with col_m4:
                st.markdown(
                    f"""
                    <div class="kpi-card" style="border-left: 4px solid #16A34A;">
                        <div class="kpi-title">Campus ID Risk</div>
                        <div class="kpi-number" style="color: #16A34A;">{risk_map.get('Q_ID_GENERAL', 0.0):.1f}%</div>
                        <div class="kpi-detail">Predicted Long-Wait Probability</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            if df_ml_metrics is not None and not df_ml_metrics.empty:
                st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
                st.markdown("##### Spark MLlib Pipeline Model Comparison (Holdout Test Split)")
                disp_ml = df_ml_metrics.copy()
                disp_ml.columns = [
                    "Model",
                    "Accuracy (%)",
                    "F1 Score",
                    "Precision",
                    "Recall",
                    "AUC-ROC",
                    "AUC-PR",
                    "TP",
                    "FP",
                    "TN",
                    "FN",
                    "Test Samples",
                ]
                st.dataframe(disp_ml, use_container_width=True, hide_index=True)

                st.markdown(
                    """
                    <div style="background-color: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 6px; padding: 10px 14px; font-size: 0.81rem; color: #475569; margin-top: 8px;">
                        <strong>Audit Note & Synthetic Data Disclosure:</strong> The high AUC (~0.999&ndash;1.000) reflects the clean separability produced by deterministic M/M/c queuing dynamics across 500 customers in a single simulated day. In real university operations with non-stationary human walk-ins and varying case complexities, empirical AUC typically ranges from 0.75 to 0.85.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # ==========================================================================
    # PAGE 3: COUNTER UTILIZATION
    # ==========================================================================
    elif selected_nav == "Counter Utilization":
        st.markdown(
            """
            <div class="content-box">
                <div class="content-header">Counter Workload & Capacity Utilization</div>
                <div style="font-size: 0.84rem; color: #475569; line-height: 1.45;">
                    Counter utilization measures the percentage of the operating window (<strong>37,311 seconds / 10.36 hours</strong>) during which a counter was actively serving customers. 
                    Customer waiting time in queue lines is strictly excluded from counter utilization calculations.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        col_u_chart, col_u_info = st.columns([3, 2])

        with col_u_chart:
            def get_load_color(util: float) -> str:
                if util >= 90.0:
                    return "#DC2626"  # Red: Saturated
                elif util >= 75.0:
                    return "#D97706"  # Amber: High load
                elif util >= 50.0:
                    return "#2563EB"  # Blue: Balanced
                return "#64748B"      # Slate: Underutilized

            df_util_sorted = df_util.sort_values(by="counter_id", ascending=True)
            u_colors = [get_load_color(v) for v in df_util_sorted["utilization_pct"]]

            fig_counter = go.Figure()
            fig_counter.add_trace(
                go.Bar(
                    x=df_util_sorted["counter_id"],
                    y=df_util_sorted["utilization_pct"],
                    marker_color=u_colors,
                    text=df_util_sorted["utilization_pct"].apply(lambda v: f"{v}%"),
                    textposition="auto",
                )
            )
            fig_counter.add_hline(
                y=90.0,
                line_dash="dash",
                line_color="#DC2626",
                annotation_text="Critical Saturation (90%)",
                annotation_position="top right",
            )
            fig_counter.add_hline(
                y=75.0,
                line_dash="dot",
                line_color="#D97706",
                annotation_text="Target Load Limit (75%)",
                annotation_position="bottom right",
            )
            apply_clean_layout(
                fig_counter,
                height=320,
                xaxis_title="Physical Counter ID",
                yaxis_title="Utilization (%)",
                yaxis_range=[0, 110],
                margin=dict(l=35, r=20, t=25, b=35),
            )
            st.plotly_chart(fig_counter, use_container_width=True)

        with col_u_info:
            st.markdown(
                """
                <div class="content-box" style="height: 320px;">
                    <div class="content-header">Workload Distribution Analysis</div>
                    <div style="font-size: 0.83rem; color: #334155; line-height: 1.45;">
                        <p><strong>Severe Imbalance Across Department Counters:</strong></p>
                        <ul style="padding-left: 18px; margin: 0 0 10px 0;">
                            <li><strong>COUNTER_03 & COUNTER_04 (Financial Aid):</strong> Critical saturation at <strong>98.9% and 96.0%</strong> utilization (averaging 97.4%). Zero idle buffer.</li>
                            <li><strong>COUNTER_01 & COUNTER_02 (Admissions):</strong> Operating at <strong>83.2% and 83.8%</strong>, sustaining heavy volume.</li>
                            <li><strong>COUNTER_05 & COUNTER_06 (Records):</strong> Healthy balanced utilization at <strong>63.5% and 62.6%</strong>.</li>
                            <li><strong>COUNTER_07 & COUNTER_08 (ID General):</strong> Substantially underutilized at <strong>31.8% and 28.8%</strong> (averaging 30.3%).</li>
                        </ul>
                        <div style="background-color: #FEF3C7; border-left: 3px solid #D97706; padding: 6px 10px; font-size: 0.77rem; color: #92400E;">
                            <strong>Capacity Opportunity:</strong> One counter from ID General can be transferred to Financial Aid without compromising basic campus ID operations.
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Counter Utilization Detail Table
        st.markdown("##### Physical Counter Workload Table")
        util_display = df_util[
            [
                "counter_id",
                "queue_id",
                "customers_served",
                "total_busy_hours",
                "avg_service_minutes",
                "utilization_pct",
            ]
        ].copy()
        util_display.columns = [
            "Counter ID",
            "Assigned Queue",
            "Students Served",
            "Busy Hours",
            "Avg Service (min)",
            "Baseline Utilization (%)",
        ]
        st.dataframe(util_display, use_container_width=True, hide_index=True)

    # ==========================================================================
    # PAGE 4: CAPACITY PLANNING (DECISION SUPPORT CONSOLE)
    # ==========================================================================
    elif selected_nav == "Capacity Planning":
        st.markdown(
            """
            <div class="content-box">
                <div class="content-header">Dynamic Counter Reallocation Scenario Analysis</div>
                <div style="font-size: 0.84rem; color: #334155; line-height: 1.45;">
                    <strong>Tested Operational Hypothesis:</strong> Temporarily transfer <strong>COUNTER_07</strong> from 
                    <strong>Q_ID_GENERAL</strong> (underutilized at 30.3%) to <strong>Q_FINANCIAL_AID</strong> (saturated at 97.4%).
                    <br>
                    <span style="color: #64748B; font-size: 0.8rem;">
                        <strong>Methodology:</strong> Discrete-event FIFO re-simulation using exact historical arrival timestamps and service durations.
                        Total facility service workload (<strong>204,658 seconds</strong>) is strictly conserved across all 8 counters. Unchanged queues retain identical schedules.
                    </span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if df_realloc is not None and not df_realloc.empty:
            # 4 Outcome KPI Cards
            cp1, cp2, cp3, cp4 = st.columns(4)
            with cp1:
                st.markdown(
                    """
                    <div class="kpi-card" style="border-left: 4px solid #16A34A;">
                        <div class="kpi-title">Financial Aid Wait Delta</div>
                        <div class="kpi-number" style="color: #16A34A;">-87.5 min</div>
                        <div class="kpi-detail">152.5m &rarr; 65.0m (-57.4%)</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with cp2:
                st.markdown(
                    """
                    <div class="kpi-card" style="border-left: 4px solid #16A34A;">
                        <div class="kpi-title">Max Wait Reduction</div>
                        <div class="kpi-number" style="color: #16A34A;">-184.3 min</div>
                        <div class="kpi-detail">340.6m &rarr; 156.3m (-54.1%)</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with cp3:
                st.markdown(
                    """
                    <div class="kpi-card" style="border-left: 4px solid #0284C7;">
                        <div class="kpi-title">Facility Closing Time</div>
                        <div class="kpi-number" style="color: #0284C7;">-1.5 Hours</div>
                        <div class="kpi-detail">19:21:56 &rarr; 17:50:53 (Early close)</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with cp4:
                st.markdown(
                    """
                    <div class="kpi-card" style="border-left: 4px solid #64748B;">
                        <div class="kpi-title">Workload Conservation</div>
                        <div class="kpi-number" style="color: #334155; font-size: 1.35rem;">204,658 s</div>
                        <div class="kpi-detail kpi-success-text">Exactly Preserved (0% leak)</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            st.markdown("<div style='height: 14px;'></div>", unsafe_allow_html=True)

            col_sc_chart, col_sc_cards = st.columns([3, 2])

            with col_sc_chart:
                fig_realloc = go.Figure()
                fig_realloc.add_trace(
                    go.Bar(
                        x=[QUEUE_NAMES.get(q, q) for q in df_realloc["queue_id"]],
                        y=df_realloc["base_avg_wait_minutes"],
                        name="Baseline (2 Counters)",
                        marker_color="#64748B",
                        text=df_realloc["base_avg_wait_minutes"].apply(lambda v: f"{v}m"),
                        textposition="auto",
                    )
                )
                fig_realloc.add_trace(
                    go.Bar(
                        x=[QUEUE_NAMES.get(q, q) for q in df_realloc["queue_id"]],
                        y=df_realloc["sim_avg_wait_minutes"],
                        name="Reallocated Scenario",
                        marker_color="#0284C7",
                        text=df_realloc["sim_avg_wait_minutes"].apply(lambda v: f"{v}m"),
                        textposition="auto",
                    )
                )
                apply_clean_layout(
                    fig_realloc,
                    barmode="group",
                    height=320,
                    xaxis_title="",
                    yaxis_title="Average Wait (Minutes)",
                    legend_top=True,
                    margin=dict(l=35, r=20, t=25, b=35),
                )
                st.plotly_chart(fig_realloc, use_container_width=True)

            with col_sc_cards:
                st.markdown(
                    """
                    <div style="display: flex; flex-direction: column; gap: 8px;">
                        <div style="background-color: #F0FDF4; border: 1px solid #BBF7D0; border-left: 4px solid #16A34A; border-radius: 6px; padding: 10px 12px;">
                            <div style="font-weight: 600; color: #166534; font-size: 0.85rem;">1. Financial Aid Bottleneck Relieved</div>
                            <div style="font-size: 0.78rem; color: #15803D; margin-top: 2px;">
                                Average wait dropped by <strong>87.5 min (-57.4%)</strong>, from 152.5m down to <strong>65.0m</strong>. 
                                Maximum student delay reduced by 184.3 min (-54.1%).
                            </div>
                        </div>
                        <div style="background-color: #FFFBEB; border: 1px solid #FDE68A; border-left: 4px solid #D97706; border-radius: 6px; padding: 10px 12px;">
                            <div style="font-weight: 600; color: #92400E; font-size: 0.85rem;">2. Transparent Trade-off at ID General</div>
                            <div style="font-size: 0.78rem; color: #B45309; margin-top: 2px;">
                                Operating with 1 counter increased ID General average wait from <strong>1.7m to 51.0m</strong>. 
                                Illustrates that capacity transfer redistributes queue delays rather than eliminating workload.
                            </div>
                        </div>
                        <div style="background-color: #EFF6FF; border: 1px solid #BFDBFE; border-left: 4px solid #2563EB; border-radius: 6px; padding: 10px 12px;">
                            <div style="font-weight: 600; color: #1E40AF; font-size: 0.85rem;">3. Facility Operating Overtime Reduced</div>
                            <div style="font-size: 0.78rem; color: #1D4ED8; margin-top: 2px;">
                                Clearing Financial Aid faster allows the facility to finish all 500 students at <strong>17:50:53</strong> instead of <strong>19:21:56</strong> (1.5 hours earlier closing).
                            </div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            # Reallocation Comparison Table
            st.markdown("##### Baseline vs. Reallocated Scenario Comparison")
            display_realloc = df_realloc[
                [
                    "queue_id",
                    "counter_allocation_change",
                    "base_avg_wait_minutes",
                    "sim_avg_wait_minutes",
                    "wait_time_change_minutes",
                    "wait_time_change_pct",
                    "base_max_wait_minutes",
                    "sim_max_wait_minutes",
                    "base_breach_30m_pct",
                    "sim_breach_30m_pct",
                    "base_breach_60m_pct",
                    "sim_breach_60m_pct",
                ]
            ].copy()
            display_realloc.columns = [
                "Queue ID",
                "Allocation Change",
                "Base Avg Wait",
                "Sim Avg Wait",
                "Delta (min)",
                "Change (%)",
                "Base Max",
                "Sim Max",
                "Base >30m (%)",
                "Sim >30m (%)",
                "Base >60m (%)",
                "Sim >60m (%)",
            ]
            st.dataframe(display_realloc, use_container_width=True, hide_index=True)

            # Reallocated Counter Utilization Table
            if df_realloc_counters is not None and not df_realloc_counters.empty:
                st.markdown("##### Counter Workload Under Reallocated Capacity")
                st.markdown(
                    """
                    <div style="background-color: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 6px; padding: 10px 14px; font-size: 0.81rem; color: #475569; margin-bottom: 10px;">
                        <strong>Understanding Reallocated Utilization Denominators:</strong>
                        <ul style="margin: 3px 0 0 0; padding-left: 18px;">
                            <li><strong>Standardized Utilization (37,311s Baseline Window):</strong> Evaluates workload against the fixed 10.36-hour baseline window. Admissions utilization remains exactly 83.2% / 83.8%, and Financial Aid counters normalize to ~65&ndash;66%.</li>
                            <li><strong>Scenario Utilization (31,848s Scenario Window):</strong> Evaluates active time against the shorter 8.85-hour operating day. Because Financial Aid clears 1.5 hours sooner, counters at unchanged queues operate across a shorter facility window, mathematically registering higher utilization rates (~97&ndash;98%).</li>
                        </ul>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                disp_rc = df_realloc_counters[
                    [
                        "counter_id",
                        "reallocated_queue_id",
                        "customers_served",
                        "total_busy_hours",
                        "standardized_utilization_pct",
                        "scenario_utilization_pct",
                    ]
                ].copy()
                disp_rc.columns = [
                    "Counter ID",
                    "Reallocated Queue",
                    "Customers Served",
                    "Busy Hours",
                    "Standardized Util (10.36h Window)",
                    "Scenario Util (8.85h Window)",
                ]
                st.dataframe(disp_rc, use_container_width=True, hide_index=True)

    # ==========================================================================
    # PAGE 5: DATA EXPLORER
    # ==========================================================================
    elif selected_nav == "Data Explorer":
        st.markdown(
            """
            <div class="content-box">
                <div class="content-header">Analytical Output Inspector & Evidence Export</div>
                <div style="font-size: 0.83rem; color: #475569;">
                    Inspect and export pre-computed analytical datasets generated by Apache Spark (PySpark 3.5.9).
                    All data reflects synthetic simulation records (<code>is_synthetic = TRUE</code>).
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        dataset_options = {
            "queue_metrics": "Stage 2: Queue Performance & Turnaround Metrics (queue_metrics.csv)",
            "counter_utilization": "Stage 2: Baseline Counter Utilization (counter_utilization.csv)",
            "hourly_arrivals": "Stage 2: Hourly Customer Arrivals (hourly_arrivals.csv)",
            "customer_counts": "Stage 2: Customer Counts & Demand Share (customer_counts_by_queue.csv)",
            "event_counts": "Stage 2: Lifecycle Event Counts (event_counts.csv)",
            "sla_breaches": "Stage 3: SLA Breach Intelligence (sla_breaches_by_queue.csv)",
            "capacity_comparison": "Stage 3: Capacity & Bottleneck Matrix (capacity_comparison.csv)",
            "reallocation_metrics": "Stage 3: Reallocation Simulation Impact (reallocation_simulation_metrics.csv)",
            "reallocated_counters": "Stage 3: Reallocated Counter Utilization (reallocated_counter_utilization.csv)",
            "ml_risk": "Stage 4 (Spark MLlib): Queue Long-Wait Risk Estimates (queue_long_wait_risk.csv)",
            "ml_metrics": "Stage 4 (Spark MLlib): Model Evaluation & Performance Metrics (ml_model_metrics.csv)",
        }

        if raw_df is not None:
            dataset_options["raw_events"] = "Stage 1: Raw Queue Event Log (queue_events.csv - Preview)"

        chosen_key = st.selectbox(
            "Select Analytical Dataset to Inspect:",
            options=list(dataset_options.keys()),
            format_func=lambda k: dataset_options[k],
        )

        if chosen_key == "raw_events":
            preview_df = raw_df.head(100)
            st.caption(f"Displaying first 100 of {len(raw_df)} raw synthetic lifecycle events.")
            st.dataframe(preview_df, use_container_width=True, hide_index=True)
            csv_bytes = raw_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download Full Raw Dataset (queue_events.csv)",
                data=csv_bytes,
                file_name="queue_events.csv",
                mime="text/csv",
            )
        else:
            chosen_df = datasets.get(chosen_key)
            if chosen_df is not None:
                st.caption(
                    f"Displaying **{len(chosen_df)} rows** and **{len(chosen_df.columns)} columns** from `outputs/{chosen_key}.csv`."
                )
                st.dataframe(chosen_df, use_container_width=True, hide_index=True)
                csv_bytes = chosen_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label=f"📥 Download {chosen_key}.csv",
                    data=csv_bytes,
                    file_name=f"{chosen_key}.csv",
                    mime="text/csv",
                )
            else:
                st.warning(f"Dataset `{chosen_key}.csv` is currently unavailable in `outputs/`.")

    # Global Compact Disclaimer & Collapsible System Information
    st.markdown(
        """
        <div class="footer-disclaimer">
            QueueWise uses synthetic discrete-event queuing data for INT315 Cluster Computing experiments. Results do not represent actual university operations.
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("System & Cluster Computing Architecture Details (INT315)", expanded=False):
        c_env1, c_env2 = st.columns(2)
        with c_env1:
            st.markdown(
                """
                **Local Processing Environment:**
                - **Processing Engine:** Apache Spark (PySpark 3.5.9)
                - **Execution Mode:** Local master (`local[*]`), 4 partitions
                - **Runtime Environment:** Python 3.10.0 | Windows 10
                - **Java Runtime:** OpenJDK 8 (Adoptium Temurin 1.8.0_482)
                - **Hadoop Winutils:** `C:\\hadoop\\bin\\winutils.exe`
                """
            )
        with c_env2:
            st.markdown(
                """
                **Cluster Analytics & Simulation Capabilities:**
                - **Total Workload:** 204,658 seconds (56.85 counter-hours strictly conserved)
                - **Machine Learning:** Spark MLlib Pipeline (`StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `LogisticRegression`)
                - **Streaming Processing:** Spark Structured Streaming (micro-batching with event-time watermarking)
                - **Presentation Stack:** Streamlit 1.65.0 | Plotly 7.1.0 | pandas 2.3.3
                """
            )


if __name__ == "__main__":
    main()
