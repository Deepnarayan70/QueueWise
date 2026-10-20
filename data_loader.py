"""
QueueWise -- Data Loader and Analytical Business Logic Service
INT315 Cluster Computing Capstone Project

Provides robust data ingestion, timestamp-aware caching, schema validation,
and reusable queuing KPI calculations. Separates data-loading and business logic
from Streamlit presentation layers.
"""

import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# ==============================================================================
# Domain Constants & Color Mappings
# ==============================================================================

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

EXPECTED_OUTPUT_FILES = {
    "event_counts": "event_counts.csv",
    "customer_counts": "customer_counts_by_queue.csv",
    "queue_metrics": "queue_metrics.csv",
    "hourly_arrivals": "hourly_arrivals.csv",
    "counter_utilization": "counter_utilization.csv",
    "sla_breaches": "sla_breaches_by_queue.csv",
    "capacity_comparison": "capacity_comparison.csv",
    "reallocation_metrics": "reallocation_simulation_metrics.csv",
    "reallocated_counters": "reallocated_counter_utilization.csv",
    "ml_risk": "queue_long_wait_risk.csv",
    "ml_metrics": "ml_model_metrics.csv",
}


# ==============================================================================
# Timestamp-Aware File Ingestion & Cache Management
# ==============================================================================

def get_directory_mtimes(directory: str) -> Tuple[Tuple[str, float], ...]:
    """
    Returns a sorted tuple of (filename, mtime) for cache invalidation.
    When a pipeline updates any CSV on disk, the mtime changes and Streamlit
    automatically invalidates cached DataFrames without requiring a manual restart.
    """
    if not os.path.exists(directory):
        return ()
    items = []
    for fname in sorted(os.listdir(directory)):
        if fname.endswith(".csv") or fname.endswith(".json"):
            fpath = os.path.join(directory, fname)
            try:
                items.append((fname, os.path.getmtime(fpath)))
            except OSError:
                pass
    return tuple(items)


@st.cache_data
def _load_analytical_datasets_cached(
    outputs_dir: str,
    mtimes_fingerprint: Tuple[Tuple[str, float], ...],
) -> Tuple[Dict[str, Optional[pd.DataFrame]], Dict[str, str]]:
    """Internal cached loader triggered whenever file modification times change."""
    dfs: Dict[str, Optional[pd.DataFrame]] = {}
    missing_files: Dict[str, str] = {}

    for key, filename in EXPECTED_OUTPUT_FILES.items():
        fpath = os.path.join(outputs_dir, filename)
        if os.path.exists(fpath):
            try:
                df = pd.read_csv(fpath)
                if df.empty:
                    dfs[key] = None
                    missing_files[key] = f"Dataset is empty: {filename}"
                else:
                    dfs[key] = df
            except Exception as e:
                dfs[key] = None
                missing_files[key] = f"Failed to read {filename}: {e}"
        else:
            dfs[key] = None
            missing_files[key] = f"Missing file: {filename}"

    return dfs, missing_files


def load_analytical_datasets(outputs_dir: str = "outputs") -> Tuple[Dict[str, Optional[pd.DataFrame]], Dict[str, str]]:
    """
    Loads all pre-computed analytical datasets from outputs/ with automatic
    cache invalidation on file modification.
    """
    mtimes = get_directory_mtimes(outputs_dir)
    return _load_analytical_datasets_cached(outputs_dir, mtimes)


@st.cache_data
def _load_raw_queue_events_cached(data_path: str, mtime: float) -> Optional[pd.DataFrame]:
    """Internal cached loader for the raw synthetic lifecycle event log."""
    if os.path.exists(data_path):
        try:
            return pd.read_csv(data_path)
        except Exception:
            return None
    return None


def load_raw_queue_events(data_path: str = os.path.join("data", "queue_events.csv")) -> Optional[pd.DataFrame]:
    """Loads raw queue events with automatic cache invalidation."""
    mtime = os.path.getmtime(data_path) if os.path.exists(data_path) else 0.0
    return _load_raw_queue_events_cached(data_path, mtime)


# ==============================================================================
# Reusable Analytical Calculations & KPI Helpers
# ==============================================================================

def compute_executive_kpis(df_queue: pd.DataFrame, df_sla: pd.DataFrame) -> Dict[str, Any]:
    """
    Computes top-level facility metrics from validated Stage 2 & 3 DataFrames.
    Avoids recalculating or duplicating formulas inside presentation templates.
    """
    if df_queue is None or df_queue.empty:
        return {
            "total_customers": 0,
            "weighted_avg_wait": 0.0,
            "breach_rate_30m": 0.0,
            "tot_breached_30m": 0,
            "tot_served": 0,
            "bottleneck_id": "N/A",
            "bottleneck_name": "N/A",
            "bottleneck_wait": 0.0,
        }

    total_customers = int(df_queue["serviced_customers"].sum())
    weighted_avg_wait = (
        round(
            (df_queue["avg_wait_minutes"] * df_queue["serviced_customers"]).sum() / total_customers,
            1,
        )
        if total_customers > 0
        else 0.0
    )

    tot_served = int(df_sla["total_customers_served"].sum()) if df_sla is not None and not df_sla.empty else 0
    tot_breached_30m = int(df_sla["breach_count_30m"].sum()) if df_sla is not None and not df_sla.empty else 0
    breach_rate_30m = round((tot_breached_30m / tot_served * 100.0), 1) if tot_served > 0 else 0.0

    bottleneck_row = df_queue.loc[df_queue["avg_wait_minutes"].idxmax()] if not df_queue.empty else None
    bottleneck_id = bottleneck_row["queue_id"] if bottleneck_row is not None else "N/A"
    bottleneck_wait = round(float(bottleneck_row["avg_wait_minutes"]), 1) if bottleneck_row is not None else 0.0

    return {
        "total_customers": total_customers,
        "weighted_avg_wait": weighted_avg_wait,
        "breach_rate_30m": breach_rate_30m,
        "tot_breached_30m": tot_breached_30m,
        "tot_served": tot_served,
        "bottleneck_id": bottleneck_id,
        "bottleneck_name": QUEUE_NAMES.get(bottleneck_id, bottleneck_id),
        "bottleneck_wait": bottleneck_wait,
    }


def compute_custom_threshold_metrics(
    raw_df: Optional[pd.DataFrame],
    df_sla: pd.DataFrame,
    selected_queues: List[str],
    threshold_minutes: int,
) -> Tuple[int, int, float, str]:
    """
    Computes custom wait threshold breach stats for filtered queues.
    Prefers customer-level raw events; falls back to 30-min analytical outputs if raw log is missing.
    """
    if raw_df is not None and not raw_df.empty:
        starts = raw_df[
            (raw_df["event_type"] == "SERVICE_START") & (raw_df["queue_id"].isin(selected_queues))
        ]
        total = len(starts)
        breached = int((starts["wait_time_seconds"] > (threshold_minutes * 60)).sum())
        rate = round((breached / total * 100.0), 1) if total > 0 else 0.0
        info_text = f"{breached} of {total} students ({rate}%) waited strictly longer than {threshold_minutes} minutes."
        return total, breached, rate, info_text

    # Fallback to pre-computed SLA output
    if df_sla is not None and not df_sla.empty:
        f_sla = df_sla[df_sla["queue_id"].isin(selected_queues)]
        total = int(f_sla["total_customers_served"].sum()) if not f_sla.empty else 0
        breached = int(f_sla["breach_count_30m"].sum()) if not f_sla.empty else 0
        rate = round((breached / total * 100.0), 1) if total > 0 else 0.0
        info_text = f"Showing 30-minute benchmark: {breached} of {total} students ({rate}%) exceeded 30 minutes."
        return total, breached, rate, info_text

    return 0, 0, 0.0, "No SLA breach data available."
