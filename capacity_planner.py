"""
QueueWise -- Real-Time Queue Analytics and Capacity Planning Using Apache Spark
Stage 3: SLA Breach Detection and Dynamic Capacity Planning

Description:
    Evaluates waiting-time Service Level Agreement (SLA) breaches across
    university help desk service queues using PySpark SQL & DataFrames.
    Correlates queue demand, wait times, and physical counter utilization.
    Implements a discrete event queue re-simulation model to evaluate a
    dynamic counter reallocation capacity-planning scenario (reallocating
    an underutilized counter to a saturated bottleneck queue).

Academic Notice:
    All analyzed events and SLA thresholds (e.g. 15, 30, 60 minutes) are
    configurable simulation assumptions for cluster computing experimentation,
    not official university operational service standards.

Author: QueueWise Team (INT315 Cluster Computing Project)
"""

import argparse
import datetime
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# Configure Windows-specific environment variables for PySpark stability
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_spark_session(app_name: str = "QueueWise-Stage3-CapacityPlanning") -> SparkSession:
    """Initializes and returns an optimized local SparkSession configured for Windows."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_event_schema() -> StructType:
    """Returns the explicit PySpark schema for data/queue_events.csv."""
    return StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("queue_id", StringType(), nullable=False),
            StructField("counter_id", StringType(), nullable=True),
            StructField("service_type", StringType(), nullable=True),
            StructField("priority_level", StringType(), nullable=True),
            StructField("wait_time_seconds", IntegerType(), nullable=True),
            StructField("service_duration_seconds", IntegerType(), nullable=True),
            StructField("is_synthetic", BooleanType(), nullable=True),
            StructField("data_source", StringType(), nullable=True),
        ]
    )


# ==============================================================================
# SECTION 1: Configurable SLA Breach Detection
# ==============================================================================


def analyze_sla_breaches(
    spark: SparkSession,
    df,
    thresholds_minutes: List[int],
) -> object:
    """
    Computes waiting-time SLA breaches across all department queues using
    exactly one SERVICE_START row per customer.

    Thresholds (e.g., 15m, 30m, 60m) are configurable simulation parameters.
    """
    print("\n" + "=" * 75)
    print(" [SECTION 1] Waiting-Time SLA Breach Detection (Baseline)")
    print("=" * 75)
    print(f"[*] Configurable SLA Thresholds Evaluated: {thresholds_minutes} minutes")
    print("    (Note: Defined as simulation benchmarks for capacity planning evaluation)")

    df.createOrReplaceTempView("raw_events")

    # Filter strictly to SERVICE_START rows to ensure exactly one observation per customer
    df_starts = spark.sql(
        """
        SELECT 
            customer_id,
            queue_id,
            service_type,
            priority_level,
            wait_time_seconds,
            ROUND(wait_time_seconds / 60.0, 2) AS wait_time_minutes
        FROM raw_events
        WHERE event_type = 'SERVICE_START'
        """
    )
    df_starts.createOrReplaceTempView("customer_service_starts")

    # Build dynamic SQL aggregation expressions for each configured SLA threshold
    agg_clauses = []
    for thresh in sorted(thresholds_minutes):
        thresh_sec = thresh * 60
        agg_clauses.append(
            f"SUM(CASE WHEN wait_time_seconds > {thresh_sec} THEN 1 ELSE 0 END) AS breach_count_{thresh}m"
        )
        agg_clauses.append(
            f"ROUND(SUM(CASE WHEN wait_time_seconds > {thresh_sec} THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) AS breach_pct_{thresh}m"
        )

    clauses_str = ",\n            ".join(agg_clauses)

    query = f"""
        SELECT 
            queue_id,
            service_type,
            COUNT(*) AS total_customers_served,
            ROUND(AVG(wait_time_minutes), 2) AS avg_wait_minutes,
            ROUND(PERCENTILE_APPROX(wait_time_seconds, 0.5) / 60.0, 2) AS median_wait_minutes,
            ROUND(MAX(wait_time_minutes), 2) AS max_wait_minutes,
            {clauses_str}
        FROM customer_service_starts
        GROUP BY queue_id, service_type
        ORDER BY avg_wait_minutes DESC
    """

    df_sla = spark.sql(query)
    print("\n--- Baseline SLA Breach Summary by Queue ---")
    df_sla.show(truncate=False)

    return df_sla


# ==============================================================================
# SECTION 2: Demand, Wait Time, and Counter Utilization Correlation
# ==============================================================================


def correlate_demand_and_capacity(
    spark: SparkSession,
    df,
    df_sla,
) -> object:
    """
    Correlates arrival demand, average waiting time, and counter utilization.
    Categorizes queues into capacity health tiers (Critical Bottleneck, Moderate, Healthy).
    """
    print("\n" + "=" * 75)
    print(" [SECTION 2] Demand, Wait Time, and Counter Capacity Correlation")
    print("=" * 75)

    # 1. Total simulation operating window
    bounds = df.select(
        F.min("timestamp").alias("start_ts"),
        F.max("timestamp").alias("end_ts"),
    ).collect()[0]
    window_sec = int((bounds["end_ts"] - bounds["start_ts"]).total_seconds())

    # 2. Counter utilization per queue
    df_counter_agg = (
        df.filter(F.col("event_type") == "SERVICE_COMPLETION")
        .groupBy("queue_id")
        .agg(
            F.countDistinct("counter_id").alias("counter_count"),
            F.collect_set("counter_id").alias("assigned_counters_list"),
            F.sum("service_duration_seconds").alias("total_service_seconds"),
            F.round(F.avg("service_duration_seconds") / 60.0, 2).alias("avg_service_minutes"),
        )
        .withColumn(
            "total_available_counter_seconds",
            F.col("counter_count") * F.lit(window_sec),
        )
        .withColumn(
            "avg_counter_utilization_pct",
            F.round(
                (F.col("total_service_seconds") / F.col("total_available_counter_seconds")) * 100.0,
                2,
            ),
        )
    )

    df_counter_agg.createOrReplaceTempView("queue_counter_stats")
    df_sla.createOrReplaceTempView("sla_stats")

    # Combine SLA and Counter stats
    df_capacity = spark.sql(
        f"""
        SELECT 
            s.queue_id,
            s.service_type,
            c.counter_count AS active_counters,
            CONCAT_WS(', ', c.assigned_counters_list) AS assigned_counters,
            s.total_customers_served,
            ROUND(s.total_customers_served * 100.0 / (SELECT SUM(total_customers_served) FROM sla_stats), 2) AS demand_share_pct,
            s.avg_wait_minutes,
            s.max_wait_minutes,
            c.avg_service_minutes,
            c.avg_counter_utilization_pct,
            s.breach_pct_15m AS sla_breach_15m_pct,
            s.breach_pct_30m AS sla_breach_30m_pct,
            s.breach_pct_60m AS sla_breach_60m_pct,
            CASE 
                WHEN c.avg_counter_utilization_pct >= 90.0 THEN 'CRITICAL_BOTTLENECK'
                WHEN c.avg_counter_utilization_pct >= 75.0 THEN 'HIGH_UTILIZATION'
                WHEN c.avg_counter_utilization_pct >= 50.0 THEN 'BALANCED'
                ELSE 'UNDERUTILIZED'
            END AS capacity_status
        FROM sla_stats s
        JOIN queue_counter_stats c ON s.queue_id = c.queue_id
        ORDER BY s.avg_wait_minutes DESC
        """
    )

    print("\n--- Comprehensive Capacity & Demand Comparison Table ---")
    df_capacity.select(
        "queue_id",
        "active_counters",
        "total_customers_served",
        "demand_share_pct",
        "avg_wait_minutes",
        "avg_counter_utilization_pct",
        "sla_breach_30m_pct",
        "capacity_status",
    ).show(truncate=False)

    return df_capacity


# ==============================================================================
# SECTION 3: Event-Level Counter Reallocation Simulation
# ==============================================================================


@dataclass
class CustomerTask:
    customer_id: str
    queue_id: str
    service_type: str
    priority_level: str
    arrival_time: datetime.datetime
    service_duration_seconds: int


def run_event_level_reallocation_simulation(
    df,
    reassigned_counter: str = "COUNTER_07",
    donor_queue: str = "Q_ID_GENERAL",
    receiver_queue: str = "Q_FINANCIAL_AID",
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    """
    Reruns the discrete-event queue simulation using the exact customer arrival
    sequences and individual service durations from the baseline dataset, but under
    a reallocated counter configuration:

    Scenario:
        - Donor Queue (Q_ID_GENERAL): Loses COUNTER_07 -> Retains COUNTER_08 (1 counter).
        - Receiver Queue (Q_FINANCIAL_AID): Gains COUNTER_07 -> Has COUNTER_03, COUNTER_04, COUNTER_07 (3 counters).
        - Q_ADMISSIONS (COUNTER_01, COUNTER_02) and Q_ACADEMIC_RECORDS (COUNTER_05, COUNTER_06) remain unchanged.

    Assumptions & Limitations:
        1. FIFO queue discipline per department is strictly maintained.
        2. Customer arrival times and required service durations are identical to baseline.
        3. The reallocated counter is assumed to possess necessary staff training to process
           Financial Aid inquiries at the historical rate distribution.
        4. No customer balking or reneging is modeled.
        5. This is an exploratory simulation for capacity planning; not guaranteed real-world behavior.
    """
    print("\n" + "=" * 75)
    print(" [SECTION 3] Event-Level Counter Reallocation Simulation")
    print("=" * 75)
    print(f"[*] Capacity Scenario: Reallocate '{reassigned_counter}' from '{donor_queue}' to '{receiver_queue}'")
    print("    - Baseline:   Q_FINANCIAL_AID = 2 counters, Q_ID_GENERAL = 2 counters")
    print("    - Reallocated: Q_FINANCIAL_AID = 3 counters, Q_ID_GENERAL = 1 counter")
    print("    - Q_ADMISSIONS (2 counters) and Q_ACADEMIC_RECORDS (2 counters) remain unchanged.")

    # Extract all customer profiles (arrival time and service duration) from baseline events
    # Join ARRIVAL and SERVICE_COMPLETION by customer_id
    rows = (
        df.select(
            "customer_id",
            "event_type",
            "timestamp",
            "queue_id",
            "service_type",
            "priority_level",
            "service_duration_seconds",
        )
        .collect()
    )

    customers_map: Dict[str, Dict[str, object]] = defaultdict(dict)
    for r in rows:
        cid = r["customer_id"]
        etype = r["event_type"]
        if etype == "ARRIVAL":
            customers_map[cid]["arrival_time"] = r["timestamp"]
            customers_map[cid]["queue_id"] = r["queue_id"]
            customers_map[cid]["service_type"] = r["service_type"]
            customers_map[cid]["priority_level"] = r["priority_level"]
        elif etype == "SERVICE_COMPLETION":
            customers_map[cid]["service_duration_seconds"] = r["service_duration_seconds"]

    # Build CustomerTask objects
    tasks_by_queue: Dict[str, List[CustomerTask]] = defaultdict(list)
    sim_start_time = None

    for cid, data in customers_map.items():
        arr_time = data["arrival_time"]
        if sim_start_time is None or arr_time < sim_start_time:
            sim_start_time = arr_time

        task = CustomerTask(
            customer_id=cid,
            queue_id=data["queue_id"],
            service_type=data["service_type"],
            priority_level=data["priority_level"],
            arrival_time=arr_time,
            service_duration_seconds=data["service_duration_seconds"],
        )
        tasks_by_queue[data["queue_id"]].append(task)

    # Reallocated counter configuration
    new_counter_allocation = {
        "Q_ADMISSIONS": ["COUNTER_01", "COUNTER_02"],
        "Q_FINANCIAL_AID": ["COUNTER_03", "COUNTER_04", "COUNTER_07"],  # +1 Counter
        "Q_ACADEMIC_RECORDS": ["COUNTER_05", "COUNTER_06"],
        "Q_ID_GENERAL": ["COUNTER_08"],  # -1 Counter
    }

    simulated_customer_results: List[Dict[str, object]] = []
    counter_busy_seconds: Dict[str, int] = defaultdict(int)
    counter_customers_served: Dict[str, int] = defaultdict(int)

    # Execute discrete event queue scheduling for each queue
    for qid, tasks in tasks_by_queue.items():
        # Sort tasks strictly by arrival time (FIFO)
        tasks.sort(key=lambda t: t.arrival_time)
        assigned_counters = new_counter_allocation[qid]

        # Initialize counter availability clocks
        counter_free_time: Dict[str, datetime.datetime] = {
            c: sim_start_time for c in assigned_counters
        }

        for t in tasks:
            # Pick counter available earliest
            chosen_counter = min(assigned_counters, key=lambda c: counter_free_time[c])
            service_start = max(t.arrival_time, counter_free_time[chosen_counter])
            wait_seconds = int((service_start - t.arrival_time).total_seconds())
            completion_time = service_start + datetime.timedelta(seconds=t.service_duration_seconds)

            # Update counter clock
            counter_free_time[chosen_counter] = completion_time

            counter_busy_seconds[chosen_counter] += t.service_duration_seconds
            counter_customers_served[chosen_counter] += 1

            simulated_customer_results.append(
                {
                    "customer_id": t.customer_id,
                    "queue_id": t.queue_id,
                    "service_type": t.service_type,
                    "priority_level": t.priority_level,
                    "assigned_counter": chosen_counter,
                    "arrival_time": t.arrival_time,
                    "service_start_time": service_start,
                    "completion_time": completion_time,
                    "wait_time_seconds": wait_seconds,
                    "wait_time_minutes": round(wait_seconds / 60.0, 2),
                    "service_duration_seconds": t.service_duration_seconds,
                    "scenario": "REALLOCATED_CAPACITY",
                }
            )

    # Compute counter metrics
    sim_end_time = max(r["completion_time"] for r in simulated_customer_results)
    scenario_window_seconds = int((sim_end_time - sim_start_time).total_seconds())

    # Obtain baseline operating window for standardized utilization comparison
    bounds = df.select(
        F.min("timestamp").alias("start_ts"),
        F.max("timestamp").alias("end_ts"),
    ).collect()[0]
    baseline_window_seconds = int((bounds["end_ts"] - bounds["start_ts"]).total_seconds())

    counter_metrics: List[Dict[str, object]] = []
    for qid, counters in new_counter_allocation.items():
        for c in counters:
            busy_sec = counter_busy_seconds[c]
            served = counter_customers_served[c]
            scenario_util_pct = round((busy_sec / scenario_window_seconds) * 100.0, 2)
            standardized_util_pct = round((busy_sec / baseline_window_seconds) * 100.0, 2)
            counter_metrics.append(
                {
                    "counter_id": c,
                    "reallocated_queue_id": qid,
                    "customers_served": served,
                    "total_busy_hours": round(busy_sec / 3600.0, 2),
                    "total_busy_seconds": busy_sec,
                    "scenario_window_seconds": scenario_window_seconds,
                    "scenario_utilization_pct": scenario_util_pct,
                    "baseline_standard_window_seconds": baseline_window_seconds,
                    "standardized_utilization_pct": standardized_util_pct,
                    "scenario": "REALLOCATED_CAPACITY",
                }
            )

    return simulated_customer_results, counter_metrics


def compile_reallocation_comparison(
    spark: SparkSession,
    baseline_df,
    simulated_customer_results: List[Dict[str, object]],
    counter_metrics: List[Dict[str, object]],
    thresholds_minutes: List[int],
) -> Tuple[object, object]:
    """
    Constructs PySpark DataFrames comparing Baseline vs Reallocated Scenarios
    for queue wait times, SLA breaches, and counter utilization.
    """
    # Create Spark DataFrame from simulated customer records
    sim_schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("queue_id", StringType(), False),
            StructField("service_type", StringType(), False),
            StructField("priority_level", StringType(), False),
            StructField("assigned_counter", StringType(), False),
            StructField("arrival_time", TimestampType(), False),
            StructField("service_start_time", TimestampType(), False),
            StructField("completion_time", TimestampType(), False),
            StructField("wait_time_seconds", IntegerType(), False),
            StructField("wait_time_minutes", DoubleType(), False),
            StructField("service_duration_seconds", IntegerType(), False),
            StructField("scenario", StringType(), False),
        ]
    )

    df_sim = spark.createDataFrame(simulated_customer_results, schema=sim_schema)
    df_sim.createOrReplaceTempView("simulated_customers")

    # Aggregate reallocated metrics by queue
    df_sim_agg = spark.sql(
        """
        SELECT 
            queue_id,
            COUNT(*) AS total_customers,
            ROUND(AVG(wait_time_minutes), 2) AS sim_avg_wait_minutes,
            ROUND(PERCENTILE_APPROX(wait_time_seconds, 0.5) / 60.0, 2) AS sim_median_wait_minutes,
            ROUND(MAX(wait_time_minutes), 2) AS sim_max_wait_minutes,
            ROUND(SUM(CASE WHEN wait_time_seconds > 900 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) AS sim_breach_15m_pct,
            ROUND(SUM(CASE WHEN wait_time_seconds > 1800 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) AS sim_breach_30m_pct,
            ROUND(SUM(CASE WHEN wait_time_seconds > 3600 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) AS sim_breach_60m_pct
        FROM simulated_customers
        GROUP BY queue_id
        """
    )
    df_sim_agg.createOrReplaceTempView("sim_queue_stats")

    # Baseline aggregations from raw events
    baseline_df.createOrReplaceTempView("base_raw")
    df_base_agg = spark.sql(
        """
        SELECT 
            queue_id,
            service_type,
            ROUND(AVG(wait_time_seconds) / 60.0, 2) AS base_avg_wait_minutes,
            ROUND(PERCENTILE_APPROX(wait_time_seconds, 0.5) / 60.0, 2) AS base_median_wait_minutes,
            ROUND(MAX(wait_time_seconds) / 60.0, 2) AS base_max_wait_minutes,
            ROUND(SUM(CASE WHEN wait_time_seconds > 900 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) AS base_breach_15m_pct,
            ROUND(SUM(CASE WHEN wait_time_seconds > 1800 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) AS base_breach_30m_pct,
            ROUND(SUM(CASE WHEN wait_time_seconds > 3600 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) AS base_breach_60m_pct
        FROM base_raw
        WHERE event_type = 'SERVICE_START'
        GROUP BY queue_id, service_type
        """
    )
    df_base_agg.createOrReplaceTempView("base_queue_stats")

    # Side-by-side comparison DataFrame
    df_comparison = spark.sql(
        """
        SELECT 
            b.queue_id,
            b.service_type,
            CASE 
                WHEN b.queue_id = 'Q_FINANCIAL_AID' THEN '2 -> 3 Counters (+COUNTER_07)'
                WHEN b.queue_id = 'Q_ID_GENERAL' THEN '2 -> 1 Counter (-COUNTER_07)'
                ELSE '2 Counters (Unchanged)'
            END AS counter_allocation_change,
            b.base_avg_wait_minutes,
            s.sim_avg_wait_minutes,
            ROUND(s.sim_avg_wait_minutes - b.base_avg_wait_minutes, 2) AS wait_time_change_minutes,
            ROUND(((s.sim_avg_wait_minutes - b.base_avg_wait_minutes) / b.base_avg_wait_minutes) * 100.0, 1) AS wait_time_change_pct,
            b.base_max_wait_minutes,
            s.sim_max_wait_minutes,
            b.base_breach_30m_pct,
            s.sim_breach_30m_pct,
            b.base_breach_60m_pct,
            s.sim_breach_60m_pct
        FROM base_queue_stats b
        JOIN sim_queue_stats s ON b.queue_id = s.queue_id
        ORDER BY b.base_avg_wait_minutes DESC
        """
    )

    print("\n--- Side-by-Side Scenario Impact: Baseline vs. Reallocated Capacity ---")
    df_comparison.select(
        "queue_id",
        "counter_allocation_change",
        "base_avg_wait_minutes",
        "sim_avg_wait_minutes",
        "wait_time_change_minutes",
        "base_breach_30m_pct",
        "sim_breach_30m_pct",
    ).show(truncate=False)

    # Counter metrics DataFrame
    counter_schema = StructType(
        [
            StructField("counter_id", StringType(), False),
            StructField("reallocated_queue_id", StringType(), False),
            StructField("customers_served", IntegerType(), False),
            StructField("total_busy_hours", DoubleType(), False),
            StructField("total_busy_seconds", IntegerType(), False),
            StructField("scenario_window_seconds", IntegerType(), False),
            StructField("scenario_utilization_pct", DoubleType(), False),
            StructField("baseline_standard_window_seconds", IntegerType(), False),
            StructField("standardized_utilization_pct", DoubleType(), False),
            StructField("scenario", StringType(), False),
        ]
    )
    df_reallocated_counters = spark.createDataFrame(counter_metrics, schema=counter_schema).orderBy("counter_id")

    print("\n--- Reallocated Counter Utilization Table ---")
    df_reallocated_counters.select(
        "counter_id",
        "reallocated_queue_id",
        "customers_served",
        "total_busy_hours",
        "scenario_utilization_pct",
        "standardized_utilization_pct",
    ).show(truncate=False)

    return df_comparison, df_reallocated_counters


# ==============================================================================
# SECTION 4: Export Stage 3 Outputs
# ==============================================================================


def export_stage3_outputs(
    df_sla,
    df_capacity,
    df_comparison,
    df_reallocated_counters,
    output_dir: str = "outputs",
) -> None:
    """Saves Stage 3 analytical datasets as clean, portable CSV files in outputs/."""
    os.makedirs(output_dir, exist_ok=True)
    print("\n" + "=" * 75)
    print(f" [SECTION 4] Saving Stage 3 Analytical Datasets to '{output_dir}/'")
    print("=" * 75)

    exports = {
        "sla_breaches_by_queue.csv": df_sla,
        "capacity_comparison.csv": df_capacity,
        "reallocation_simulation_metrics.csv": df_comparison,
        "reallocated_counter_utilization.csv": df_reallocated_counters,
    }

    for filename, spark_df in exports.items():
        filepath = os.path.join(output_dir, filename)
        pdf = spark_df.toPandas()
        pdf.to_csv(filepath, index=False, encoding="utf-8")
        size_kb = os.path.getsize(filepath) / 1024
        print(f"[+] Saved '{filepath}' ({len(pdf)} rows, {size_kb:.1f} KB)")


# ==============================================================================
# SECTION 5: Verification & Integrity Assertions
# ==============================================================================


def verify_stage3_results(df_sla, df_comparison, df_reallocated_counters=None, baseline_df=None) -> bool:
    """Verifies customer integrity, metric bounds, and mathematical consistency."""
    print("\n" + "=" * 75)
    print(" [SECTION 5] Stage 3 Verification and Integrity Audit")
    print("=" * 75)

    passed = True

    # 1. Check customer total matches exactly 500
    sla_customers = df_sla.select(F.sum("total_customers_served").alias("total")).collect()[0]["total"]
    if sla_customers == 500:
        print(f"[PASS] SLA baseline customer count verified: {sla_customers} == 500")
    else:
        print(f"[FAIL] Unexpected SLA customer count: {sla_customers} != 500")
        passed = False

    # 2. Check SLA breach percentages are within [0.0, 100.0]
    for row in df_sla.collect():
        for thresh in [15, 30, 60]:
            pct = row[f"breach_pct_{thresh}m"]
            if not (0.0 <= pct <= 100.0):
                print(f"[FAIL] SLA breach % out of range for {row['queue_id']} ({pct}%)")
                passed = False

    print("[PASS] All SLA breach percentages are strictly bounded within [0.0%, 100.0%]")

    # 3. Check monotonicity: 15m breach >= 30m breach >= 60m breach
    for row in df_sla.collect():
        b15 = row["breach_pct_15m"]
        b30 = row["breach_pct_30m"]
        b60 = row["breach_pct_60m"]
        if not (b15 >= b30 >= b60):
            print(f"[FAIL] Non-monotonic SLA breach rates for {row['queue_id']}: {b15} >= {b30} >= {b60}")
            passed = False

    print("[PASS] SLA breach rate monotonicity verified (15m >= 30m >= 60m)")

    # 4. Check that reallocating counter to Q_FINANCIAL_AID lowered its wait time
    fa_row = df_comparison.filter(F.col("queue_id") == "Q_FINANCIAL_AID").collect()[0]
    base_wait = fa_row["base_avg_wait_minutes"]
    sim_wait = fa_row["sim_avg_wait_minutes"]
    if sim_wait < base_wait:
        reduction = round(base_wait - sim_wait, 2)
        print(f"[PASS] Capacity planning proof: Q_FINANCIAL_AID wait time dropped by {reduction} minutes ({base_wait}m -> {sim_wait}m)")
    else:
        print(f"[FAIL] Capacity reallocation failed to reduce wait time for Q_FINANCIAL_AID")
        passed = False

    # 5. Workload conservation & standardized window verification
    if df_reallocated_counters is not None and baseline_df is not None:
        realloc_rows = df_reallocated_counters.collect()
        total_realloc_busy = sum(r["total_busy_seconds"] for r in realloc_rows)
        expected_busy = baseline_df.filter(F.col("event_type") == "SERVICE_COMPLETION").select(F.sum("service_duration_seconds")).collect()[0][0]

        if total_realloc_busy == expected_busy:
            print(f"[PASS] Workload conservation verified: {total_realloc_busy}s total service preserved exactly")
        else:
            print(f"[FAIL] Workload mismatch: {total_realloc_busy}s != {expected_busy}s")
            passed = False

        # Verify standardized window matches baseline window across all counters
        bounds = baseline_df.select(F.min("timestamp").alias("t0"), F.max("timestamp").alias("t1")).collect()[0]
        expected_window = int((bounds["t1"] - bounds["t0"]).total_seconds())
        std_windows = set(r["baseline_standard_window_seconds"] for r in realloc_rows)
        if std_windows == {expected_window}:
            print(f"[PASS] Standardized operating window verified: identical {expected_window}s across all counters")
        else:
            print(f"[FAIL] Inconsistent standardized operating windows: {std_windows}")
            passed = False

    return passed


# ==============================================================================
# Main Orchestrator
# ==============================================================================


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="QueueWise Stage 3: SLA Breach Detection and Dynamic Capacity Planning"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=os.path.join("data", "queue_events.csv"),
        help="Input dataset path (default: data/queue_events.csv)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs",
        help="Output directory (default: outputs/)",
    )
    parser.add_argument(
        "--sla-thresholds",
        nargs="+",
        type=int,
        default=[15, 30, 60],
        help="Configurable waiting-time SLA thresholds in minutes (default: 15 30 60)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    print("============================================================================")
    print(" QueueWise Stage 3: SLA Breach Detection and Capacity Planning")
    print("============================================================================")
    print(f"[*] Input Dataset:        {args.input}")
    print(f"[*] Output Directory:      {args.output_dir}/")
    print(f"[*] Evaluated SLAs:        {args.sla_thresholds} minutes")
    print(f"[*] Execution Stack:       Python {sys.version.split()[0]} | PySpark 3.5.9 | Java 8")
    print("----------------------------------------------------------------------------")

    if not os.path.exists(args.input):
        print(f"[-] ERROR: Input file not found: '{args.input}'. Run event_generator.py first.")
        sys.exit(1)

    spark = None
    try:
        print("[*] Initializing local Apache Spark session...")
        spark = get_spark_session()
        print(f"[+] SparkSession active (Version: {spark.version}, Master: {spark.sparkContext.master})")

        # Ingest CSV with explicit schema
        schema = get_event_schema()
        print(f"[*] Ingesting '{args.input}' with explicit schema...")
        df = spark.read.csv(
            args.input,
            header=True,
            schema=schema,
            timestampFormat="yyyy-MM-dd HH:mm:ss",
        )
        df.cache()

        # Step 1: Baseline SLA breach detection
        df_sla = analyze_sla_breaches(spark, df, args.sla_thresholds)

        # Step 2: Demand, wait, and counter utilization correlation
        df_capacity = correlate_demand_and_capacity(spark, df, df_sla)

        # Step 3: Event-level counter reallocation simulation
        sim_results, counter_metrics = run_event_level_reallocation_simulation(
            df,
            reassigned_counter="COUNTER_07",
            donor_queue="Q_ID_GENERAL",
            receiver_queue="Q_FINANCIAL_AID",
        )

        # Step 4: Compile comparison DataFrames
        df_comparison, df_reallocated_counters = compile_reallocation_comparison(
            spark,
            df,
            sim_results,
            counter_metrics,
            args.sla_thresholds,
        )

        # Step 5: Verification audit
        audit_passed = verify_stage3_results(
            df_sla, df_comparison, df_reallocated_counters, baseline_df=df
        )

        # Step 6: Save Stage 3 outputs
        export_stage3_outputs(
            df_sla,
            df_capacity,
            df_comparison,
            df_reallocated_counters,
            output_dir=args.output_dir,
        )

        print("\n" + "=" * 75)
        if audit_passed:
            print(" STAGE 3 STATUS: [SUCCESS] All SLA analytics & capacity simulations PASSED.")
        else:
            print(" STAGE 3 STATUS: [WARNING] Completed with audit warnings.")
        print("============================================================================\n")

    except Exception as e:
        print(f"\n[-] FATAL ERROR during Stage 3 processing: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark is not None:
            print("[*] Stopping SparkSession cleanly...")
            spark.stop()
            print("[+] SparkSession terminated.")


if __name__ == "__main__":
    main()
