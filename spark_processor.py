"""
QueueWise -- Real-Time Queue Analytics and Capacity Planning Using Apache Spark
Stage 2: Apache Spark Event Ingestion and Queue Analytics

Description:
    Ingests synthetic queue lifecycle events from data/queue_events.csv using
    PySpark 3.5.9. Demonstrates both low-level Resilient Distributed Dataset (RDD)
    transformations/actions and high-level Spark SQL & DataFrames. Computes
    key queuing KPIs (wait times, service durations, arrival rates, counter
    utilization) and exports structured analytics to outputs/.

Academic Notice:
    All analyzed events are synthetically generated for cluster computing
    experiments (is_synthetic=TRUE, data_source=SYNTHETIC_SIMULATION).

Author: QueueWise Team (INT315 Cluster Computing Project)
"""

import os
import sys
from datetime import datetime
from typing import Dict, Tuple

# Configure Windows-specific environment variables for PySpark worker stability
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_spark_session(app_name: str = "QueueWise-Stage2-Analytics") -> SparkSession:
    """
    Initializes and returns an optimized local SparkSession configured for Windows.
    Enforces IPv4 stack and localhost binding to prevent socket timeouts.
    """
    # Note on Architecture: master("local[*]") initializes a local standalone Spark instance
    # utilizing all logical CPU cores on the host workstation (os.cpu_count() = 16 threads).
    # spark.sql.shuffle.partitions is set to 4 to avoid excessive shuffle partitioning on local datasets.
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
    # Set log level to WARN to reduce JVM log clutter
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_event_schema() -> StructType:
    """
    Returns the explicit PySpark schema corresponding to data/queue_events.csv.
    Enforcing explicit schema avoids costly reflection and ensures type safety.
    """
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
# SECTION 1: Resilient Distributed Dataset (RDD) Operations
# ==============================================================================


def demonstrate_rdd_analytics(df) -> Dict[str, object]:
    """
    Demonstrates core Spark RDD transformations and actions:
    - map(): project row attributes into key-value pairs
    - filter(): select rows matching lifecycle criteria
    - distinct(): eliminate duplicate customer/queue pairs
    - reduceByKey(): aggregate counts per partition and merge
    - count(): action returning total records
    - collect(): action pulling distributed results back to driver
    """
    print("\n" + "=" * 70)
    print(" [SECTION 1] PySpark RDD Analytics Demonstration")
    print("=" * 70)

    events_rdd = df.rdd

    # 1. Action: count() -> Total number of events
    # Explanation: Triggers a distributed count action across all RDD partitions.
    total_events = events_rdd.count()
    print(f"\n1. RDD Action: count() -> Total Event Records: {total_events}")

    # 2. Transformation: map() -> distinct() -> Action: count()
    # Explanation: Projects customer_id from each row, computes distinct customer IDs
    # across worker partitions, and counts them.
    distinct_customers = events_rdd.map(lambda r: r.customer_id).distinct().count()
    print(f"2. RDD Transformation: map().distinct().count() -> Unique Customers: {distinct_customers}")

    # 3. Transformation: map() -> reduceByKey() -> Action: collect()
    # Explanation: Emits (event_type, 1) tuples, applies reduceByKey to sum counts
    # locally on partitions before shuffling, minimizing network overhead.
    event_type_counts_rdd = (
        events_rdd.map(lambda r: (r.event_type, 1))
        .reduceByKey(lambda a, b: a + b)
    )
    event_type_counts = dict(event_type_counts_rdd.collect())
    print("\n3. RDD Transformation: map().reduceByKey() -> Events by Type:")
    for etype, cnt in sorted(event_type_counts.items()):
        print(f"   - {etype:<20}: {cnt:>5} records")

    # 4. Transformation: filter() -> map() -> distinct() -> reduceByKey()
    # Explanation: Evaluates distinct customer arrivals per queue. Filters ARRIVAL events,
    # projects (queue_id, customer_id), deduplicates, then counts customers per queue.
    customers_per_queue_rdd = (
        events_rdd.filter(lambda r: r.event_type == "ARRIVAL")
        .map(lambda r: (r.queue_id, r.customer_id))
        .distinct()
        .map(lambda pair: (pair[0], 1))
        .reduceByKey(lambda a, b: a + b)
    )
    customers_per_queue = dict(customers_per_queue_rdd.collect())
    print("\n4. RDD Pipeline: filter(ARRIVAL).distinct().reduceByKey() -> Customers per Queue:")
    for qid, cnt in sorted(customers_per_queue.items()):
        print(f"   - {qid:<20}: {cnt:>5} customers")

    # 5. Transformation: mapPartitions() -> Physical Partition Distribution
    # Explanation: Demonstrates worker partition boundary processing. Evaluates record
    # count per partition without moving data across network or shuffle partitions.
    num_partitions = events_rdd.getNumPartitions()
    partition_counts_rdd = events_rdd.mapPartitions(lambda it: [sum(1 for _ in it)])
    partition_distribution = partition_counts_rdd.collect()
    print(f"\n5. RDD Transformation: mapPartitions() -> Distribution across {num_partitions} Partitions:")
    for p_idx, count_in_p in enumerate(partition_distribution):
        print(f"   - Worker Partition {p_idx}: {count_in_p} records")

    return {
        "total_events": total_events,
        "distinct_customers": distinct_customers,
        "event_type_counts": event_type_counts,
        "customers_per_queue": customers_per_queue,
        "num_partitions": num_partitions,
        "partition_distribution": partition_distribution,
    }


# ==============================================================================
# SECTION 2: Spark SQL & DataFrame Analytics
# ==============================================================================


def demonstrate_dataframe_and_sql(spark: SparkSession, df) -> Dict[str, object]:
    """
    Executes relational queries using Spark SQL and DataFrame API.
    Calculates queue-level wait times, service durations, and hourly arrival rates.
    """
    print("\n" + "=" * 70)
    print(" [SECTION 2] Spark SQL & DataFrame Analytics")
    print("=" * 70)

    # Register DataFrame as temporary in-memory SQL table
    df.createOrReplaceTempView("queue_events")

    # --------------------------------------------------------------------------
    # Query 1: Event Counts by Event Type
    # --------------------------------------------------------------------------
    print("\n--- Spark SQL Query 1: Event Counts by Type ---")
    df_event_counts = spark.sql(
        """
        SELECT 
            event_type,
            COUNT(*) AS event_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total
        FROM queue_events
        GROUP BY event_type
        ORDER BY event_count DESC
        """
    )
    df_event_counts.show(truncate=False)

    # --------------------------------------------------------------------------
    # Query 2: Customer Counts by Queue (Using ARRIVAL events)
    # --------------------------------------------------------------------------
    print("\n--- Spark SQL Query 2: Customer Counts by Queue ---")
    df_cust_by_queue = spark.sql(
        """
        SELECT 
            queue_id,
            service_type,
            COUNT(DISTINCT customer_id) AS total_customers,
            ROUND(COUNT(DISTINCT customer_id) * 100.0 / 
                  (SELECT COUNT(DISTINCT customer_id) FROM queue_events), 2) AS share_pct
        FROM queue_events
        WHERE event_type = 'ARRIVAL'
        GROUP BY queue_id, service_type
        ORDER BY total_customers DESC
        """
    )
    df_cust_by_queue.show(truncate=False)

    # --------------------------------------------------------------------------
    # Query 3: Queue Metrics (Wait Time & Service Duration)
    # Note: Wait time is measured at SERVICE_START; service duration at SERVICE_COMPLETION.
    # We join both lifecycle views on queue_id.
    # --------------------------------------------------------------------------
    print("\n--- Spark SQL Query 3: Queue Wait & Service Performance Metrics ---")
    df_queue_metrics = spark.sql(
        """
        WITH wait_stats AS (
            SELECT 
                queue_id,
                service_type,
                COUNT(DISTINCT customer_id) AS serviced_customers,
                ROUND(AVG(wait_time_seconds) / 60.0, 2) AS avg_wait_minutes,
                ROUND(MAX(wait_time_seconds) / 60.0, 2) AS max_wait_minutes,
                ROUND(PERCENTILE_APPROX(wait_time_seconds, 0.5) / 60.0, 2) AS median_wait_minutes
            FROM queue_events
            WHERE event_type = 'SERVICE_START'
            GROUP BY queue_id, service_type
        ),
        service_stats AS (
            SELECT 
                queue_id,
                ROUND(AVG(service_duration_seconds) / 60.0, 2) AS avg_service_minutes,
                ROUND(MAX(service_duration_seconds) / 60.0, 2) AS max_service_minutes
            FROM queue_events
            WHERE event_type = 'SERVICE_COMPLETION'
            GROUP BY queue_id
        )
        SELECT 
            w.queue_id,
            w.service_type,
            w.serviced_customers,
            w.avg_wait_minutes,
            w.median_wait_minutes,
            w.max_wait_minutes,
            s.avg_service_minutes,
            s.max_service_minutes,
            ROUND(w.avg_wait_minutes + s.avg_service_minutes, 2) AS avg_total_turnaround_minutes
        FROM wait_stats w
        JOIN service_stats s ON w.queue_id = s.queue_id
        ORDER BY w.avg_wait_minutes DESC
        """
    )
    df_queue_metrics.show(truncate=False)

    print("\n--- Spark SQL Catalyst Physical Execution Plan (explain()) ---")
    df_queue_metrics.explain(extended=False)

    # --------------------------------------------------------------------------
    # Query 4: Hourly Arrival Volume (Demand Profiling)
    # --------------------------------------------------------------------------
    print("\n--- Spark SQL Query 4: Hourly Arrival Distribution ---")
    df_hourly_arrivals = spark.sql(
        """
        SELECT 
            DATE_FORMAT(timestamp, 'yyyy-MM-dd HH:00:00') AS arrival_hour,
            HOUR(timestamp) AS hour_of_day,
            COUNT(*) AS arrival_count
        FROM queue_events
        WHERE event_type = 'ARRIVAL'
        GROUP BY arrival_hour, hour_of_day
        ORDER BY arrival_hour ASC
        """
    )
    df_hourly_arrivals.show(truncate=False)

    # --------------------------------------------------------------------------
    # Query 5: Priority Level Analysis (Standard vs Priority Service)
    # --------------------------------------------------------------------------
    print("\n--- Spark SQL Query 5: Priority Level Comparison ---")
    df_priority_metrics = spark.sql(
        """
        SELECT 
            priority_level,
            COUNT(DISTINCT customer_id) AS customer_count,
            ROUND(AVG(wait_time_seconds) / 60.0, 2) AS avg_wait_minutes,
            ROUND(MAX(wait_time_seconds) / 60.0, 2) AS max_wait_minutes,
            ROUND(AVG(service_duration_seconds) / 60.0, 2) AS avg_service_minutes
        FROM queue_events
        WHERE event_type = 'SERVICE_COMPLETION'
        GROUP BY priority_level
        ORDER BY avg_wait_minutes DESC
        """
    )
    df_priority_metrics.show(truncate=False)

    return {
        "event_counts": df_event_counts,
        "customers_by_queue": df_cust_by_queue,
        "queue_metrics": df_queue_metrics,
        "hourly_arrivals": df_hourly_arrivals,
        "priority_metrics": df_priority_metrics,
    }


# ==============================================================================
# SECTION 3: Counter Utilization Analysis
# ==============================================================================


def calculate_counter_utilization(spark: SparkSession, df) -> object:
    """
    Computes physical counter utilization with a defensible queuing formula:

    Formula:
        Utilization (%) = (Total Cumulative Service Seconds / Total Facility Operating Seconds) * 100

    Definitions:
        - Total Service Seconds (Busy Time): Sum of service_duration_seconds on that counter
          from SERVICE_COMPLETION events.
        - Operating Window (Simulated Window): Time difference between the first arrival
          in the simulation and the final completion across all counters.
        - Waiting time is strictly EXCLUDED from utilization, as waiting occurs in queue
          lines before the customer is seated at the counter.

    Assumptions:
        - 1 counter serves 1 customer at a time (verified in Stage 1).
        - Counters are staffed and operational throughout the simulation window.
    """
    print("\n" + "=" * 70)
    print(" [SECTION 3] Counter Utilization Analysis")
    print("=" * 70)

    # Compute overall facility simulation window
    sim_bounds = df.select(
        F.min("timestamp").alias("first_event"),
        F.max("timestamp").alias("last_event"),
    ).collect()[0]

    sim_start = sim_bounds["first_event"]
    sim_end = sim_bounds["last_event"]
    total_window_seconds = int((sim_end - sim_start).total_seconds())

    print(f"[*] Simulation Operating Window: {sim_start} to {sim_end}")
    print(f"[*] Total Window Duration:       {total_window_seconds} seconds ({total_window_seconds / 3600.0:.2f} hours)")

    # Aggregate busy time per counter from SERVICE_COMPLETION events
    df_counter_util = (
        df.filter(F.col("event_type") == "SERVICE_COMPLETION")
        .groupBy("counter_id", "queue_id")
        .agg(
            F.count("customer_id").alias("customers_served"),
            F.sum("service_duration_seconds").alias("total_busy_seconds"),
            F.round(F.avg("service_duration_seconds") / 60.0, 2).alias("avg_service_minutes"),
            F.min("timestamp").alias("first_service_end"),
            F.max("timestamp").alias("last_service_end"),
        )
        .withColumn(
            "total_busy_hours",
            F.round(F.col("total_busy_seconds") / 3600.0, 2),
        )
        .withColumn(
            "facility_window_seconds",
            F.lit(total_window_seconds),
        )
        .withColumn(
            "utilization_pct",
            F.round(
                (F.col("total_busy_seconds") / F.lit(total_window_seconds)) * 100.0,
                2,
            ),
        )
        .orderBy("counter_id")
    )

    print("\n--- Counter Utilization Summary Table ---")
    df_counter_util.select(
        "counter_id",
        "queue_id",
        "customers_served",
        "total_busy_hours",
        "avg_service_minutes",
        "utilization_pct",
    ).show(truncate=False)

    return df_counter_util


# ==============================================================================
# SECTION 4: Export Analytics Outputs
# ==============================================================================


def export_analytics_outputs(results: Dict[str, object], output_dir: str = "outputs") -> None:
    """
    Saves computed analytical DataFrames as clean, standard CSV files in outputs/.
    Uses .toPandas().to_csv() to write atomic single-file CSVs rather than
    Hadoop partitioned directories (part-00000...), ensuring seamless portability on Windows.
    """
    os.makedirs(output_dir, exist_ok=True)
    print("\n" + "=" * 70)
    print(f" [SECTION 4] Saving Analytical Datasets to '{output_dir}/'")
    print("=" * 70)

    export_map = {
        "event_counts.csv": results["event_counts"],
        "customer_counts_by_queue.csv": results["customers_by_queue"],
        "queue_metrics.csv": results["queue_metrics"],
        "hourly_arrivals.csv": results["hourly_arrivals"],
        "counter_utilization.csv": results["counter_utilization"],
    }

    for filename, spark_df in export_map.items():
        filepath = os.path.join(output_dir, filename)
        # Convert Spark DataFrame to Pandas for clean single-file CSV export
        pdf = spark_df.toPandas()
        pdf.to_csv(filepath, index=False, encoding="utf-8")
        size_kb = os.path.getsize(filepath) / 1024
        print(f"[+] Saved '{filepath}' ({len(pdf)} rows, {size_kb:.1f} KB)")


# ==============================================================================
# SECTION 5: Verification & Integrity Assertions
# ==============================================================================


def verify_analytics_against_source(df, rdd_results: Dict[str, object]) -> bool:
    """
    Verifies that the ingested Spark dataset strictly matches the expected
    input parameters from Stage 1:
    - Exactly 1,500 total events
    - Exactly 500 unique customers
    - Zero data leakage or dropped records
    """
    print("\n" + "=" * 70)
    print(" [SECTION 5] Verification and Integrity Audit")
    print("=" * 70)

    total_events = rdd_results["total_events"]
    distinct_customers = rdd_results["distinct_customers"]

    passed = True

    # 1. Total event count assertion
    if total_events == 1500:
        print(f"[PASS] Total event count verified: {total_events} == 1500")
    else:
        print(f"[FAIL] Unexpected event count: {total_events} != 1500")
        passed = False

    # 2. Distinct customer assertion
    if distinct_customers == 500:
        print(f"[PASS] Distinct customer count verified: {distinct_customers} == 500")
    else:
        print(f"[FAIL] Unexpected customer count: {distinct_customers} != 500")
        passed = False

    # 3. Synthetic label audit
    non_synthetic = df.filter(F.col("is_synthetic") != True).count()
    if non_synthetic == 0:
        print("[PASS] Synthetic labeling verified: 100% records marked is_synthetic=TRUE")
    else:
        print(f"[FAIL] Data purity violation: {non_synthetic} records are not labeled synthetic!")
        passed = False

    return passed


# ==============================================================================
# Main Orchestrator
# ==============================================================================


def main() -> None:
    input_path = os.path.join("data", "queue_events.csv")
    output_dir = "outputs"

    print("========================================================================")
    print(" QueueWise Stage 2: Apache Spark Event Ingestion and Queue Analytics")
    print("========================================================================")
    print(f"[*] Input Dataset:   {input_path}")
    print(f"[*] Output Directory: {output_dir}/")
    print(f"[*] Execution Stack:  Python {sys.version.split()[0]} | PySpark 3.5.9 | Java 8")
    print("------------------------------------------------------------------------")

    # Validate input file existence
    if not os.path.exists(input_path):
        print(f"[-] ERROR: Input file not found: '{input_path}'.")
        print("    Please run Stage 1 generator first: python event_generator.py")
        sys.exit(1)

    spark = None
    try:
        print("[*] Initializing local Apache Spark session...")
        spark = get_spark_session()
        print(f"[+] SparkSession active (Version: {spark.version}, Master: {spark.sparkContext.master})")

        # Ingest CSV with explicit schema
        schema = get_event_schema()
        print(f"[*] Ingesting '{input_path}' with explicit schema...")
        df = spark.read.csv(
            input_path,
            header=True,
            schema=schema,
            timestampFormat="yyyy-MM-dd HH:mm:ss",
        )

        # Cache DataFrame to optimize multiple downstream SQL and RDD queries
        df.cache()

        # Step 1: Demonstrate RDD operations
        rdd_results = demonstrate_rdd_analytics(df)

        # Step 2: Demonstrate Spark SQL & DataFrames
        df_results = demonstrate_dataframe_and_sql(spark, df)

        # Step 3: Counter utilization calculation
        counter_util = calculate_counter_utilization(spark, df)
        df_results["counter_utilization"] = counter_util

        # Step 4: Verification against source expectations
        audit_passed = verify_analytics_against_source(df, rdd_results)

        # Step 5: Save analytical outputs
        export_analytics_outputs(df_results, output_dir=output_dir)

        print("\n" + "=" * 70)
        if audit_passed:
            print(" STAGE 2 STATUS: [SUCCESS] All Spark analytics & verifications PASSED.")
        else:
            print(" STAGE 2 STATUS: [WARNING] Analytics completed with validation warnings.")
        print("========================================================================\n")

    except Exception as e:
        print(f"\n[-] FATAL ERROR during Spark processing: {e}", file=sys.stderr)
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
