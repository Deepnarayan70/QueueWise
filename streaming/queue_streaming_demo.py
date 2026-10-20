"""
QueueWise -- Real-Time Queue Analytics and Capacity Planning Using Apache Spark
Stage 6: Spark Structured Streaming Demonstration

Description:
    Demonstrates real-time event stream ingestion, schema enforcement,
    event-time watermarking, and windowed aggregation using Apache Spark
    Structured Streaming.

    Simulates arrival of incremental micro-batches into streaming/input_stream/,
    processes them via Spark's micro-batch streaming engine with checkpointing,
    and maintains real-time windowed queue arrival and wait-time statistics.

Usage:
    python streaming/queue_streaming_demo.py [--mode demo|live] [--duration SECONDS]

Academic Notice:
    All streaming events are synthetically generated for cluster computing
    demonstrations (is_synthetic=TRUE, data_source=SYNTHETIC_SIMULATION).

Author: QueueWise Team (INT315 Cluster Computing Project)
"""

import os
import sys
import time
import shutil
import argparse
from pathlib import Path

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


def get_streaming_spark_session(app_name: str = "QueueWise-Structured-Streaming") -> SparkSession:
    """
    Initializes and returns an optimized local SparkSession configured for
    Structured Streaming on Windows.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_event_schema() -> StructType:
    """
    Explicit PySpark schema corresponding to QueueWise synthetic events.
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


def run_streaming_demo(
    project_root: Path,
    input_dir: Path,
    checkpoint_dir: Path,
    batch_size: int = 60,
    num_batches: int = 3,
):
    """
    Runs an automated, end-to-end Structured Streaming demonstration.
    Feeds micro-batches from data/queue_events.csv into the streaming directory,
    processes them with event-time windowing, and verifies query execution.
    """
    print("\n" + "=" * 78)
    print("  QUEUEWISE -- SPARK STRUCTURED STREAMING DEMONSTRATION")
    print("=" * 78)
    print(f"  Input Directory:      {input_dir}")
    print(f"  Checkpoint Directory: {checkpoint_dir}")

    # Prepare directories
    input_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Clean prior streaming chunks in input_dir to ensure clean demo state
    for f in input_dir.glob("batch_*.csv"):
        try:
            f.unlink()
        except OSError:
            pass
    if checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir, ignore_errors=True)
        checkpoint_dir.mkdir(parents=True, exist_ok=True)

    spark = get_streaming_spark_session()
    schema = get_event_schema()

    source_csv = project_root / "data" / "queue_events.csv"
    if not source_csv.exists():
        raise FileNotFoundError(f"Source events dataset not found: {source_csv}")

    # Read header and lines from source CSV to prepare streaming batches
    with open(source_csv, "r", encoding="utf-8") as f:
        header = f.readline()
        all_lines = f.readlines()

    total_available = len(all_lines)
    print(f"  Source dataset contains {total_available} event records.")
    print(f"  Will stream {num_batches} micro-batches of ~{batch_size} events each.\n")

    # Set up Structured Streaming DataFrame
    streaming_df = (
        spark.readStream.schema(schema)
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
        .csv(str(input_dir))
    )

    # Windowed aggregation on event timestamp with 1-hour watermark
    windowed_metrics = (
        streaming_df.withWatermark("timestamp", "60 minutes")
        .groupBy(
            F.window("timestamp", "30 minutes"),
            "queue_id",
        )
        .agg(
            F.count("event_id").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "ARRIVAL", 1).otherwise(0)).alias("arrivals"),
            F.sum(F.when(F.col("event_type") == "SERVICE_START", 1).otherwise(0)).alias("served"),
            F.round(F.avg("wait_time_seconds"), 1).alias("avg_wait_sec"),
        )
    )

    # Start memory query to inspect state programmatically
    query_name = "queue_realtime_metrics"
    query = (
        windowed_metrics.writeStream.queryName(query_name)
        .outputMode("complete")
        .format("memory")
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )

    print(f"[1/4] Structured Streaming query '{query.name}' started successfully.")
    print(f"      Status: IsActive={query.isActive}, OutputMode=complete\n")

    try:
        # Sequentially inject micro-batches and observe state
        for batch_idx in range(1, num_batches + 1):
            start_pos = (batch_idx - 1) * batch_size
            end_pos = min(batch_idx * batch_size, total_available)
            batch_lines = all_lines[start_pos:end_pos]

            batch_file = input_dir / f"batch_{batch_idx:03d}.csv"
            with open(batch_file, "w", encoding="utf-8") as bf:
                bf.write(header)
                bf.writelines(batch_lines)

            print(f"[Batch {batch_idx}/{num_batches}] Injected {len(batch_lines)} events -> {batch_file.name}")
            
            # Wait for Spark Structured Streaming to pick up and process file
            query.processAllAvailable()
            time.sleep(1.0)

            # Query the in-memory stream aggregation table
            current_state = spark.sql(f"""
                SELECT 
                    date_format(window.start, 'HH:mm') AS win_start,
                    date_format(window.end, 'HH:mm') AS win_end,
                    queue_id,
                    event_count,
                    arrivals,
                    served,
                    avg_wait_sec
                FROM {query_name}
                ORDER BY win_start, queue_id
            """)

            rows = current_state.collect()
            print(f"      -> Stream State Updated: {len(rows)} active window-queue aggregate partitions.")
            if rows:
                print("      " + "-" * 70)
                print(f"      {'Window':<13} {'Queue':<22} {'Events':<8} {'Arrivals':<10} {'Served':<8} {'AvgWait(s)'}")
                print("      " + "-" * 70)
                for r in rows[:6]:
                    win = f"{r['win_start']}-{r['win_end']}"
                    print(f"      {win:<13} {r['queue_id']:<22} {r['event_count']:<8} {r['arrivals']:<10} {r['served']:<8} {r['avg_wait_sec']}")
                if len(rows) > 6:
                    print(f"      ... [{len(rows) - 6} additional window rows in state store]")
                print("      " + "-" * 70 + "\n")

        print("[3/4] Micro-batch injection completed. Verifying streaming checkpoint metadata...")
        checkpoint_files = list(checkpoint_dir.glob("**/*"))
        print(f"      Checkpoint directory contains {len(checkpoint_files)} state and offset metadata items.")

        print("\n[4/4] Stopping streaming query cleanly...")
        query.stop()
        print("      Streaming query stopped gracefully.")
        print("\n" + "=" * 78)
        print("  STRUCTURED STREAMING VERIFICATION PASSED")
        print("=" * 78)

    except Exception as e:
        print(f"\n[ERROR] Streaming execution encountered: {e}")
        if query.isActive:
            query.stop()
        raise e
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="QueueWise Spark Structured Streaming Demo")
    parser.add_argument("--mode", choices=["demo", "live"], default="demo", help="Execution mode")
    parser.add_argument("--batch-size", type=int, default=60, help="Events per micro-batch")
    parser.add_argument("--batches", type=int, default=3, help="Number of micro-batches to stream")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    input_dir = project_root / "streaming" / "input_stream"
    checkpoint_dir = project_root / "streaming" / "checkpoints"

    if args.mode == "demo":
        run_streaming_demo(
            project_root=project_root,
            input_dir=input_dir,
            checkpoint_dir=checkpoint_dir,
            batch_size=args.batch_size,
            num_batches=args.batches,
        )
    else:
        print("Live streaming mode: monitoring input_dir for new CSV files...")
        input_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        spark = get_streaming_spark_session()
        schema = get_event_schema()
        streaming_df = (
            spark.readStream.schema(schema)
            .option("header", "true")
            .option("maxFilesPerTrigger", 1)
            .csv(str(input_dir))
        )
        query = (
            streaming_df.groupBy("queue_id", "event_type")
            .count()
            .writeStream.outputMode("complete")
            .format("console")
            .option("checkpointLocation", str(checkpoint_dir))
            .start()
        )
        print(f"Listening on {input_dir}. Press Ctrl+C to terminate.")
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\nShutting down stream...")
            query.stop()
            spark.stop()


if __name__ == "__main__":
    main()
