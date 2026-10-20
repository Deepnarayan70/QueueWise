"""
QueueWise — Spark MLlib Long-Wait Prediction Pipeline
INT315 Cluster Computing Project

Module: ml/queue_wait_prediction.py

Description:
    Trains and evaluates a genuine PySpark MLlib classification pipeline to predict
    whether a simulated customer arrival will experience a long waiting time
    (wait_time_seconds > 1800, i.e., > 30 minutes).

Leakage Prevention:
    - Features are strictly limited to arrival-time observable attributes:
      queue_id, priority_level, arrival_hour, arrival_minute_of_day, is_rush_hour,
      and prior arrival load (queue_arrivals_prior_30m, total_arrivals_prior_30m).
    - Actual wait_time_seconds, service timestamps, durations, and customer IDs
      are completely excluded from the predictor feature set.

Synthetic Data Disclosure:
    - All training data is generated from a synthetic discrete-event simulation
      for INT315 Cluster Computing and does not represent actual university operations.
"""

import os
import sys
import json
from typing import Dict, Any, Tuple

# Configure Windows-specific environment variables for PySpark worker stability
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DoubleType,
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# ==============================================================================
# Configuration & Constants
# ==============================================================================

WORKSPACE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(WORKSPACE_DIR, "data", "queue_events.csv")
OUTPUTS_DIR = os.path.join(WORKSPACE_DIR, "outputs")
MODELS_DIR = os.path.join(WORKSPACE_DIR, "models")
MODEL_SAVE_PATH = os.path.join(MODELS_DIR, "spark_mllib_wait_predictor")

RANDOM_SEED = 42
LONG_WAIT_THRESHOLD_SECONDS = 1800  # 30 minutes benchmark


def get_spark_session(app_name: str = "QueueWise-MLlib-Prediction") -> SparkSession:
    """Initializes a local PySpark session configured for Windows."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract_customer_arrival_dataset(spark: SparkSession, csv_path: str):
    """
    Constructs a clean, non-leaking customer-level dataset from lifecycle events.
    Each customer is represented by exactly one row with arrival-time features
    and a binary target label (long_wait: 1 if wait > 1800s else 0).
    """
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("queue_id", StringType(), False),
        StructField("counter_id", StringType(), True),
        StructField("service_type", StringType(), False),
        StructField("priority_level", StringType(), False),
        StructField("wait_time_seconds", IntegerType(), True),
        StructField("service_duration_seconds", IntegerType(), True),
        StructField("is_synthetic", StringType(), False),
        StructField("data_source", StringType(), False),
    ])

    df_raw = spark.read.option("header", "true").schema(schema).csv(csv_path)

    # 1. Extract ARRIVAL events (observable features at entry)
    df_arr = df_raw.filter(F.col("event_type") == "ARRIVAL").select(
        F.col("customer_id"),
        F.col("timestamp").alias("arrival_timestamp"),
        F.col("queue_id"),
        F.col("priority_level"),
    )

    # 2. Extract SERVICE_START events (used strictly to construct the target label)
    df_start = df_raw.filter(F.col("event_type") == "SERVICE_START").select(
        F.col("customer_id"),
        F.col("wait_time_seconds"),
    )

    # 3. Join customer arrival with their eventual wait time
    df_joined = df_arr.join(df_start, on="customer_id", how="inner")

    # 4. Feature Engineering (Strictly past-observable & arrival-time only)
    # Target: 1.0 if wait_time_seconds > 1800 (30m) else 0.0
    df_featured = (
        df_joined.withColumn(
            "label",
            F.when(F.col("wait_time_seconds") > LONG_WAIT_THRESHOLD_SECONDS, 1.0).otherwise(0.0)
        )
        .withColumn("ts_epoch", F.col("arrival_timestamp").cast("long"))
        .withColumn("arrival_hour", F.hour(F.col("arrival_timestamp")).cast(DoubleType()))
        .withColumn("arrival_minute", F.minute(F.col("arrival_timestamp")).cast(DoubleType()))
        .withColumn(
            "arrival_minute_of_day",
            (F.col("arrival_hour") * 60.0 + F.col("arrival_minute")).cast(DoubleType())
        )
        .withColumn(
            "is_rush_hour",
            F.when(F.col("arrival_hour").isin([11.0, 12.0]), 1.0).otherwise(0.0)
        )
    )

    # Compute prior arrival density strictly using preceding events in (-1800s, -1s)
    w_queue = (
        Window.partitionBy("queue_id")
        .orderBy("ts_epoch")
        .rangeBetween(-1800, -1)
    )
    w_total = (
        Window.orderBy("ts_epoch")
        .rangeBetween(-1800, -1)
    )

    df_final = (
        df_featured.withColumn(
            "queue_arrivals_prior_30m",
            F.count("customer_id").over(w_queue).cast(DoubleType())
        )
        .withColumn(
            "total_arrivals_prior_30m",
            F.count("customer_id").over(w_total).cast(DoubleType())
        )
        .select(
            "customer_id",
            "queue_id",
            "priority_level",
            "arrival_hour",
            "arrival_minute_of_day",
            "is_rush_hour",
            "queue_arrivals_prior_30m",
            "total_arrivals_prior_30m",
            "label",
        )
    )

    return df_final


def build_and_train_mllib_pipeline(df_data):
    """
    Constructs a genuine Spark MLlib Pipeline with StringIndexer, OneHotEncoder,
    VectorAssembler, and classifiers. Evaluates on an 80/20 train/test split.
    """
    print("\n" + "=" * 70)
    print(" [STAGE 1] Dataset Summary & Class Balance")
    print("=" * 70)

    total_count = df_data.count()
    pos_count = df_data.filter(F.col("label") == 1.0).count()
    neg_count = df_data.filter(F.col("label") == 0.0).count()
    pos_pct = (pos_count / total_count) * 100.0
    neg_pct = (neg_count / total_count) * 100.0

    print(f"Total Customer Observations: {total_count}")
    print(f"Class 0 (Normal Wait <= 30 min): {neg_count} ({neg_pct:.1f}%)")
    print(f"Class 1 (Long Wait > 30 min):   {pos_count} ({pos_pct:.1f}%)")

    # 80/20 Train / Test Split
    train_df, test_df = df_data.randomSplit([0.8, 0.2], seed=RANDOM_SEED)
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"\nTrain Set: {train_count} rows | Test Set: {test_count} rows (Seed={RANDOM_SEED})")

    # Pipeline stages: StringIndexer -> OneHotEncoder -> VectorAssembler
    indexer_queue = StringIndexer(
        inputCol="queue_id", outputCol="queue_index", handleInvalid="keep"
    )
    indexer_priority = StringIndexer(
        inputCol="priority_level", outputCol="priority_index", handleInvalid="keep"
    )

    encoder = OneHotEncoder(
        inputCols=["queue_index", "priority_index"],
        outputCols=["queue_vec", "priority_vec"],
        dropLast=False,
    )

    assembler = VectorAssembler(
        inputCols=[
            "queue_vec",
            "priority_vec",
            "arrival_hour",
            "arrival_minute_of_day",
            "is_rush_hour",
            "queue_arrivals_prior_30m",
            "total_arrivals_prior_30m",
        ],
        outputCol="features",
    )

    # 1. Model A: Logistic Regression (Interpretable baseline)
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=20,
        regParam=0.01,
        elasticNetParam=0.0,
    )

    pipeline_lr = Pipeline(stages=[indexer_queue, indexer_priority, encoder, assembler, lr])
    print("\n[*] Training Model A: Spark MLlib LogisticRegression...")
    model_lr = pipeline_lr.fit(train_df)
    pred_lr = model_lr.transform(test_df)

    # 2. Model B: Random Forest Classifier (Non-linear ensembles)
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=30,
        maxDepth=5,
        seed=RANDOM_SEED,
    )

    pipeline_rf = Pipeline(stages=[indexer_queue, indexer_priority, encoder, assembler, rf])
    print("[*] Training Model B: Spark MLlib RandomForestClassifier...")
    model_rf = pipeline_rf.fit(train_df)
    pred_rf = model_rf.transform(test_df)

    # ==============================================================================
    # Evaluation
    # ==============================================================================
    print("\n" + "=" * 70)
    print(" [STAGE 2] Test-Set Evaluation & Performance Comparison")
    print("=" * 70)

    eval_acc = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    eval_f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    eval_precision = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedPrecision")
    eval_recall = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedRecall")
    eval_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    eval_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")

    def evaluate_predictions(predictions, model_name: str) -> Dict[str, Any]:
        acc = eval_acc.evaluate(predictions)
        f1 = eval_f1.evaluate(predictions)
        prec = eval_precision.evaluate(predictions)
        rec = eval_recall.evaluate(predictions)
        auc = eval_auc.evaluate(predictions)
        pr = eval_pr.evaluate(predictions)

        # Confusion Matrix
        tp = predictions.filter("label = 1.0 AND prediction = 1.0").count()
        fp = predictions.filter("label = 0.0 AND prediction = 1.0").count()
        tn = predictions.filter("label = 0.0 AND prediction = 0.0").count()
        fn = predictions.filter("label = 1.0 AND prediction = 0.0").count()

        print(f"\n--- {model_name} Evaluation Metrics ---")
        print(f"  Accuracy:                 {acc * 100:.2f}%")
        print(f"  F1-Score:                 {f1:.4f}")
        print(f"  Weighted Precision:       {prec:.4f}")
        print(f"  Weighted Recall:          {rec:.4f}")
        print(f"  Area Under ROC (AUC):     {auc:.4f}")
        print(f"  Area Under PR Curve:      {pr:.4f}")
        print(f"  Confusion Matrix:         TP={tp}, FP={fp}, TN={tn}, FN={fn}")

        return {
            "model_name": model_name,
            "accuracy_pct": round(acc * 100, 2),
            "f1_score": round(f1, 4),
            "precision": round(prec, 4),
            "recall": round(rec, 4),
            "auc_roc": round(auc, 4),
            "auc_pr": round(pr, 4),
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
            "test_sample_count": predictions.count(),
        }

    res_lr = evaluate_predictions(pred_lr, "LogisticRegression")
    res_rf = evaluate_predictions(pred_rf, "RandomForestClassifier")

    # Select champion model based on F1-Score
    best_model = model_rf if res_rf["f1_score"] >= res_lr["f1_score"] else model_lr
    best_results = res_rf if res_rf["f1_score"] >= res_lr["f1_score"] else res_lr
    print(f"\n[+] Champion Model Selected: {best_results['model_name']} (F1={best_results['f1_score']})")

    # Save trained champion model
    os.makedirs(MODELS_DIR, exist_ok=True)
    if os.path.exists(MODEL_SAVE_PATH):
        import shutil
        shutil.rmtree(MODEL_SAVE_PATH)
    best_model.save(MODEL_SAVE_PATH)
    print(f"[+] Saved Champion PipelineModel to: {MODEL_SAVE_PATH}")

    # ==============================================================================
    # Queue-Level Long-Wait Risk Profiling
    # ==============================================================================
    all_preds = best_model.transform(df_data)
    queue_risk_df = (
        all_preds.groupBy("queue_id")
        .agg(
            F.count("customer_id").alias("total_arrivals"),
            F.sum(F.when(F.col("prediction") == 1.0, 1).otherwise(0)).alias("predicted_long_waits"),
            F.sum(F.when(F.col("label") == 1.0, 1).otherwise(0)).alias("actual_long_waits"),
        )
        .withColumn(
            "predicted_long_wait_risk_pct",
            F.round(F.col("predicted_long_waits") / F.col("total_arrivals") * 100.0, 2)
        )
        .withColumn(
            "actual_long_wait_pct",
            F.round(F.col("actual_long_waits") / F.col("total_arrivals") * 100.0, 2)
        )
        .withColumn(
            "risk_category",
            F.when(F.col("predicted_long_wait_risk_pct") >= 75.0, "CRITICAL_RISK")
            .when(F.col("predicted_long_wait_risk_pct") >= 50.0, "HIGH_RISK")
            .when(F.col("predicted_long_wait_risk_pct") >= 25.0, "MODERATE_RISK")
            .otherwise("LOW_RISK")
        )
        .orderBy(F.desc("predicted_long_wait_risk_pct"))
    )

    print("\n" + "=" * 70)
    print(" [STAGE 3] Queue-Level Predicted Long-Wait Risk Scores")
    print("=" * 70)
    queue_risk_df.show(truncate=False)

    # Save analytical outputs to outputs/
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    
    # 1. queue_long_wait_risk.csv
    risk_csv_path = os.path.join(OUTPUTS_DIR, "queue_long_wait_risk.csv")
    queue_risk_df.toPandas().to_csv(risk_csv_path, index=False)
    print(f"[+] Saved queue risk profile to: {risk_csv_path}")

    # 2. ml_model_metrics.csv
    import pandas as pd
    metrics_csv_path = os.path.join(OUTPUTS_DIR, "ml_model_metrics.csv")
    metrics_summary_df = pd.DataFrame([res_lr, res_rf])
    metrics_summary_df.to_csv(metrics_csv_path, index=False)
    print(f"[+] Saved model comparison metrics to: {metrics_csv_path}")

    # 3. ml_model_summary.json
    summary_json_path = os.path.join(OUTPUTS_DIR, "ml_model_summary.json")
    with open(summary_json_path, "w") as f:
        json.dump({
            "champion_model": best_results["model_name"],
            "features": [
                "queue_id (OneHotEncoded)",
                "priority_level (OneHotEncoded)",
                "arrival_hour",
                "arrival_minute_of_day",
                "is_rush_hour",
                "queue_arrivals_prior_30m",
                "total_arrivals_prior_30m"
            ],
            "target": "long_wait (wait_time_seconds > 1800s)",
            "data_source": "SYNTHETIC_SIMULATION (Seed 42)",
            "total_customers": total_count,
            "train_customers": train_count,
            "test_customers": test_count,
            "evaluation": best_results,
            "comparison": [res_lr, res_rf]
        }, f, indent=2)
    print(f"[+] Saved model metadata summary to: {summary_json_path}")

    return best_model, best_results


def main():
    print("=" * 70)
    print(" QueueWise — Spark MLlib Queue Long-Wait Prediction Pipeline")
    print("=" * 70)
    print(f"Data source: {DATA_PATH}")

    spark = get_spark_session()
    try:
        df_featured = extract_customer_arrival_dataset(spark, DATA_PATH)
        best_model, results = build_and_train_mllib_pipeline(df_featured)
        print("\n" + "=" * 70)
        print(" [SUCCESS] Spark MLlib training and evaluation completed cleanly.")
        print("=" * 70)
    finally:
        spark.stop()
        print("[+] PySpark session terminated.")


if __name__ == "__main__":
    main()
