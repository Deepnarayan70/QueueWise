"""
QueueWise REST API Routes
Exposes JSON endpoints for queue analytics, simulation models, counter workloads,
datasets catalog, and CSV export downloads.
"""

from typing import List, Optional
from flask import Blueprint, jsonify, request, send_file
from backend.services.data_service import DataService, QUEUE_NAMES, DATASET_METADATA

api_bp = Blueprint("api", __name__, url_prefix="/api")
data_service = DataService()


@api_bp.route("/health", methods=["GET"])
def get_health():
    """Health check endpoint indicating pipeline readiness and dataset status."""
    health_data = data_service.check_health()
    status_code = 200 if health_data["status"] == "healthy" else 503
    return jsonify({
        "status": "success",
        "data": health_data,
    }), status_code


@api_bp.route("/overview", methods=["GET"])
def get_overview():
    """Returns top-level KPIs, SLA benchmark chart data, hourly arrivals, and operational alerts."""
    try:
        overview_data = data_service.get_overview_data()
        return jsonify({
            "status": "success",
            "data": overview_data,
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": "Failed to compute overview metrics.",
            "details": str(e),
        }), 500


@api_bp.route("/queues", methods=["GET"])
def get_queues():
    """Returns list of active department queues and their metadata."""
    queues = data_service.get_queues_list()
    return jsonify({
        "status": "success",
        "data": queues,
    })


@api_bp.route("/queues/performance", methods=["GET"])
def get_queue_performance():
    """
    Returns queue turnaround metrics, empirical wait CDF distributions,
    custom threshold breach stats, and ML risk predictions.
    Optional query parameters:
      - queues: comma-separated queue IDs (e.g., 'Q_FINANCIAL_AID,Q_ADMISSIONS')
      - threshold: integer threshold in minutes (default: 30)
    """
    queues_param = request.args.get("queues")
    threshold_param = request.args.get("threshold", "30")

    # Validate threshold
    try:
        threshold_minutes = int(threshold_param)
        if threshold_minutes <= 0 or threshold_minutes > 360:
            return jsonify({
                "status": "error",
                "message": "Invalid threshold: must be an integer between 1 and 360 minutes.",
            }), 400
    except ValueError:
        return jsonify({
            "status": "error",
            "message": f"Invalid threshold value '{threshold_param}': must be an integer.",
        }), 400

    # Parse queues filter
    selected_queues: Optional[List[str]] = None
    if queues_param:
        tokens = [t.strip() for t in queues_param.split(",") if t.strip()]
        valid_queues = [q for q in tokens if q in QUEUE_NAMES]
        if not valid_queues:
            return jsonify({
                "status": "error",
                "message": "None of the specified queue IDs are valid.",
                "valid_queues": list(QUEUE_NAMES.keys()),
            }), 400
        selected_queues = valid_queues

    try:
        perf_data = data_service.get_queue_performance_data(
            selected_queues=selected_queues,
            threshold_minutes=threshold_minutes,
        )
        return jsonify({
            "status": "success",
            "data": perf_data,
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve queue performance data.",
            "details": str(e),
        }), 500


@api_bp.route("/counters/utilization", methods=["GET"])
def get_counter_utilization():
    """Returns workload, active hours, and utilization rates across all physical service counters."""
    try:
        util_data = data_service.get_counter_utilization_data()
        return jsonify({
            "status": "success",
            "data": util_data,
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": "Failed to compute counter utilization metrics.",
            "details": str(e),
        }), 500


@api_bp.route("/capacity/baseline", methods=["GET"])
def get_capacity_baseline():
    """Returns baseline capacity allocation and waiting time metrics."""
    try:
        baseline_data = data_service.get_capacity_baseline_data()
        return jsonify({
            "status": "success",
            "data": baseline_data,
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve baseline capacity data.",
            "details": str(e),
        }), 500


@api_bp.route("/capacity/scenario", methods=["GET"])
def get_capacity_scenario():
    """Returns reallocated scenario results, trade-off comparisons, and workload conservation proof."""
    try:
        scenario_data = data_service.get_capacity_scenario_data()
        return jsonify({
            "status": "success",
            "data": scenario_data,
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve capacity scenario data.",
            "details": str(e),
        }), 500


@api_bp.route("/datasets", methods=["GET"])
def list_datasets():
    """Returns catalog of all analytical datasets available for inspection and export."""
    catalog = data_service.get_datasets_list()
    return jsonify({
        "status": "success",
        "data": catalog,
    })


@api_bp.route("/datasets/<dataset_name>", methods=["GET"])
def get_dataset_preview(dataset_name: str):
    """
    Returns tabular preview (up to limit rows) for a specific analytical dataset.
    Prevents path traversal by strictly validating dataset_name against catalog.
    """
    limit = request.args.get("limit", 100)
    try:
        limit_int = min(max(1, int(limit)), 500)
    except ValueError:
        limit_int = 100

    preview = data_service.get_dataset_preview(dataset_name, limit=limit_int)
    if preview is None:
        return jsonify({
            "status": "error",
            "message": f"Dataset '{dataset_name}' not found or unavailable.",
        }), 404

    return jsonify({
        "status": "success",
        "data": preview,
    })


@api_bp.route("/datasets/<dataset_name>/download", methods=["GET"])
def download_dataset(dataset_name: str):
    """
    Direct file download of analytical CSV dataset with proper MIME type and attachment headers.
    Guards strictly against arbitrary file access.
    """
    fpath = data_service.get_dataset_file_path(dataset_name)
    if fpath is None:
        return jsonify({
            "status": "error",
            "message": f"Dataset '{dataset_name}' not available for download.",
        }), 404

    filename = DATASET_METADATA[dataset_name]["filename"]
    return send_file(
        fpath,
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv",
    )


@api_bp.route("/ml/risk", methods=["GET"])
def get_ml_risk():
    """Returns supervised machine learning predictions for customer long-wait risk (>30m)."""
    perf_data = data_service.get_queue_performance_data()
    return jsonify({
        "status": "success",
        "data": {
            "risk_estimates": perf_data["ml_risk"],
            "model_disclosure": "High AUC (~0.999-1.000) reflects synthetic M/M/c discrete-event queuing mechanics with clean separation boundaries.",
        },
    })


@api_bp.route("/ml/metrics", methods=["GET"])
def get_ml_metrics():
    """Returns classification evaluation metrics across holdout test partitions."""
    perf_data = data_service.get_queue_performance_data()
    return jsonify({
        "status": "success",
        "data": perf_data["ml_metrics"],
    })
