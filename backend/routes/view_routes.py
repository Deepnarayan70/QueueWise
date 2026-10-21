"""
QueueWise Frontend View Routes
Serves HTML5 pages directly from the Flask application to provide
a cohesive, single-origin experience without CORS complications.
"""

import os
from flask import Blueprint, current_app, send_from_directory, abort

views_bp = Blueprint("views", __name__)


def _get_frontend_dir() -> str:
    """Returns absolute path to frontend directory."""
    return os.path.join(current_app.root_path, "..", "frontend")


@views_bp.route("/", methods=["GET"])
def index():
    """Renders main Overview page."""
    frontend_dir = _get_frontend_dir()
    return send_from_directory(frontend_dir, "index.html")


@views_bp.route("/queue-performance", methods=["GET"])
def queue_performance():
    """Renders Queue Performance page."""
    pages_dir = os.path.join(_get_frontend_dir(), "pages")
    return send_from_directory(pages_dir, "queue-performance.html")


@views_bp.route("/counter-utilization", methods=["GET"])
def counter_utilization():
    """Renders Counter Utilization page."""
    pages_dir = os.path.join(_get_frontend_dir(), "pages")
    return send_from_directory(pages_dir, "counter-utilization.html")


@views_bp.route("/capacity-planning", methods=["GET"])
def capacity_planning():
    """Renders Capacity Planning page."""
    pages_dir = os.path.join(_get_frontend_dir(), "pages")
    return send_from_directory(pages_dir, "capacity-planning.html")


@views_bp.route("/data-explorer", methods=["GET"])
def data_explorer():
    """Renders Data Explorer & Export page."""
    pages_dir = os.path.join(_get_frontend_dir(), "pages")
    return send_from_directory(pages_dir, "data-explorer.html")


@views_bp.route("/pages/<path:filename>", methods=["GET"])
def serve_page(filename: str):
    """Direct URL fallback for /pages/*.html."""
    pages_dir = os.path.join(_get_frontend_dir(), "pages")
    if os.path.exists(os.path.join(pages_dir, filename)):
        return send_from_directory(pages_dir, filename)
    abort(404)
