"""
QueueWise Flask Application
Central entrypoint serving both the REST API and the HTML5 frontend.
"""

import os
from flask import Flask, jsonify, request
from backend.routes.api_routes import api_bp
from backend.routes.view_routes import views_bp


def create_app(test_config=None) -> Flask:
    """Creates and configures the Flask application."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    frontend_dir = os.path.join(project_root, "frontend")

    app = Flask(
        __name__,
        static_folder=frontend_dir,
        static_url_path="",
    )

    if test_config:
        app.config.update(test_config)

    # Register routes
    app.register_blueprint(views_bp)
    app.register_blueprint(api_bp)

    # JSON error handlers for API requests
    @app.errorhandler(404)
    def handle_not_found(e):
        if request.path.startswith("/api/"):
            return jsonify({
                "status": "error",
                "message": "Resource not found.",
                "path": request.path,
            }), 404
        return "<h3>404 Not Found</h3><p>The requested page or asset does not exist.</p><p><a href='/'>Return to Overview</a></p>", 404

    @app.errorhandler(500)
    def handle_server_error(e):
        if request.path.startswith("/api/"):
            return jsonify({
                "status": "error",
                "message": "Internal server error.",
            }), 500
        return "<h3>500 Internal Server Error</h3><p>An unexpected error occurred.</p><p><a href='/'>Return to Overview</a></p>", 500

    @app.after_request
    def set_security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        return response

    return app


if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "127.0.0.1")
    print(f"\n=======================================================")
    print(f" QueueWise Operations Intelligence Platform")
    print(f" Running at http://{host}:{port}/")
    print(f" API documentation & health: http://{host}:{port}/api/health")
    print(f"=======================================================\n")
    app.run(host=host, port=port, debug=False)
