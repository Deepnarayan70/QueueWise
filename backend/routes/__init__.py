"""
QueueWise Backend Routes Package
"""

from backend.routes.api_routes import api_bp
from backend.routes.view_routes import views_bp

__all__ = ["api_bp", "views_bp"]
