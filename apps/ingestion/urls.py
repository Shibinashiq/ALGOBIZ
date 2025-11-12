"""
URL routing for ingestion API endpoints.
"""
from django.urls import path

from .views import BulkIngestionView, HealthCheckView, JobStatusView

app_name = "ingestion"

urlpatterns = [
    # Core ingestion endpoints
    path("data/ingest/", BulkIngestionView.as_view(), name="bulk-ingest"),
    path("data/status/<str:task_id>/", JobStatusView.as_view(), name="job-status"),
    # Health check
    path("health/", HealthCheckView.as_view(), name="health-check"),
]
