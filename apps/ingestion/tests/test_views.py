"""
Unit tests for ingestion API views.
"""
import pytest
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from apps.ingestion.models import IngestionJob

pytestmark = pytest.mark.django_db


@pytest.fixture
def api_client():
    """Create API client for testing."""
    return APIClient()


class TestBulkIngestionView:
    """Tests for BulkIngestionView."""

    def test_successful_ingestion(self, api_client, sample_student_records):
        """Test successful bulk ingestion request."""
        records = sample_student_records(10)
        data = {"records": records}

        url = reverse("ingestion:bulk-ingest")
        response = api_client.post(url, data, format="json")

        assert response.status_code == status.HTTP_201_CREATED
        assert "task_id" in response.data
        assert response.data["status"] == "PENDING"
        assert response.data["total_records"] == 10

        # Verify job was created
        task_id = response.data["task_id"]
        job = IngestionJob.objects.get(task_id=task_id)
        assert job.total_records == 10

    def test_ingestion_with_max_records(self, api_client, sample_student_records):
        """Test ingestion with maximum allowed records (1000)."""
        records = sample_student_records(1000)
        data = {"records": records}

        url = reverse("ingestion:bulk-ingest")
        response = api_client.post(url, data, format="json")

        assert response.status_code == status.HTTP_201_CREATED
        assert response.data["total_records"] == 1000

    def test_ingestion_exceeds_max_records(self, api_client, sample_student_records):
        """Test ingestion fails when exceeding 1000 records."""
        records = sample_student_records(1001)
        data = {"records": records}

        url = reverse("ingestion:bulk-ingest")
        response = api_client.post(url, data, format="json")

        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_ingestion_empty_records(self, api_client):
        """Test ingestion fails with empty records list."""
        data = {"records": []}

        url = reverse("ingestion:bulk-ingest")
        response = api_client.post(url, data, format="json")

        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_ingestion_invalid_record(self, api_client):
        """Test ingestion fails with invalid record data."""
        data = {"records": [{"invalid": "data"}]}

        url = reverse("ingestion:bulk-ingest")
        response = api_client.post(url, data, format="json")

        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_ingestion_duplicate_student_ids(self, api_client, sample_student_record):
        """Test ingestion fails with duplicate student IDs."""
        record1 = sample_student_record.copy()
        record2 = sample_student_record.copy()
        record2["email"] = "different@example.com"

        data = {"records": [record1, record2]}

        url = reverse("ingestion:bulk-ingest")
        response = api_client.post(url, data, format="json")

        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_ingestion_returns_immediately(self, api_client, sample_student_records):
        """Test that ingestion endpoint returns immediately (non-blocking)."""
        import time

        records = sample_student_records(100)
        data = {"records": records}

        url = reverse("ingestion:bulk-ingest")

        start_time = time.time()
        response = api_client.post(url, data, format="json")
        end_time = time.time()

        # Should return in less than 1 second (non-blocking)
        assert (end_time - start_time) < 1.0
        assert response.status_code == status.HTTP_201_CREATED


class TestJobStatusView:
    """Tests for JobStatusView."""

    def test_get_job_status_pending(self, api_client, ingestion_job):
        """Test getting status of pending job."""
        url = reverse("ingestion:job-status", kwargs={"task_id": ingestion_job.task_id})
        response = api_client.get(url)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["task_id"] == ingestion_job.task_id
        assert response.data["status"] == "PENDING"
        assert response.data["total_records"] == 100
        assert response.data["processed_records"] == 0

    def test_get_job_status_processing(self, api_client, ingestion_job):
        """Test getting status of processing job."""
        ingestion_job.status = IngestionJob.Status.PROCESSING
        ingestion_job.processed_records = 70
        ingestion_job.save()

        url = reverse("ingestion:job-status", kwargs={"task_id": ingestion_job.task_id})
        response = api_client.get(url)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["status"] == "PROCESSING"
        assert response.data["progress_percentage"] == 70
        assert "PROCESSING (70% complete)" in response.data["status_message"]

    def test_get_job_status_completed(self, api_client, completed_ingestion_job):
        """Test getting status of completed job."""
        url = reverse("ingestion:job-status", kwargs={"task_id": completed_ingestion_job.task_id})
        response = api_client.get(url)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["status"] == "COMPLETED"
        assert response.data["processed_records"] == 95
        assert response.data["failed_records"] == 5

    def test_get_job_status_not_found(self, api_client):
        """Test getting status of non-existent job."""
        url = reverse("ingestion:job-status", kwargs={"task_id": "non-existent-id"})
        response = api_client.get(url)

        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestHealthCheckView:
    """Tests for HealthCheckView."""

    def test_health_check(self, api_client):
        """Test health check endpoint."""
        url = reverse("ingestion:health-check")
        response = api_client.get(url)

        assert response.status_code == status.HTTP_200_OK
        assert response.data["status"] == "healthy"
        assert "service" in response.data
        assert "version" in response.data
