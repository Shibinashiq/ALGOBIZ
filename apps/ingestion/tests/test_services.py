"""
Unit tests for ingestion services.
"""
import pytest
from django.core.cache import cache

from apps.ingestion.models import IngestionJob, StudentRecord
from apps.ingestion.services import IngestionService

pytestmark = pytest.mark.django_db


class TestIngestionService:
    """Tests for IngestionService."""

    def test_create_job(self):
        """Test creating a new ingestion job."""
        task_id = "test-task-123"
        total_records = 100

        job = IngestionService.create_job(task_id, total_records)

        assert job.task_id == task_id
        assert job.total_records == total_records
        assert job.status == IngestionJob.Status.PENDING

    def test_get_job_by_task_id(self, ingestion_job):
        """Test retrieving job by task ID."""
        retrieved_job = IngestionService.get_job_by_task_id(ingestion_job.task_id)
        assert retrieved_job.id == ingestion_job.id
        assert retrieved_job.task_id == ingestion_job.task_id

    def test_get_job_by_task_id_caching(self, ingestion_job):
        """Test that job retrieval uses caching."""
        # First call - should cache
        job1 = IngestionService.get_job_by_task_id(ingestion_job.task_id)

        # Second call - should use cache
        job2 = IngestionService.get_job_by_task_id(ingestion_job.task_id)

        assert job1.id == job2.id

        # Verify cache key exists
        cache_key = f"{IngestionService.CACHE_KEY_PREFIX}:{ingestion_job.task_id}"
        cached_job = cache.get(cache_key)
        assert cached_job is not None

    def test_get_job_not_found(self):
        """Test retrieving non-existent job raises exception."""
        with pytest.raises(IngestionJob.DoesNotExist):
            IngestionService.get_job_by_task_id("non-existent-id")

    def test_update_job_status(self, ingestion_job):
        """Test updating job status."""
        updated_job = IngestionService.update_job_status(
            ingestion_job.task_id,
            IngestionJob.Status.PROCESSING,
            processed_records=50,
            failed_records=5,
        )

        assert updated_job.status == IngestionJob.Status.PROCESSING
        assert updated_job.processed_records == 50
        assert updated_job.failed_records == 5
        assert updated_job.started_at is not None

    def test_update_job_to_completed(self, ingestion_job):
        """Test updating job to completed status."""
        updated_job = IngestionService.update_job_status(
            ingestion_job.task_id,
            IngestionJob.Status.COMPLETED,
            processed_records=100,
        )

        assert updated_job.status == IngestionJob.Status.COMPLETED
        assert updated_job.completed_at is not None

    def test_validate_records(self, sample_student_records):
        """Test record validation."""
        records = sample_student_records(10)
        valid, errors = IngestionService.validate_records(records)

        assert len(valid) == 10
        assert len(errors) == 0

    def test_validate_records_with_errors(self, sample_student_records):
        """Test record validation with invalid records."""
        records = sample_student_records(5)
        # Add invalid record
        records.append({"student_id": "STU999", "invalid_field": "test"})

        valid, errors = IngestionService.validate_records(records)

        assert len(valid) == 5
        assert len(errors) == 1
        assert errors[0]["index"] == 5

    def test_bulk_create_records(self, ingestion_job, sample_student_records):
        """Test bulk creating student records."""
        records = sample_student_records(50)
        valid_records, _ = IngestionService.validate_records(records)

        created_count = IngestionService.bulk_create_records(ingestion_job, valid_records)

        assert created_count == 50
        assert StudentRecord.objects.filter(job=ingestion_job).count() == 50

    def test_log_errors(self, ingestion_job):
        """Test logging validation errors."""
        validation_errors = [
            {"index": 0, "record": {"student_id": "STU001"}, "errors": {"email": ["Invalid"]}},
            {"index": 1, "record": {"student_id": "STU002"}, "errors": {"grade": ["Invalid"]}},
        ]

        error_count = IngestionService.log_errors(ingestion_job, validation_errors)

        assert error_count == 2
        assert ingestion_job.errors.count() == 2

    def test_get_job_statistics(self, completed_ingestion_job):
        """Test getting job statistics."""
        stats = IngestionService.get_job_statistics(completed_ingestion_job.task_id)

        assert stats["task_id"] == completed_ingestion_job.task_id
        assert stats["status"] == IngestionJob.Status.COMPLETED
        assert stats["total_records"] == 100
        assert stats["processed_records"] == 95
        assert stats["failed_records"] == 5
        assert stats["success_rate"] == 95.0
