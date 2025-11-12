"""
Unit tests for ingestion models.
"""
import pytest
from django.utils import timezone

from apps.ingestion.models import IngestionError, IngestionJob, StudentRecord

pytestmark = pytest.mark.django_db


class TestIngestionJob:
    """Tests for IngestionJob model."""

    def test_create_job(self):
        """Test creating an ingestion job."""
        job = IngestionJob.objects.create(task_id="test-123", total_records=100)

        assert job.task_id == "test-123"
        assert job.total_records == 100
        assert job.status == IngestionJob.Status.PENDING
        assert job.processed_records == 0
        assert job.failed_records == 0

    def test_progress_percentage(self, ingestion_job):
        """Test progress percentage calculation."""
        ingestion_job.processed_records = 50
        assert ingestion_job.progress_percentage == 50

        ingestion_job.processed_records = 100
        assert ingestion_job.progress_percentage == 100

    def test_progress_percentage_zero_records(self):
        """Test progress percentage with zero total records."""
        job = IngestionJob.objects.create(task_id="test-456", total_records=0)
        assert job.progress_percentage == 0

    def test_duration_calculation(self, ingestion_job):
        """Test job duration calculation."""
        assert ingestion_job.duration is None

        ingestion_job.started_at = timezone.now()
        ingestion_job.save()

        duration = ingestion_job.duration
        assert duration is not None
        assert duration >= 0

    def test_job_string_representation(self, ingestion_job):
        """Test string representation of job."""
        expected = f"Job {ingestion_job.task_id} - {ingestion_job.status}"
        assert str(ingestion_job) == expected


class TestStudentRecord:
    """Tests for StudentRecord model."""

    def test_create_student_record(self, ingestion_job):
        """Test creating a student record."""
        record = StudentRecord.objects.create(
            job=ingestion_job,
            student_id="STU001",
            first_name="John",
            last_name="Doe",
            email="john.doe@example.com",
            grade="10",
        )

        assert record.student_id == "STU001"
        assert record.first_name == "John"
        assert record.last_name == "Doe"
        assert record.email == "john.doe@example.com"
        assert record.grade == "10"
        assert record.job == ingestion_job

    def test_unique_student_id_per_job(self, ingestion_job):
        """Test that student_id must be unique within a job."""
        StudentRecord.objects.create(
            job=ingestion_job,
            student_id="STU001",
            first_name="John",
            last_name="Doe",
            email="john@example.com",
            grade="10",
        )

        # Creating another record with same student_id in same job should fail
        from django.db import IntegrityError

        with pytest.raises(IntegrityError):
            StudentRecord.objects.create(
                job=ingestion_job,
                student_id="STU001",
                first_name="Jane",
                last_name="Smith",
                email="jane@example.com",
                grade="9",
            )

    def test_student_record_string_representation(self, ingestion_job):
        """Test string representation of student record."""
        record = StudentRecord.objects.create(
            job=ingestion_job,
            student_id="STU001",
            first_name="John",
            last_name="Doe",
            email="john@example.com",
            grade="10",
        )

        expected = "John Doe (STU001)"
        assert str(record) == expected


class TestIngestionError:
    """Tests for IngestionError model."""

    def test_create_ingestion_error(self, ingestion_job):
        """Test creating an ingestion error."""
        error = IngestionError.objects.create(
            job=ingestion_job,
            record_index=5,
            error_type="ValidationError",
            error_message="Invalid email format",
            raw_data={"student_id": "STU001", "email": "invalid"},
        )

        assert error.job == ingestion_job
        assert error.record_index == 5
        assert error.error_type == "ValidationError"
        assert error.error_message == "Invalid email format"
        assert error.raw_data["student_id"] == "STU001"

    def test_error_string_representation(self, ingestion_job):
        """Test string representation of error."""
        error = IngestionError.objects.create(
            job=ingestion_job,
            record_index=10,
            error_type="ValidationError",
            error_message="Test error",
            raw_data={},
        )

        expected = f"Error in Job {ingestion_job.task_id} - Record 10"
        assert str(error) == expected
