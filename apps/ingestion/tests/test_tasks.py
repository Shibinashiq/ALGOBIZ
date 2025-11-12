"""
Unit tests for Celery tasks.
"""
from unittest.mock import patch

import pytest

from apps.ingestion.models import IngestionJob, StudentRecord
from apps.ingestion.tasks import cleanup_old_jobs, generate_job_report, process_ingestion

pytestmark = pytest.mark.django_db


class TestProcessIngestionTask:
    """Tests for process_ingestion Celery task."""

    @patch("apps.ingestion.tasks.time.sleep")
    def test_process_ingestion_success(self, mock_sleep, sample_student_records):
        """Test successful ingestion processing."""
        records = sample_student_records(100)
        task_id = "test-task-123"

        # Call task with apply() to simulate Celery execution with task_id
        result = process_ingestion.apply(args=[records], task_id=task_id).result

        assert result["success"] is True
        assert result["task_id"] == task_id
        assert result["status"] == "COMPLETED"
        assert result["processed_records"] == 100
        assert result["failed_records"] == 0

        # Verify job was created and completed
        job = IngestionJob.objects.get(task_id=task_id)
        assert job.status == IngestionJob.Status.COMPLETED
        assert job.processed_records == 100

        # Verify records were created
        assert StudentRecord.objects.filter(job=job).count() == 100

        # Verify external API simulation was called (once per 100 records)
        assert mock_sleep.call_count == 1

    @patch("apps.ingestion.tasks.time.sleep")
    def test_process_ingestion_with_validation_errors(self, mock_sleep, sample_student_records):
        """Test ingestion with some invalid records."""
        records = sample_student_records(10)
        # Add invalid records
        records.append({"student_id": "INVALID", "invalid": "data"})
        records.append({"student_id": "INVALID2", "bad": "record"})

        task_id = "test-task-456"

        result = process_ingestion.apply(args=[records], task_id=task_id).result

        assert result["success"] is True
        assert result["processed_records"] == 10
        assert result["failed_records"] == 2

        # Verify errors were logged
        job = IngestionJob.objects.get(task_id=task_id)
        assert job.errors.count() == 2

    @patch("apps.ingestion.tasks.time.sleep")
    def test_process_ingestion_batch_processing(self, mock_sleep, sample_student_records):
        """Test that records are processed in batches of 100."""
        records = sample_student_records(250)
        task_id = "test-task-789"

        process_ingestion.apply(args=[records], task_id=task_id)

        # Should call sleep 3 times (3 batches: 100, 100, 50)
        assert mock_sleep.call_count == 3

    @patch("apps.ingestion.tasks.time.sleep")
    def test_process_ingestion_progress_updates(self, mock_sleep, sample_student_records):
        """Test that job progress is updated during processing."""
        records = sample_student_records(200)
        task_id = "test-task-progress"

        process_ingestion.apply(args=[records], task_id=task_id)

        job = IngestionJob.objects.get(task_id=task_id)
        assert job.processed_records == 200
        assert job.progress_percentage == 100

    @patch("apps.ingestion.tasks.IngestionService.bulk_create_records")
    def test_process_ingestion_handles_errors(self, mock_bulk_create, sample_student_records):
        """Test that ingestion handles processing errors gracefully."""
        mock_bulk_create.side_effect = Exception("Database error")

        records = sample_student_records(10)
        task_id = "test-task-error"

        result = process_ingestion.apply(args=[records], task_id=task_id).result

        assert result["success"] is False
        assert "error" in result

        # Verify job status was updated to FAILED
        job = IngestionJob.objects.get(task_id=task_id)
        assert job.status == IngestionJob.Status.FAILED
        assert job.error_message is not None


class TestCleanupOldJobsTask:
    """Tests for cleanup_old_jobs task."""

    def test_cleanup_old_completed_jobs(self):
        """Test cleaning up old completed jobs."""
        from datetime import timedelta

        from django.utils import timezone

        # Create old completed job
        old_date = timezone.now() - timedelta(days=35)
        IngestionJob.objects.create(
            task_id="old-job",
            total_records=100,
            status=IngestionJob.Status.COMPLETED,
            completed_at=old_date,
        )

        # Create recent completed job
        IngestionJob.objects.create(
            task_id="recent-job",
            total_records=100,
            status=IngestionJob.Status.COMPLETED,
            completed_at=timezone.now(),
        )

        result = cleanup_old_jobs(days=30)

        assert result["deleted_jobs"] == 1

        # Verify old job was deleted
        assert not IngestionJob.objects.filter(task_id="old-job").exists()

        # Verify recent job still exists
        assert IngestionJob.objects.filter(task_id="recent-job").exists()


class TestGenerateJobReportTask:
    """Tests for generate_job_report task."""

    def test_generate_report_for_existing_job(self, completed_ingestion_job):
        """Test generating report for existing job."""
        result = generate_job_report(completed_ingestion_job.task_id)

        assert "task_id" in result
        assert result["task_id"] == completed_ingestion_job.task_id
        assert result["status"] == IngestionJob.Status.COMPLETED
        assert result["total_records"] == 100

    def test_generate_report_for_non_existent_job(self):
        """Test generating report for non-existent job."""
        result = generate_job_report("non-existent-id")

        assert "error" in result
        assert result["error"] == "Job not found"
