"""
Service layer for ingestion business logic.
Separates business logic from views and tasks.
"""
import logging
from typing import Dict, List

from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

from .models import IngestionError, IngestionJob, StudentRecord
from .serializers import StudentRecordSerializer

logger = logging.getLogger(__name__)


class IngestionService:
    """
    Handles business logic for data ingestion operations.
    """

    CACHE_KEY_PREFIX = "ingestion_job"
    CACHE_TIMEOUT = 3600  # 1 hour

    @staticmethod
    def create_job(task_id: str, total_records: int) -> IngestionJob:
        """
        Create a new ingestion job.

        Args:
            task_id: Unique task identifier from Celery
            total_records: Total number of records to process

        Returns:
            Created IngestionJob instance
        """
        job = IngestionJob.objects.create(
            task_id=task_id,
            total_records=total_records,
            status=IngestionJob.Status.PENDING,
        )
        logger.info(f"Created ingestion job {task_id} with {total_records} records")
        return job

    @staticmethod
    def get_job_by_task_id(task_id: str) -> IngestionJob:
        """
        Retrieve job by task ID with caching.

        Args:
            task_id: Task identifier

        Returns:
            IngestionJob instance

        Raises:
            IngestionJob.DoesNotExist: If job not found
        """
        cache_key = f"{IngestionService.CACHE_KEY_PREFIX}:{task_id}"

        # Try cache first
        cached_job = cache.get(cache_key)
        if cached_job:
            return cached_job

        # Fetch from database
        job = IngestionJob.objects.get(task_id=task_id)

        # Cache the result
        cache.set(cache_key, job, IngestionService.CACHE_TIMEOUT)

        return job

    @staticmethod
    def update_job_status(
        task_id: str,
        status: str,
        processed_records: int = None,
        failed_records: int = None,
        error_message: str = None,
    ) -> IngestionJob:
        """
        Update job status and progress.

        Args:
            task_id: Task identifier
            status: New status
            processed_records: Number of processed records
            failed_records: Number of failed records
            error_message: Error message if failed

        Returns:
            Updated IngestionJob instance
        """
        job = IngestionJob.objects.get(task_id=task_id)

        job.status = status

        if status == IngestionJob.Status.PROCESSING and not job.started_at:
            job.started_at = timezone.now()

        if status in [IngestionJob.Status.COMPLETED, IngestionJob.Status.FAILED]:
            job.completed_at = timezone.now()

        if processed_records is not None:
            job.processed_records = processed_records

        if failed_records is not None:
            job.failed_records = failed_records

        if error_message:
            job.error_message = error_message

        job.save()

        # Invalidate cache
        cache_key = f"{IngestionService.CACHE_KEY_PREFIX}:{task_id}"
        cache.delete(cache_key)

        logger.info(
            f"Updated job {task_id}: status={status}, "
            f"processed={processed_records}, failed={failed_records}"
        )

        return job

    @staticmethod
    def validate_records(records: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """
        Validate a list of student records.

        Args:
            records: List of record dictionaries

        Returns:
            Tuple of (valid_records, validation_errors)
        """
        valid_records = []
        validation_errors = []

        for index, record in enumerate(records):
            serializer = StudentRecordSerializer(data=record)
            if serializer.is_valid():
                valid_records.append(serializer.validated_data)
            else:
                validation_errors.append(
                    {
                        "index": index,
                        "record": record,
                        "errors": serializer.errors,
                    }
                )

        logger.info(
            f"Validated {len(records)} records: "
            f"{len(valid_records)} valid, {len(validation_errors)} invalid"
        )

        return valid_records, validation_errors

    @staticmethod
    @transaction.atomic
    def bulk_create_records(job: IngestionJob, records: List[Dict]) -> int:
        """
        Bulk create student records in database.
        Uses transaction for atomicity and bulk_create for performance.

        Args:
            job: IngestionJob instance
            records: List of validated record dictionaries

        Returns:
            Number of records created
        """
        student_records = [
            StudentRecord(
                job=job,
                student_id=record["student_id"],
                first_name=record["first_name"],
                last_name=record["last_name"],
                email=record["email"],
                phone=record.get("phone", ""),
                date_of_birth=record.get("date_of_birth"),
                grade=record["grade"],
                section=record.get("section", ""),
                roll_number=record.get("roll_number", ""),
                address=record.get("address", ""),
                city=record.get("city", ""),
                state=record.get("state", ""),
                postal_code=record.get("postal_code", ""),
                country=record.get("country", "India"),
            )
            for record in records
        ]

        # Bulk create with batch size for optimal performance
        StudentRecord.objects.bulk_create(student_records, batch_size=500)

        logger.info(f"Bulk created {len(student_records)} records for job {job.task_id}")

        return len(student_records)

    @staticmethod
    @transaction.atomic
    def log_errors(job: IngestionJob, validation_errors: List[Dict]) -> int:
        """
        Log validation errors to database.

        Args:
            job: IngestionJob instance
            validation_errors: List of validation error dictionaries

        Returns:
            Number of errors logged
        """
        error_records = [
            IngestionError(
                job=job,
                record_index=error["index"],
                error_type="ValidationError",
                error_message=str(error["errors"]),
                raw_data=error["record"],
            )
            for error in validation_errors
        ]

        IngestionError.objects.bulk_create(error_records, batch_size=500)

        logger.warning(f"Logged {len(error_records)} errors for job {job.task_id}")

        return len(error_records)

    @staticmethod
    def get_job_statistics(task_id: str) -> Dict:
        """
        Get comprehensive statistics for a job.

        Args:
            task_id: Task identifier

        Returns:
            Dictionary with job statistics
        """
        job = IngestionService.get_job_by_task_id(task_id)

        stats = {
            "task_id": job.task_id,
            "status": job.status,
            "total_records": job.total_records,
            "processed_records": job.processed_records,
            "failed_records": job.failed_records,
            "success_rate": (
                (job.processed_records / job.total_records * 100) if job.total_records > 0 else 0
            ),
            "progress_percentage": job.progress_percentage,
            "duration": job.duration,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
        }

        return stats
