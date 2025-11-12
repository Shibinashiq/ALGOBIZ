"""
Celery tasks for asynchronous data ingestion processing.
Optimized for high-volume concurrent job processing.
"""
import logging
import time
from typing import List

from celery import shared_task
from django.conf import settings

from .models import IngestionJob
from .services import IngestionService

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    name="apps.ingestion.tasks.process_ingestion",
    max_retries=3,
    default_retry_delay=60,
)
def process_ingestion(self, records: List[dict]) -> dict:
    """
    Process bulk data ingestion asynchronously.

    This task:
    1. Validates all records
    2. Simulates external API calls (0.5s per 100 records)
    3. Bulk inserts validated records into PostgreSQL
    4. Updates job status in real-time

    Args:
        records: List of student record dictionaries

    Returns:
        Dictionary with processing results
    """
    task_id = self.request.id
    logger.info(f"Starting ingestion task {task_id} with {len(records)} records")

    try:
        # Get or create job
        try:
            job = IngestionService.get_job_by_task_id(task_id)
        except IngestionJob.DoesNotExist:
            job = IngestionService.create_job(task_id, len(records))

        # Update status to PROCESSING
        IngestionService.update_job_status(task_id, IngestionJob.Status.PROCESSING)

        # Step 1: Validate all records
        logger.info(f"Task {task_id}: Validating {len(records)} records")
        valid_records, validation_errors = IngestionService.validate_records(records)

        # Log validation errors
        if validation_errors:
            IngestionService.log_errors(job, validation_errors)
            logger.warning(f"Task {task_id}: Found {len(validation_errors)} validation errors")

        # Step 2: Process in batches with simulated external API calls
        batch_size = 100
        total_batches = (len(valid_records) + batch_size - 1) // batch_size
        processed_count = 0

        logger.info(f"Task {task_id}: Processing {len(valid_records)} valid records in {total_batches} batches")

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(valid_records))
            batch = valid_records[start_idx:end_idx]

            # Simulate external API call (0.5s per 100 records)
            logger.debug(f"Task {task_id}: Simulating external API call for batch {batch_num + 1}/{total_batches}")
            time.sleep(settings.EXTERNAL_API_DELAY)

            # Bulk insert batch
            created_count = IngestionService.bulk_create_records(job, batch)
            processed_count += created_count

            # Update progress in real-time
            IngestionService.update_job_status(
                task_id,
                IngestionJob.Status.PROCESSING,
                processed_records=processed_count,
                failed_records=len(validation_errors),
            )

            logger.info(
                f"Task {task_id}: Processed batch {batch_num + 1}/{total_batches} "
                f"({processed_count}/{len(valid_records)} records)"
            )

        # Step 3: Mark as completed
        IngestionService.update_job_status(
            task_id,
            IngestionJob.Status.COMPLETED,
            processed_records=processed_count,
            failed_records=len(validation_errors),
        )

        result = {
            "task_id": task_id,
            "status": "COMPLETED",
            "total_records": len(records),
            "processed_records": processed_count,
            "failed_records": len(validation_errors),
            "success": True,
        }

        logger.info(
            f"Task {task_id} completed successfully: "
            f"{processed_count} processed, {len(validation_errors)} failed"
        )

        return result

    except Exception as exc:
        logger.error(f"Task {task_id} failed with error: {str(exc)}", exc_info=True)

        # Update job status to FAILED
        try:
            IngestionService.update_job_status(
                task_id, IngestionJob.Status.FAILED, error_message=str(exc)
            )
        except Exception as update_exc:
            logger.error(f"Failed to update job status: {str(update_exc)}")

        # Retry the task
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying task {task_id} (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(exc=exc) from exc

        # Max retries reached
        return {
            "task_id": task_id,
            "status": "FAILED",
            "error": str(exc),
            "success": False,
        }


@shared_task(name="apps.ingestion.tasks.cleanup_old_jobs")
def cleanup_old_jobs(days: int = 30) -> dict:
    """
    Cleanup old completed jobs and their records.
    Should be run periodically via Celery Beat.

    Args:
        days: Number of days to keep jobs

    Returns:
        Dictionary with cleanup statistics
    """
    from datetime import timedelta

    from django.utils import timezone

    cutoff_date = timezone.now() - timedelta(days=days)

    # Delete old completed jobs
    deleted_count, _ = IngestionJob.objects.filter(
        status=IngestionJob.Status.COMPLETED, completed_at__lt=cutoff_date
    ).delete()

    logger.info(f"Cleaned up {deleted_count} jobs older than {days} days")

    return {"deleted_jobs": deleted_count, "cutoff_date": cutoff_date.isoformat()}


@shared_task(name="apps.ingestion.tasks.generate_job_report")
def generate_job_report(task_id: str) -> dict:
    """
    Generate a detailed report for a completed job.

    Args:
        task_id: Task identifier

    Returns:
        Dictionary with job report
    """
    try:
        stats = IngestionService.get_job_statistics(task_id)
        logger.info(f"Generated report for job {task_id}")
        return stats
    except IngestionJob.DoesNotExist:
        logger.error(f"Job {task_id} not found for report generation")
        return {"error": "Job not found"}
