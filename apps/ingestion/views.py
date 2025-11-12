"""
REST API views for data ingestion.
Implements non-blocking asynchronous endpoints.
"""
import logging

from drf_spectacular.utils import extend_schema, extend_schema_view
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .exceptions import JobNotFoundError
from .models import IngestionJob
from .serializers import (
    BulkIngestionRequestSerializer,
    IngestionJobCreateResponseSerializer,
    IngestionJobStatusSerializer,
)
from .services import IngestionService
from .tasks import process_ingestion

logger = logging.getLogger(__name__)


@extend_schema_view(
    post=extend_schema(
        summary="Bulk Data Ingestion",
        description=(
            "Submit up to 1,000 student records for asynchronous processing. "
            "Returns immediately with a task_id for status tracking."
        ),
        request=BulkIngestionRequestSerializer,
        responses={
            201: IngestionJobCreateResponseSerializer,
            400: {"description": "Invalid request data"},
        },
        tags=["Data Ingestion"],
    )
)
class BulkIngestionView(APIView):
    """
    POST /api/data/ingest/

    High-volume ingestion endpoint that accepts up to 1,000 records
    and returns immediately with a task ID for async processing.
    """

    def post(self, request):
        """
        Submit bulk data for asynchronous ingestion.

        Request Body:
        {
            "records": [
                {
                    "student_id": "STU001",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "john.doe@example.com",
                    "grade": "10",
                    ...
                },
                ...
            ]
        }

        Response:
        {
            "task_id": "abc-123-def-456",
            "status": "PENDING",
            "message": "Ingestion job created successfully",
            "total_records": 1000
        }
        """
        # Validate request data
        serializer = BulkIngestionRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        records = serializer.validated_data["records"]
        total_records = len(records)

        logger.info(f"Received ingestion request with {total_records} records")

        # Dispatch async task
        task = process_ingestion.apply_async(
            kwargs={"records": records}
        )

        # Create job record
        job = IngestionService.create_job(task.id, total_records)

        # Return immediately with task ID
        response_data = {
            "task_id": job.task_id,
            "status": job.status,
            "message": "Ingestion job created successfully",
            "total_records": total_records,
        }

        logger.info(f"Created ingestion job {job.task_id} for {total_records} records")

        return Response(response_data, status=status.HTTP_201_CREATED)


@extend_schema_view(
    get=extend_schema(
        summary="Get Job Status",
        description=(
            "Retrieve real-time status of an ingestion job including progress percentage. "
            "Status can be: PENDING, PROCESSING (with %), COMPLETED, or FAILED."
        ),
        responses={
            200: IngestionJobStatusSerializer,
            404: {"description": "Job not found"},
        },
        tags=["Data Ingestion"],
    )
)
class JobStatusView(APIView):
    """
    GET /api/data/status/<task_id>/

    Real-time status check endpoint that reports job progress.
    """

    def get(self, request, task_id):
        """
        Get real-time status of an ingestion job.

        Response:
        {
            "task_id": "abc-123-def-456",
            "status": "PROCESSING",
            "total_records": 1000,
            "processed_records": 700,
            "failed_records": 5,
            "progress_percentage": 70,
            "error_message": null,
            "created_at": "2024-01-01T10:00:00Z",
            "started_at": "2024-01-01T10:00:01Z",
            "completed_at": null,
            "duration": 35.5
        }
        """
        try:
            job = IngestionService.get_job_by_task_id(task_id)
        except IngestionJob.DoesNotExist as exc:
            logger.warning(f"Job not found: {task_id}")
            raise JobNotFoundError(detail=f"Job with task_id '{task_id}' not found") from exc

        serializer = IngestionJobStatusSerializer(job)

        # Add human-readable status message
        response_data = serializer.data
        if job.status == IngestionJob.Status.PROCESSING:
            response_data["status_message"] = (
                f"{job.status} ({job.progress_percentage}% complete)"
            )
        else:
            response_data["status_message"] = job.status

        logger.debug(f"Status check for job {task_id}: {job.status}")

        return Response(response_data, status=status.HTTP_200_OK)


@extend_schema_view(
    get=extend_schema(
        summary="Health Check",
        description="Check if the API is running and healthy.",
        responses={200: {"description": "API is healthy"}},
        tags=["System"],
    )
)
class HealthCheckView(APIView):
    """
    GET /api/health/

    Simple health check endpoint.
    """

    def get(self, request):
        """Health check endpoint."""
        return Response(
            {
                "status": "healthy",
                "service": "School Management System",
                "version": "1.0.0",
            },
            status=status.HTTP_200_OK,
        )
