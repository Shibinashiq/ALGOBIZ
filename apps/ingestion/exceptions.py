"""
Custom exceptions and error handlers for the ingestion API.
"""
from rest_framework import status
from rest_framework.exceptions import APIException
from rest_framework.views import exception_handler


class IngestionValidationError(APIException):
    """Raised when ingestion data validation fails."""

    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = "Invalid data provided for ingestion"
    default_code = "validation_error"


class IngestionProcessingError(APIException):
    """Raised when ingestion processing fails."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    default_detail = "Error processing ingestion job"
    default_code = "processing_error"


class JobNotFoundError(APIException):
    """Raised when job is not found."""

    status_code = status.HTTP_404_NOT_FOUND
    default_detail = "Ingestion job not found"
    default_code = "job_not_found"


def custom_exception_handler(exc, context):
    """
    Custom exception handler that provides consistent error responses.
    """
    response = exception_handler(exc, context)

    if response is not None:
        custom_response_data = {
            "error": True,
            "message": response.data.get("detail", str(exc)),
            "status_code": response.status_code,
        }

        # Add validation errors if present
        if hasattr(exc, "detail") and isinstance(exc.detail, dict):
            custom_response_data["errors"] = exc.detail

        response.data = custom_response_data

    return response
