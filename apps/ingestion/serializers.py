"""
Serializers for data ingestion API.
Handles validation and serialization of student records.
"""
from datetime import datetime

from rest_framework import serializers

from .models import IngestionJob, StudentRecord


class StudentRecordSerializer(serializers.Serializer):
    """
    Validates individual student record data.
    """

    student_id = serializers.CharField(max_length=50, required=True)
    first_name = serializers.CharField(max_length=100, required=True)
    last_name = serializers.CharField(max_length=100, required=True)
    email = serializers.EmailField(max_length=255, required=True)
    phone = serializers.CharField(max_length=20, required=False, allow_blank=True)
    date_of_birth = serializers.DateField(required=False, allow_null=True)

    grade = serializers.CharField(max_length=20, required=True)
    section = serializers.CharField(max_length=10, required=False, allow_blank=True)
    roll_number = serializers.CharField(max_length=50, required=False, allow_blank=True)

    address = serializers.CharField(required=False, allow_blank=True)
    city = serializers.CharField(max_length=100, required=False, allow_blank=True)
    state = serializers.CharField(max_length=100, required=False, allow_blank=True)
    postal_code = serializers.CharField(max_length=20, required=False, allow_blank=True)
    country = serializers.CharField(max_length=100, default="India")

    def validate_date_of_birth(self, value):
        """Validate date of birth is not in the future."""
        if value and value > datetime.now().date():
            raise serializers.ValidationError("Date of birth cannot be in the future")
        return value

    def validate_grade(self, value):
        """Validate grade is within acceptable range."""
        valid_grades = [
            "Nursery",
            "LKG",
            "UKG",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
        ]
        if value not in valid_grades:
            raise serializers.ValidationError(
                f"Invalid grade. Must be one of: {', '.join(valid_grades)}"
            )
        return value


class BulkIngestionRequestSerializer(serializers.Serializer):
    """
    Validates bulk ingestion request with up to 1,000 records.
    """

    records = serializers.ListField(
        child=StudentRecordSerializer(), min_length=1, max_length=1000, required=True
    )

    def validate_records(self, value):
        """Additional validation for the records list."""
        if not value:
            raise serializers.ValidationError("Records list cannot be empty")

        # Check for duplicate student_ids within the batch
        student_ids = [record["student_id"] for record in value]
        if len(student_ids) != len(set(student_ids)):
            raise serializers.ValidationError("Duplicate student_id found in the batch")

        return value


class IngestionJobStatusSerializer(serializers.ModelSerializer):
    """
    Serializes ingestion job status for API responses.
    """

    progress_percentage = serializers.IntegerField(read_only=True)
    duration = serializers.FloatField(read_only=True)

    class Meta:
        model = IngestionJob
        fields = [
            "task_id",
            "status",
            "total_records",
            "processed_records",
            "failed_records",
            "progress_percentage",
            "error_message",
            "created_at",
            "started_at",
            "completed_at",
            "duration",
        ]
        read_only_fields = fields


class IngestionJobCreateResponseSerializer(serializers.Serializer):
    """
    Response serializer for job creation.
    """

    task_id = serializers.CharField()
    status = serializers.CharField()
    message = serializers.CharField()
    total_records = serializers.IntegerField()


class StudentRecordDetailSerializer(serializers.ModelSerializer):
    """
    Detailed serializer for student records.
    """

    class Meta:
        model = StudentRecord
        fields = [
            "id",
            "student_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "date_of_birth",
            "grade",
            "section",
            "roll_number",
            "address",
            "city",
            "state",
            "postal_code",
            "country",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]
