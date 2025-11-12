"""
Unit tests for ingestion serializers.
"""
from datetime import date, timedelta

import pytest

from apps.ingestion.serializers import (
    BulkIngestionRequestSerializer,
    StudentRecordSerializer,
)

pytestmark = pytest.mark.unit


class TestStudentRecordSerializer:
    """Tests for StudentRecordSerializer."""

    def test_valid_student_record(self, sample_student_record):
        """Test serialization of valid student record."""
        serializer = StudentRecordSerializer(data=sample_student_record)
        assert serializer.is_valid()
        assert serializer.validated_data["student_id"] == sample_student_record["student_id"]

    def test_missing_required_fields(self):
        """Test validation fails with missing required fields."""
        data = {"student_id": "STU001"}
        serializer = StudentRecordSerializer(data=data)
        assert not serializer.is_valid()
        assert "first_name" in serializer.errors
        assert "last_name" in serializer.errors
        assert "email" in serializer.errors
        assert "grade" in serializer.errors

    def test_invalid_email(self, sample_student_record):
        """Test validation fails with invalid email."""
        sample_student_record["email"] = "invalid-email"
        serializer = StudentRecordSerializer(data=sample_student_record)
        assert not serializer.is_valid()
        assert "email" in serializer.errors

    def test_invalid_grade(self, sample_student_record):
        """Test validation fails with invalid grade."""
        sample_student_record["grade"] = "99"
        serializer = StudentRecordSerializer(data=sample_student_record)
        assert not serializer.is_valid()
        assert "grade" in serializer.errors

    def test_future_date_of_birth(self, sample_student_record):
        """Test validation fails with future date of birth."""
        future_date = date.today() + timedelta(days=365)
        sample_student_record["date_of_birth"] = future_date.isoformat()
        serializer = StudentRecordSerializer(data=sample_student_record)
        assert not serializer.is_valid()
        assert "date_of_birth" in serializer.errors

    def test_optional_fields(self):
        """Test that optional fields can be omitted."""
        data = {
            "student_id": "STU001",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com",
            "grade": "10",
        }
        serializer = StudentRecordSerializer(data=data)
        assert serializer.is_valid()


class TestBulkIngestionRequestSerializer:
    """Tests for BulkIngestionRequestSerializer."""

    def test_valid_bulk_request(self, sample_student_records):
        """Test serialization of valid bulk request."""
        records = sample_student_records(10)
        data = {"records": records}
        serializer = BulkIngestionRequestSerializer(data=data)
        assert serializer.is_valid()
        assert len(serializer.validated_data["records"]) == 10

    def test_empty_records_list(self):
        """Test validation fails with empty records list."""
        data = {"records": []}
        serializer = BulkIngestionRequestSerializer(data=data)
        assert not serializer.is_valid()
        assert "records" in serializer.errors

    def test_exceeds_max_records(self, sample_student_records):
        """Test validation fails when exceeding 1000 records."""
        records = sample_student_records(1001)
        data = {"records": records}
        serializer = BulkIngestionRequestSerializer(data=data)
        assert not serializer.is_valid()
        assert "records" in serializer.errors

    def test_duplicate_student_ids(self, sample_student_record):
        """Test validation fails with duplicate student IDs."""
        record1 = sample_student_record.copy()
        record2 = sample_student_record.copy()
        record2["email"] = "different@example.com"  # Different email, same student_id

        data = {"records": [record1, record2]}
        serializer = BulkIngestionRequestSerializer(data=data)
        assert not serializer.is_valid()
        assert "records" in serializer.errors

    def test_max_records_allowed(self, sample_student_records):
        """Test that exactly 1000 records is valid."""
        records = sample_student_records(1000)
        data = {"records": records}
        serializer = BulkIngestionRequestSerializer(data=data)
        assert serializer.is_valid()
        assert len(serializer.validated_data["records"]) == 1000
