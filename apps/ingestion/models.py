"""
Database models for data ingestion system.
Optimized for high-volume concurrent writes.
"""
from django.db import models
from django.utils import timezone


class IngestionJob(models.Model):
    """
    Tracks the status of asynchronous ingestion jobs.
    """

    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"
        PROCESSING = "PROCESSING", "Processing"
        COMPLETED = "COMPLETED", "Completed"
        FAILED = "FAILED", "Failed"

    task_id = models.CharField(max_length=255, unique=True, db_index=True)
    status = models.CharField(
        max_length=20, choices=Status.choices, default=Status.PENDING, db_index=True
    )
    total_records = models.IntegerField(default=0)
    processed_records = models.IntegerField(default=0)
    failed_records = models.IntegerField(default=0)
    error_message = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "ingestion_jobs"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["task_id", "status"]),
            models.Index(fields=["created_at"]),
        ]

    def __str__(self):
        return f"Job {self.task_id} - {self.status}"

    @property
    def progress_percentage(self):
        """Calculate progress percentage."""
        if self.total_records == 0:
            return 0
        return int((self.processed_records / self.total_records) * 100)

    @property
    def duration(self):
        """Calculate job duration in seconds."""
        if not self.started_at:
            return None
        end_time = self.completed_at or timezone.now()
        return (end_time - self.started_at).total_seconds()


class StudentRecord(models.Model):
    """
    Stores ingested student records.
    Optimized with indexes for common queries.
    """

    job = models.ForeignKey(
        IngestionJob, on_delete=models.CASCADE, related_name="records", db_index=True
    )

    # Student Information
    student_id = models.CharField(max_length=50, db_index=True)
    first_name = models.CharField(max_length=100)
    last_name = models.CharField(max_length=100)
    email = models.EmailField(max_length=255)
    phone = models.CharField(max_length=20, blank=True)
    date_of_birth = models.DateField(null=True, blank=True)

    # Academic Information
    grade = models.CharField(max_length=20)
    section = models.CharField(max_length=10, blank=True)
    roll_number = models.CharField(max_length=50, blank=True)

    # Address
    address = models.TextField(blank=True)
    city = models.CharField(max_length=100, blank=True)
    state = models.CharField(max_length=100, blank=True)
    postal_code = models.CharField(max_length=20, blank=True)
    country = models.CharField(max_length=100, default="India")

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "student_records"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["student_id", "email"]),
            models.Index(fields=["grade", "section"]),
            models.Index(fields=["job", "created_at"]),
        ]
        # Prevent duplicate student_id within same job
        unique_together = [["job", "student_id"]]

    def __str__(self):
        return f"{self.first_name} {self.last_name} ({self.student_id})"


class IngestionError(models.Model):
    """
    Stores validation errors and failed records for debugging.
    """

    job = models.ForeignKey(
        IngestionJob, on_delete=models.CASCADE, related_name="errors", db_index=True
    )
    record_index = models.IntegerField()
    error_type = models.CharField(max_length=100)
    error_message = models.TextField()
    raw_data = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "ingestion_errors"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["job", "error_type"]),
        ]

    def __str__(self):
        return f"Error in Job {self.job.task_id} - Record {self.record_index}"
