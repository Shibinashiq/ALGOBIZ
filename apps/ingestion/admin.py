"""
Admin interface for ingestion models.
"""
from django.contrib import admin

from .models import IngestionError, IngestionJob, StudentRecord


@admin.register(IngestionJob)
class IngestionJobAdmin(admin.ModelAdmin):
    list_display = [
        "task_id",
        "status",
        "total_records",
        "processed_records",
        "failed_records",
        "progress_percentage",
        "created_at",
        "duration",
    ]
    list_filter = ["status", "created_at"]
    search_fields = ["task_id"]
    readonly_fields = [
        "task_id",
        "created_at",
        "started_at",
        "completed_at",
        "progress_percentage",
        "duration",
    ]
    date_hierarchy = "created_at"


@admin.register(StudentRecord)
class StudentRecordAdmin(admin.ModelAdmin):
    list_display = [
        "student_id",
        "first_name",
        "last_name",
        "email",
        "grade",
        "section",
        "created_at",
    ]
    list_filter = ["grade", "section", "created_at"]
    search_fields = ["student_id", "first_name", "last_name", "email"]
    readonly_fields = ["created_at", "updated_at"]
    date_hierarchy = "created_at"


@admin.register(IngestionError)
class IngestionErrorAdmin(admin.ModelAdmin):
    list_display = ["job", "record_index", "error_type", "created_at"]
    list_filter = ["error_type", "created_at"]
    search_fields = ["job__task_id", "error_message"]
    readonly_fields = ["created_at"]
    date_hierarchy = "created_at"
