"""
Pytest fixtures for ingestion tests.
"""
import pytest
from faker import Faker

fake = Faker()


@pytest.fixture
def sample_student_record():
    """Generate a single valid student record."""
    return {
        "student_id": fake.unique.bothify(text="STU####"),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number()[:20],
        "date_of_birth": fake.date_of_birth(minimum_age=5, maximum_age=18).isoformat(),
        "grade": fake.random_element(elements=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]),
        "section": fake.random_element(elements=["A", "B", "C"]),
        "roll_number": str(fake.random_int(min=1, max=100)),
        "address": fake.address()[:200],
        "city": fake.city(),
        "state": fake.state(),
        "postal_code": fake.postcode(),
        "country": "India",
    }


@pytest.fixture
def sample_student_records(sample_student_record):
    """Generate multiple valid student records."""

    def _generate(count=10):
        records = []
        for _ in range(count):
            record = sample_student_record.copy()
            record["student_id"] = fake.unique.bothify(text="STU####")
            record["email"] = fake.email()
            records.append(record)
        return records

    return _generate


@pytest.fixture
def ingestion_job():
    """Create a test ingestion job."""
    from apps.ingestion.models import IngestionJob
    
    return IngestionJob.objects.create(
        task_id=fake.uuid4(), total_records=100, status=IngestionJob.Status.PENDING
    )


@pytest.fixture
def completed_ingestion_job():
    """Create a completed test ingestion job."""
    from apps.ingestion.models import IngestionJob
    
    return IngestionJob.objects.create(
        task_id=fake.uuid4(),
        total_records=100,
        processed_records=95,
        failed_records=5,
        status=IngestionJob.Status.COMPLETED,
    )
