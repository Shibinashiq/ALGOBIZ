# Distributed Task Processing System with Scalability

**Django system for school management with 1,200,000 users and 300,000 concurrent users**

## Core Requirements Implementation

### 1. High-Volume Ingestion API ✅
- **Endpoint**: `POST /api/data/ingest/`
- **Accepts**: JSON payload up to 1,000 records
- **Returns**: Immediately (asynchronously) with task ID

### 2. Asynchronous Processing ✅
- **System**: Celery message queue
- **Validates**: Data schema of 1,000 records
- **Simulates**: External API call (time.sleep(0.5)) for every 100 records
- **Persists**: Validated records to PostgreSQL database

### 3. Real-time Status Check ✅
- **Endpoint**: `GET /api/data/status/<task_id>/`
- **Reports**: PENDING, PROCESSING (70% complete), COMPLETED, FAILED

### 4. Concurrency Optimization ✅
- **Handles**: 10 concurrent ingestion jobs
- **Non-blocking**: Main web application thread
- **No contention**: Database optimized for concurrent access

## Quick Start

```bash
# Start services
docker-compose up -d

# Test 1000 records ingestion
python3 test_api.py

# Run unit tests
docker-compose exec web pytest
```

## Testing the APIs

### Submit 1000 Records
```bash
python3 test_api.py
```

### Check Status with Task ID
```bash
curl http://localhost:8000/api/data/status/<task_id>/
```

## API Endpoints

- `POST /api/data/ingest/` - Submit up to 1,000 records
- `GET /api/data/status/<task_id>/` - Check job status
- `GET /api/health/` - Health check

## Access Points

- API: http://localhost:8000
- Admin: http://localhost:8000/admin/
- API Docs: http://localhost:8000/api/docs/
- Celery Monitor: http://localhost:5555

## Project Structure

```
Task/
├── manage.py              # Django management
├── config/                # Settings & URLs
├── apps/ingestion/        # Main app
│   ├── models.py         # Database models
│   ├── views.py          # API endpoints
│   ├── tasks.py          # Background jobs
│   └── tests/            # Unit tests
├── docker-compose.yml    # Docker setup
└── requirements.txt      # Dependencies
```
