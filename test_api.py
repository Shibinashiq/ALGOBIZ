#!/usr/bin/env python3
"""Simple API test script"""

import json
import subprocess
import time

def run_curl(cmd):
    """Run curl command and return response"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    except Exception as e:
        return f"Error: {e}"

def generate_1000_records():
    """Generate 1000 student records for testing"""
    records = []
    for i in range(1, 1001):
        record = {
            "student_id": f"STU{i:06d}",
            "first_name": f"Student{i}",
            "last_name": f"LastName{i}",
            "email": f"student{i}@school.edu",
            "phone": f"+91{9000000000 + i}",
            "date_of_birth": "2010-05-15",
            "grade": str((i % 12) + 1),
            "section": ["A", "B", "C"][i % 3],
            "roll_number": str((i % 100) + 1),
            "address": f"{i} School Street, Block {i % 10}",
            "city": "Mumbai",
            "state": "Maharashtra",
            "postal_code": "400001",
            "country": "India"
        }
        records.append(record)
    return records

def test_api():
    """Test the ingestion API with 1000 records"""
    print("=" * 50)
    print("Testing 1000 Records Data Ingestion API")
    print("=" * 50)
    
    # Test 1: Health check
    print("1. Health Check...")
    health = run_curl("curl -s http://localhost:8000/api/health/")
    print(f"   Response: {health}")
    
    # Test 2: Generate and submit 1000 records
    print("\n2. Generating 1000 student records...")
    records = generate_1000_records()
    sample_data = {"records": records}
    print(f"   Generated {len(records)} records")
    
    # Save to temp file
    with open("/tmp/test_data.json", "w") as f:
        json.dump(sample_data, f)
    
    # Submit job
    response = run_curl('curl -s -X POST http://localhost:8000/api/data/ingest/ -H "Content-Type: application/json" -d @/tmp/test_data.json')
    print(f"   Response: {response}")
    
    # Extract task_id
    try:
        data = json.loads(response)
        task_id = data.get("task_id")
        if task_id:
            print(f"   Task ID: {task_id}")
            print(f"   Status: {data.get('status')}")
            print(f"   Total Records: {data.get('total_records')}")
            
            # Test 3: Monitor progress until completion
            print("\n3. Monitoring progress...")
            status = "PENDING"
            attempt = 0
            max_attempts = 30  # Wait up to 60 seconds
            
            while status not in ["COMPLETED", "FAILED"] and attempt < max_attempts:
                time.sleep(2)
                attempt += 1
                
                status_response = run_curl(f"curl -s http://localhost:8000/api/data/status/{task_id}/")
                try:
                    status_data = json.loads(status_response)
                    status = status_data.get("status", "UNKNOWN")
                    progress = status_data.get("progress_percentage", 0)
                    processed = status_data.get("processed_records", 0)
                    
                    print(f"   Attempt {attempt}: {status} - {progress}% ({processed}/1000 records)")
                    
                except json.JSONDecodeError:
                    print(f"   Attempt {attempt}: Error parsing response")
            
            # Test 4: Final results
            print("\n4. Final Results:")
            final_response = run_curl(f"curl -s http://localhost:8000/api/data/status/{task_id}/")
            try:
                final_data = json.loads(final_response)
                print(f"   Status: {final_data.get('status')}")
                print(f"   Total Records: {final_data.get('total_records')}")
                print(f"   Processed: {final_data.get('processed_records')}")
                print(f"   Failed: {final_data.get('failed_records')}")
                print(f"   Duration: {final_data.get('duration', 0):.2f}s")
                
                if final_data.get('status') == 'COMPLETED':
                    throughput = final_data.get('processed_records', 0) / max(final_data.get('duration', 1), 1)
                    print(f"   Throughput: {throughput:.2f} records/sec")
                    print("\n🎉 SUCCESS: All 1000 records processed!")
                else:
                    print(f"\n❌ FAILED: {final_data.get('error_message', 'Unknown error')}")
                    
            except json.JSONDecodeError:
                print("   Error: Could not parse final response")
                
        else:
            print("   Error: No task_id in response")
    except json.JSONDecodeError:
        print("   Error: Invalid JSON response")

if __name__ == "__main__":
    test_api()
