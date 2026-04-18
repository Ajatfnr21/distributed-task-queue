"""Tests for Distributed Task Queue"""

import pytest
from fastapi.testclient import TestClient

from worker import app, celery_app, process_data_task, send_notification_task

client = TestClient(app)


class TestHealth:
    def test_health_check(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_info(self):
        response = client.get("/")
        assert response.status_code == 200
        assert "Distributed Task Queue" in response.json()["name"]
        assert "available_tasks" in response.json()


class TestTaskSubmission:
    def test_submit_valid_task(self):
        request = {
            "task_name": "process_data_task",
            "args": [{"key": "value"}, "standard"],
            "kwargs": {},
            "priority": 5
        }
        response = client.post("/tasks/submit", json=request)
        assert response.status_code == 200
        data = response.json()
        assert "task_id" in data
        assert data["status"] == "submitted"

    def test_submit_unknown_task(self):
        request = {
            "task_name": "unknown_task",
            "args": [],
            "kwargs": {}
        }
        response = client.post("/tasks/submit", json=request)
        assert response.status_code == 400

    def test_get_task_status(self):
        # Submit a task first
        request = {
            "task_name": "process_data_task",
            "args": [{"test": "data"}],
            "kwargs": {}
        }
        submit_response = client.post("/tasks/submit", json=request)
        task_id = submit_response.json()["task_id"]
        
        # Get status
        response = client.get(f"/tasks/{task_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == task_id
        assert "status" in data


class TestWorkers:
    def test_list_workers(self):
        response = client.get("/workers")
        assert response.status_code == 200
        data = response.json()
        assert "workers" in data

    def test_list_queues(self):
        response = client.get("/queues")
        assert response.status_code == 200
        data = response.json()
        assert "queues" in data
        assert "broker_status" in data


class TestBatch:
    def test_submit_batch(self):
        requests = [
            {"task_name": "process_data_task", "args": [{"id": 1}]},
            {"task_name": "process_data_task", "args": [{"id": 2}]},
            {"task_name": "process_data_task", "args": [{"id": 3}]}
        ]
        response = client.post("/batch/submit", json=requests)
        assert response.status_code == 200
        data = response.json()
        assert "group_id" in data
        assert data["tasks_count"] == 3
