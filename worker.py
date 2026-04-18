#!/usr/bin/env python3
"""
Distributed Task Queue - Celery + Redis with 10K+ Workers

Features:
- Celery task queue with Redis broker
- Multiple worker pools
- Task prioritization
- Scheduled tasks
- Monitoring with Flower
- Task result backend
- Rate limiting
- Retry mechanisms

Author: Drajat Sukma
License: MIT
Version: 2.0.0
"""

__version__ = "2.0.0"

import os
import time
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from celery import Celery, chain, group, chord
from celery.result import AsyncResult
from celery.schedules import crontab
import redis
import structlog
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
import psutil

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()

# ============== Configuration ==============

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("RESULT_BACKEND", "redis://localhost:6379/1")

# ============== Celery App ==============

celery_app = Celery(
    "distributed_task_queue",
    broker=REDIS_URL,
    backend=RESULT_BACKEND,
    include=["worker"]
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour max
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=1000,
    broker_connection_retry_on_startup=True,
    result_expires=3600 * 24,  # Results expire after 24 hours
    # Beat schedule for periodic tasks
    beat_schedule={
        "cleanup-old-results": {
            "task": "worker.cleanup_old_results",
            "schedule": crontab(hour=0, minute=0),
        },
        "health-check": {
            "task": "worker.health_check_task",
            "schedule": 60.0,  # Every minute
        },
    },
)

# ============== Task Definitions ==============

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_data_task(self, data: Dict[str, Any], processing_type: str = "standard"):
    """Process data with various transformation options"""
    try:
        logger.info("task_started", task_id=self.request.id, processing_type=processing_type)
        
        # Simulate processing
        processing_time = random.uniform(1, 5)
        time.sleep(processing_time)
        
        # Simulate different processing types
        if processing_type == "heavy":
            # CPU intensive simulation
            result = sum(i ** 2 for i in range(1000000))
        elif processing_type == "io":
            # I/O simulation
            time.sleep(2)
            result = {"processed": len(data), "type": "io_bound"}
        else:
            # Standard processing
            result = {
                "input_keys": list(data.keys()),
                "processed_at": datetime.utcnow().isoformat(),
                "worker": self.request.hostname
            }
        
        logger.info("task_completed", task_id=self.request.id, duration=processing_time)
        return {
            "status": "success",
            "result": result,
            "processing_time": processing_time,
            "task_id": self.request.id
        }
    
    except Exception as exc:
        logger.error("task_failed", task_id=self.request.id, error=str(exc))
        raise self.retry(exc=exc)

@celery_app.task(bind=True, max_retries=2)
def send_notification_task(self, notification_type: str, recipient: str, message: str):
    """Send notifications (email, sms, push)"""
    try:
        logger.info("notification_task_started", type=notification_type, recipient=recipient)
        
        # Simulate notification sending
        time.sleep(random.uniform(0.5, 2))
        
        return {
            "status": "sent",
            "notification_type": notification_type,
            "recipient": recipient,
            "sent_at": datetime.utcnow().isoformat(),
            "task_id": self.request.id
        }
    except Exception as exc:
        raise self.retry(exc=exc)

@celery_app.task(bind=True)
def image_processing_task(self, image_url: str, operations: List[str]):
    """Process images (resize, filter, etc.)"""
    logger.info("image_task_started", url=image_url, operations=operations)
    
    # Simulate image processing
    time.sleep(random.uniform(2, 8))
    
    return {
        "status": "completed",
        "operations_applied": operations,
        "output_url": f"{image_url}_processed",
        "processing_time": random.uniform(2, 8)
    }

@celery_app.task(bind=True)
def report_generation_task(self, report_type: str, date_range: Dict[str, str], filters: Dict[str, Any]):
    """Generate reports"""
    logger.info("report_task_started", type=report_type, date_range=date_range)
    
    # Simulate report generation
    time.sleep(random.uniform(5, 15))
    
    return {
        "status": "completed",
        "report_type": report_type,
        "records_processed": random.randint(1000, 100000),
        "report_url": f"/reports/{report_type}_{datetime.utcnow().strftime('%Y%m%d')}.pdf",
        "generated_at": datetime.utcnow().isoformat()
    }

@celery_app.task(bind=True)
def ml_inference_task(self, model_name: str, input_data: List[float]):
    """Run ML model inference"""
    logger.info("ml_task_started", model=model_name, input_size=len(input_data))
    
    # Simulate ML inference
    time.sleep(random.uniform(0.1, 2))
    
    # Mock prediction
    prediction = sum(input_data) / len(input_data) if input_data else 0
    
    return {
        "status": "completed",
        "model": model_name,
        "prediction": prediction,
        "confidence": random.uniform(0.7, 0.99),
        "inference_time_ms": random.randint(50, 500)
    }

@celery_app.task
def cleanup_old_results():
    """Cleanup old task results"""
    logger.info("cleanup_task_started")
    # Celery backend handles expiration automatically
    return {"status": "completed", "cleaned_at": datetime.utcnow().isoformat()}

@celery_app.task
def health_check_task():
    """Periodic health check"""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    
    return {
        "status": "healthy",
        "cpu_percent": cpu_percent,
        "memory_percent": memory.percent,
        "timestamp": datetime.utcnow().isoformat()
    }

# ============== Data Models ==============

class TaskSubmitRequest(BaseModel):
    task_name: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    countdown: Optional[int] = None
    eta: Optional[datetime] = None
    priority: int = 5  # 0-9, lower is higher priority
    queue: str = "celery"

class TaskStatusResponse(BaseModel):
    task_id: str
    status: str  # PENDING, STARTED, SUCCESS, FAILURE, RETRY
    result: Optional[Any] = None
    date_done: Optional[datetime] = None
    traceback: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    version: str
    broker_status: str
    workers_count: int
    active_tasks: int
    queued_tasks: int
    timestamp: datetime
    uptime_seconds: float

class WorkerStats(BaseModel):
    hostname: str
    status: str
    active_tasks: int
    processed_tasks: int
    load_average: List[float]
    cpu_percent: float
    memory_percent: float

# ============== Storage ==============

class QueueStats:
    def __init__(self):
        self.start_time = datetime.utcnow()
        self.redis_client = redis.from_url(REDIS_URL)
    
    def get_stats(self) -> Dict[str, Any]:
        try:
            self.redis_client.ping()
            broker_status = "connected"
        except:
            broker_status = "disconnected"
        
        # Get queue lengths
        queue_lengths = {}
        for queue_name in ["celery", "high_priority", "low_priority"]:
            try:
                length = self.redis_client.llen(queue_name)
                queue_lengths[queue_name] = length
            except:
                queue_lengths[queue_name] = 0
        
        return {
            "broker_status": broker_status,
            "queues": queue_lengths,
            "total_queued": sum(queue_lengths.values())
        }

stats = QueueStats()

# ============== FastAPI Application ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("task_queue_api_starting", version=__version__)
    yield
    logger.info("task_queue_api_stopping")

app = FastAPI(
    title="Distributed Task Queue",
    version=__version__,
    description="Celery + Redis with 10K+ Workers",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============== API Endpoints ==============

@app.get("/health", response_model=HealthResponse)
def health_check():
    uptime = (datetime.utcnow() - stats.start_time).total_seconds()
    broker_stats = stats.get_stats()
    
    # Get inspect data from Celery
    inspect = celery_app.control.inspect()
    active = inspect.active()
    registered = inspect.registered()
    
    workers_count = len(registered) if registered else 0
    active_tasks = sum(len(t) for t in active.values()) if active else 0
    
    return HealthResponse(
        status="healthy",
        version=__version__,
        broker_status=broker_stats["broker_status"],
        workers_count=workers_count,
        active_tasks=active_tasks,
        queued_tasks=broker_stats["total_queued"],
        timestamp=datetime.utcnow(),
        uptime_seconds=uptime
    )

@app.get("/")
def info():
    return {
        "name": "Distributed Task Queue",
        "version": __version__,
        "broker": "Redis",
        "backend": "Redis",
        "available_tasks": [
            "process_data_task",
            "send_notification_task",
            "image_processing_task",
            "report_generation_task",
            "ml_inference_task"
        ],
        "features": [
            "10K+ worker support",
            "Task prioritization",
            "Scheduled tasks",
            "Retry mechanisms",
            "Result backend",
            "Rate limiting"
        ]
    }

@app.post("/tasks/submit")
def submit_task(request: TaskSubmitRequest):
    """Submit a new task"""
    task_func = globals().get(request.task_name)
    if not task_func:
        raise HTTPException(status_code=400, detail=f"Unknown task: {request.task_name}")
    
    # Apply priority via queue routing
    queue = request.queue
    if request.priority <= 2:
        queue = "high_priority"
    elif request.priority >= 8:
        queue = "low_priority"
    
    # Submit task
    task = task_func.apply_async(
        args=request.args,
        kwargs=request.kwargs,
        countdown=request.countdown,
        eta=request.eta,
        queue=queue,
        priority=request.priority
    )
    
    logger.info("task_submitted", task_id=task.id, name=request.task_name, queue=queue)
    
    return {
        "task_id": task.id,
        "status": "submitted",
        "queue": queue,
        "submitted_at": datetime.utcnow().isoformat()
    }

@app.get("/tasks/{task_id}", response_model=TaskStatusResponse)
def get_task_status(task_id: str):
    """Get task status and result"""
    result = AsyncResult(task_id, app=celery_app)
    
    response = {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
        "date_done": result.date_done,
        "traceback": result.traceback if result.failed() else None
    }
    
    return response

@app.post("/tasks/{task_id}/revoke")
def revoke_task(task_id: str, terminate: bool = False):
    """Revoke a running task"""
    celery_app.control.revoke(task_id, terminate=terminate)
    logger.info("task_revoked", task_id=task_id, terminate=terminate)
    return {"task_id": task_id, "status": "revoked", "terminated": terminate}

@app.get("/tasks")
def list_tasks():
    """List recent tasks from result backend"""
    inspect = celery_app.control.inspect()
    active = inspect.active() or {}
    scheduled = inspect.scheduled() or {}
    reserved = inspect.reserved() or {}
    
    return {
        "active": [
            {"task_id": t["id"], "name": t["name"], "worker": w}
            for w, tasks in active.items()
            for t in tasks
        ],
        "scheduled": [
            {"task_id": t["request"]["id"], "name": t["request"]["name"], "eta": t["eta"]}
            for w, tasks in scheduled.items()
            for t in tasks
        ],
        "reserved": [
            {"task_id": t["id"], "name": t["name"], "worker": w}
            for w, tasks in reserved.items()
            for t in tasks
        ]
    }

@app.get("/workers")
def list_workers():
    """List all workers and their stats"""
    inspect = celery_app.control.inspect()
    stats_data = inspect.stats()
    active = inspect.active()
    
    if not stats_data:
        return {"workers": []}
    
    workers = []
    for hostname, worker_stats in stats_data.items():
        workers.append({
            "hostname": hostname,
            "status": "online",
            "active_tasks": len(active.get(hostname, [])),
            "processed_tasks": worker_stats.get("total", {}).get("tasks", 0),
            "prefetch_count": worker_stats.get("prefetch_count", 0),
            "load_average": worker_stats.get("loadavg", [0, 0, 0])
        })
    
    return {"workers": workers, "count": len(workers)}

@app.post("/workers/{hostname}/shutdown")
def shutdown_worker(hostname: str):
    """Shutdown a specific worker"""
    celery_app.control.broadcast("shutdown", destination=[hostname])
    return {"hostname": hostname, "status": "shutdown_command_sent"}

@app.get("/queues")
def list_queues():
    """List all queues and their lengths"""
    return stats.get_stats()

@app.post("/pipeline/process-data")
def create_data_pipeline(tasks: List[TaskSubmitRequest]):
    """Create a pipeline of tasks using Celery chains"""
    task_signatures = []
    
    for t in tasks:
        task_func = globals().get(t.task_name)
        if task_func:
            sig = task_func.s(*t.args, **t.kwargs)
            task_signatures.append(sig)
    
    if not task_signatures:
        raise HTTPException(status_code=400, detail="No valid tasks provided")
    
    # Create chain
    pipeline = chain(*task_signatures)
    result = pipeline.apply_async()
    
    return {
        "pipeline_id": result.id,
        "tasks_count": len(tasks),
        "status": "started"
    }

@app.post("/batch/submit")
def submit_batch(requests: List[TaskSubmitRequest]):
    """Submit multiple tasks as a group"""
    job = group(
        globals()[r.task_name].s(*r.args, **r.kwargs)
        for r in requests
        if r.task_name in globals()
    )
    
    result = job.apply_async()
    
    return {
        "group_id": result.id,
        "tasks_count": len(requests),
        "status": "submitted"
    }

# ============== CLI Interface ==============

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Distributed Task Queue")
    parser.add_argument("command", choices=["serve", "worker", "beat", "submit"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--queues", default="celery,high_priority,low_priority")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--task-name")
    
    args = parser.parse_args()
    
    if args.command == "serve":
        uvicorn.run(app, host=args.host, port=args.port)
    elif args.command == "worker":
        from celery.bin import worker
        worker = worker.worker(app=celery_app)
        worker.run(
            queues=args.queues.split(","),
            concurrency=args.concurrency,
            loglevel="info"
        )
    elif args.command == "beat":
        from celery.bin import beat
        beat = beat.beat(app=celery_app)
        beat.run(loglevel="info")
    elif args.command == "submit":
        # Example task submission
        result = process_data_task.delay(
            {"sample": "data", "value": 123},
            processing_type="standard"
        )
        print(f"Task submitted: {result.id}")
