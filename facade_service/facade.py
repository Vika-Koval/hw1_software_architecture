import asyncio
import datetime
import os
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

LOGGING_SERVICE_URL = os.getenv("LOGGING_SERVICE_URL", "http://logging-service:8000")
COUNTER_SERVICE_URL = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8000")

metrics = {"logging_time_sec": 0.0, "counter_time_sec": 0.0}
metrics_lock = asyncio.Lock()
http_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=15.0)
    yield
    if http_client:
        await http_client.aclose()

app = FastAPI(lifespan=lifespan)

class TransactionRequest(BaseModel):
    user_Id: str
    amount: float

async def update_metrics(logging_elapsed: float = 0.0, counter_elapsed: float = 0.0):
    async with metrics_lock:
        metrics["logging_time_sec"] += logging_elapsed
        metrics["counter_time_sec"] += counter_elapsed


@app.post("/transaction")
async def process_transaction(req: TransactionRequest):
    tx_id = str(uuid.uuid4())
    tx_timestamp = datetime.datetime.now().isoformat()
    
    payload = {
        "transaction_Id": tx_id,
        "user_Id": req.user_Id,
        "amount": req.amount,
        "timestamp": tx_timestamp
    }

    log_task = http_client.post(f"{LOGGING_SERVICE_URL}/log", json=payload)
    counter_task = http_client.post(f"{COUNTER_SERVICE_URL}/update", json=payload)
    
    responses = await asyncio.gather(log_task, counter_task, return_exceptions=True)

    for resp in responses:
        if isinstance(resp, Exception):
            raise HTTPException(status_code=500, detail="Microservice communication error")
        resp.raise_for_status()

    log_response, counter_response = responses

    await update_metrics(
        logging_elapsed=log_response.elapsed.total_seconds(),
        counter_elapsed=counter_response.elapsed.total_seconds()
    )

    return {
        "transaction_Id": tx_id,
        "balance": counter_response.json().get("balance")
    }


@app.get("/user/{user_Id}")
async def get_user_details(user_Id: str):
    log_task = http_client.get(f"{LOGGING_SERVICE_URL}/logs/{user_Id}")
    counter_task = http_client.get(f"{COUNTER_SERVICE_URL}/user/{user_Id}")

    responses = await asyncio.gather(log_task, counter_task, return_exceptions=True)

    for resp in responses:
        if isinstance(resp, Exception):
            raise HTTPException(status_code=500, detail="Microservice communication error")
        resp.raise_for_status()

    log_response, counter_response = responses

    await update_metrics(
        logging_elapsed=log_response.elapsed.total_seconds(),
        counter_elapsed=counter_response.elapsed.total_seconds()
    )

    return {
        "balance": counter_response.json().get("balance"),
        "transactions": log_response.json()
    }


@app.get("/accounts")
async def get_all_accounts():
    response = await http_client.get(f"{COUNTER_SERVICE_URL}/accounts")
    response.raise_for_status()
    
    await update_metrics(counter_elapsed=response.elapsed.total_seconds())
    
    return response.json()



@app.get("/metrics")
async def get_performance_metrics():
    async with metrics_lock:
        return metrics.copy()

@app.post("/metrics/reset")
async def reset_performance_metrics():
    async with metrics_lock:
        metrics["logging_time_sec"] = 0.0
        metrics["counter_time_sec"] = 0.0
    return {"status": "Metrics have been successfully reset"}