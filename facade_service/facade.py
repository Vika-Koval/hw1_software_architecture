import asyncio
import datetime
import os
import uuid
import random
import json
import time
import grpc
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

LOGGING_TARGETS = [t.strip() for t in os.getenv("LOGGING_GRPC_TARGETS", "logging-service-1:50051").split(",")]
COUNTER_SERVICE_URL = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8001")

metrics = {"logging_time_sec": 0.0, "counter_time_sec": 0.0}
metrics_lock = asyncio.Lock()
http_client = None
grpc_channels = {}  

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, grpc_channels
    
    http_client = httpx.AsyncClient(timeout=15.0)
    
    print(f"Facade Service: Initializing gRPC clients for targets: {LOGGING_TARGETS}", flush=True)
    for target in LOGGING_TARGETS:
        grpc_channels[target] = grpc.aio.insecure_channel(target)
    print(f"Facade Service: Initialized {len(grpc_channels)} gRPC clients", flush=True)

    yield 

    for channel in grpc_channels.values():
        await channel.close()
    if http_client:
        await http_client.aclose()
    print(f"Facade Service: Shutdown complete", flush=True)

app = FastAPI(lifespan=lifespan)

class TransactionRequest(BaseModel):
    user_Id: str
    amount: float

async def update_metrics(log_time: float = 0.0, count_time: float = 0.0):
    async with metrics_lock:
        metrics["logging_time_sec"] += log_time
        metrics["counter_time_sec"] += count_time

async def send_grpc_with_failover(method_name: str, payload: dict) -> tuple[dict, float]:
    targets = list(grpc_channels.keys())
    random.shuffle(targets)
    
    request_bytes = json.dumps(payload).encode("utf-8")
    last_error = None

    for target in targets:
        channel = grpc_channels[target]
        try:
            print(f"Facade: Attempting to log transaction to {target}", flush=True)
            start_timer = time.perf_counter()
            
            method_stub = channel.unary_unary(
                f'/LoggingService/{method_name}',
                request_serializer=lambda x: x,
                response_deserializer=lambda x: x,
            )
            response_bytes = await method_stub(request_bytes, timeout=4.0)
            
            elapsed_time = time.perf_counter() - start_timer
            print(f"Facade: Successfully logged to {target}: {response_bytes.decode('utf-8')}", flush=True)
            return json.loads(response_bytes.decode("utf-8")), elapsed_time
            
        except grpc.RpcError as e:
            print(f"Facade: Error calling {target}: {e}", flush=True)
            last_error = e
            continue
            
    raise HTTPException(status_code=503, detail=f"All logging-service instances unavailable: {last_error}")


@app.post("/transaction")
async def process_transaction(req: TransactionRequest):
    tx_id = str(uuid.uuid4())
    payload = {
        "transaction_Id": tx_id,
        "user_Id": req.user_Id,
        "amount": req.amount,
        "timestamp": datetime.datetime.now().isoformat()
    }

    # 1. Послідовно виконуємо логування
    log_data, log_time = await send_grpc_with_failover("SaveLog", payload)
    
    # 2. Тільки після завершення логування запускаємо лічильник
    start_counter = time.perf_counter()
    counter_response = await http_client.post(f"{COUNTER_SERVICE_URL}/update", json=payload)
    counter_time = time.perf_counter() - start_counter
    
    counter_response.raise_for_status()

    await update_metrics(log_time, counter_time)

    return {
        "transaction_Id": tx_id,
        "balance": counter_response.json().get("balance")
    }
@app.get("/user/{user_Id}")
async def get_user_details(user_Id: str):
    # 1. Послідовно отримуємо логи
    log_data, log_time = await send_grpc_with_failover("FetchLogs", {})
    
    # 2. Тільки після цього йдемо в counter-service
    start_counter = time.perf_counter()
    counter_response = await http_client.get(f"{COUNTER_SERVICE_URL}/user/{user_Id}")
    counter_time = time.perf_counter() - start_counter
    
    counter_response.raise_for_status()

    user_transactions = [log for log in log_data if log.get("user_Id") == user_Id]

    await update_metrics(log_time, counter_time)

    return {
        "balance": counter_response.json().get("balance"),
        "transactions": user_transactions
    }

@app.get("/accounts")
async def get_all_accounts():
    start_counter = time.perf_counter()
    response = await http_client.get(f"{COUNTER_SERVICE_URL}/accounts")
    counter_time = time.perf_counter() - start_counter
    
    response.raise_for_status()
    await update_metrics(count_time=counter_time)
    
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