import asyncio
import time
import datetime
import uuid
import json
import os
import grpc

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service:9092")
COUNTER_SERVICE_URL = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8000")
LOGGING_SERVICE_URL = os.getenv("LOGGING_SERVICE_URL", "logging-service:50051").replace("http://", "")

class TransactionRequest(BaseModel):
    user_Id: str
    amount: float

app = FastAPI()

producer: AIOKafkaProducer = None
http_client: httpx.AsyncClient = None

metrics = {
    "logging_time_sec": 0.0,
    "counter_time_sec": 0.0
}
metrics_lock = asyncio.Lock()

@app.on_event("startup")
async def startup_event():
    global producer, http_client
    http_client = httpx.AsyncClient(timeout=5.0)
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()
    await http_client.aclose()

async def update_metrics(log_time=0.0, counter_time=0.0, count_time=None, kafka_time=None) -> None:
    try:
        lt = float(log_time) if log_time is not None else 0.0
    except Exception:
        lt = 0.0

    ct = 0.0
    for val in (counter_time, count_time, kafka_time):
        if val is None:
            continue
        try:
            ct += float(val)
        except Exception:
            continue

    async with metrics_lock:
        metrics["logging_time_sec"] = metrics.get("logging_time_sec", 0.0) + lt
        metrics["counter_time_sec"] = metrics.get("counter_time_sec", 0.0) + ct




async def send_grpc_request(method: str, payload: dict):
    start_time = time.time()
    data = {}
    
    try:
        async with grpc.aio.insecure_channel(LOGGING_SERVICE_URL) as channel:
            unary_unary = channel.unary_unary(
                f"/LoggingService/{method}",
                request_serializer=lambda x: json.dumps(x).encode("utf-8"),
                response_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
            
            response = await unary_unary(payload)
            
            if method == "SaveLog":
                data = {"status": "success"}
                
            elif method == "FetchLogs":
                target_user = payload.get("user_id")
                transactions = []
                
                for tx in response:
                    tx_id = tx.get("transaction_id") or tx.get("transaction_Id")
                    
                    if target_user and tx.get("user_id") != target_user:
                        continue
                        
                    transactions.append({
                        "transaction_id": tx_id,
                        "amount": tx.get("amount")
                    })
                
                data = {"transactions": transactions}

    except Exception as e:
        print(f"Logging service gRPC error: {e}")
        
    return data, time.time() - start_time

async def call_counter_service(endpoint: str):
    start_time = time.time()
    try:
        resp = await http_client.get(f"{COUNTER_SERVICE_URL}{endpoint}")
        data = resp.json() if resp.status_code == 200 else {}
    except Exception as e:
        print(f"Counter service error: {e}")
        data = {}
    return data, time.time() - start_time


@app.post("/transaction")
async def process_transaction(req: TransactionRequest):
    tx_id = str(uuid.uuid4())
    payload = {
        "transaction_id": tx_id,
        "user_id": req.user_Id,
        "amount": req.amount,
        "timestamp": datetime.datetime.now().isoformat()
    }

    _, log_time = await send_grpc_request("SaveLog", payload)

    start_kafka = time.time()
    try:
        meta = await producer.send_and_wait("balance-updates", value=payload)
        offset = meta.offset
    except Exception as e:
        print(f"Kafka error: {e}")
        offset = -1
    kafka_time = time.time() - start_kafka

    await update_metrics(log_time=log_time, kafka_time=kafka_time)

    return {"transaction_id": tx_id, "queued": True, "offset": offset}


@app.get("/user/{user_Id}")
async def get_user_details(user_Id: str):
    log_task = asyncio.create_task(send_grpc_request("FetchLogs", {"user_id": user_Id}))
    counter_task = asyncio.create_task(call_counter_service(f"/user/{user_Id}"))
    
    results = await asyncio.gather(log_task, counter_task, return_exceptions=True)
    
    log_data, log_time = results[0] if not isinstance(results[0], Exception) else ([], 0.0)
    counter_data, counter_time = results[1] if not isinstance(results[1], Exception) else ({}, 0.0)
    
    await update_metrics(log_time=log_time, counter_time=counter_time)
    
    return {
        "user_id": user_Id,
        "balance": counter_data.get("balance", 0.0),
        "transactions": log_data.get("transactions", [])
    }


@app.get("/accounts")
async def get_accounts():
    data, counter_time = await call_counter_service("/accounts")
    await update_metrics(counter_time=counter_time)
    return data


@app.get("/metrics")
async def get_metrics():
    async with metrics_lock:
        return metrics


@app.post("/metrics/reset")
async def reset_metrics():
    async with metrics_lock:
        metrics["logging_time_sec"] = 0.0
        metrics["counter_time_sec"] = 0.0
    return {"status": "metrics reset"}


@app.get("/health")
async def health_check():
    return {"status": "Facade is running"}