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
from aiokafka import AIOKafkaProducer

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8500")

metrics = {"logging_time_sec": 0.0, "counter_time_sec": 0.0}
metrics_lock = asyncio.Lock()
http_client = None
kafka_producer = None
kafka_topic = "balance-updates"
grpc_channels = {}  

async def get_config_value(key: str) -> str:
    res = await http_client.get(f"{CONFIG_SERVER_URL}/config/{key}")
    res.raise_for_status()
    return res.json()["value"]

async def discover_services(service_name: str) -> list[str]:
    try:
        res = await http_client.get(f"{CONFIG_SERVER_URL}/services/{service_name}")
        res.raise_for_status()
        instances_dict = res.json().get("instances", {})
        return list(instances_dict.values())
    except Exception as e:
        print(f"Facade: Error discovering {service_name}: {e}", flush=True)
        return []

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, kafka_producer, kafka_topic
    
    http_client = httpx.AsyncClient(timeout=10.0)
    
    print("Fetching Kafka configuration", flush=True)
    for _ in range(10):
        try:
            kafka_broker = await get_config_value("kafka.bootstrap.servers")
            kafka_topic = await get_config_value("kafka.balance_updates.topic")
            break
        except Exception:
            await asyncio.sleep(2)
            
    print(f"Initializing Kafka Producer at {kafka_broker}", flush=True)
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await kafka_producer.start()

    yield 

    print("Shutdown complete", flush=True)
    if kafka_producer:
        await kafka_producer.stop()
    for channel in grpc_channels.values():
        await channel.close()
    if http_client:
        await http_client.aclose()

app = FastAPI(lifespan=lifespan)

class TransactionRequest(BaseModel):
    user_Id: str
    amount: float

async def update_metrics(log_time: float = 0.0, count_time: float = 0.0):
    async with metrics_lock:
        metrics["logging_time_sec"] += log_time
        metrics["counter_time_sec"] += count_time

def get_grpc_channel(target: str):
    if target not in grpc_channels:
        grpc_channels[target] = grpc.aio.insecure_channel(target)
    return grpc_channels[target]

async def send_grpc_with_failover(method_name: str, payload: dict) -> tuple[dict, float]:
    targets = await discover_services("logging-service")
    if not targets:
        raise HTTPException(status_code=503, detail="No logging-service instances found")

    random.shuffle(targets)
    request_bytes = json.dumps(payload).encode("utf-8")
    last_error = None

    for target in targets:
        channel = get_grpc_channel(target)
        try:
            start_timer = time.perf_counter()
            method_stub = channel.unary_unary(
                f'/LoggingService/{method_name}',
                request_serializer=lambda x: x,
                response_deserializer=lambda x: x,
            )
            response_bytes = await method_stub(request_bytes, timeout=4.0)
            elapsed_time = time.perf_counter() - start_timer
            
            print(f"Successfully called {method_name} on {target}", flush=True)
            return json.loads(response_bytes.decode("utf-8")), elapsed_time
            
        except grpc.RpcError as e:
            last_error = e
            continue
            
    raise HTTPException(status_code=503, detail=f"All logging services failed. Last error: {last_error}")

async def call_counter_service(endpoint: str) -> tuple[dict, float]:
    targets = await discover_services("counter-service")
    if not targets:
        raise HTTPException(status_code=503, detail="No counter-service instances found")

    random.shuffle(targets)
    last_error = None

    for target in targets:
        try:
            start_timer = time.perf_counter()
            response = await http_client.get(f"{target}{endpoint}")
            response.raise_for_status()
            elapsed_time = time.perf_counter() - start_timer
            return response.json(), elapsed_time
        except Exception as e:
            last_error = e
            continue

    raise HTTPException(status_code=503, detail=f"All counter services failed. Last error: {last_error}")

@app.post("/transaction")
async def process_transaction(req: TransactionRequest):
    tx_id = str(uuid.uuid4())
    payload = {
        "transaction_Id": tx_id,
        "user_Id": req.user_Id,
        "amount": req.amount,
        "timestamp": datetime.datetime.now().isoformat()
    }

    log_data, log_time = await send_grpc_with_failover("SaveLog", payload)
    
    start_kafka = time.perf_counter()
    await kafka_producer.send_and_wait(kafka_topic, value=payload)
    kafka_time = time.perf_counter() - start_kafka

    await update_metrics(log_time, kafka_time)

    return {
        "transaction_Id": tx_id,
        "status": "queued"
    }

@app.get("/user/{user_Id}")
async def get_user_details(user_Id: str):
    log_task = asyncio.create_task(send_grpc_with_failover("FetchLogs", {}))
    counter_task = asyncio.create_task(call_counter_service(f"/user/{user_Id}"))

    results = await asyncio.gather(log_task, counter_task, return_exceptions=True)

    if isinstance(results[0], Exception): raise results[0]
    if isinstance(results[1], Exception): raise results[1]

    (log_data, log_time), (counter_data, counter_time) = results

    user_transactions = [log for log in log_data if log.get("user_Id") == user_Id]
    await update_metrics(log_time, counter_time)

    return {
        "balance": counter_data.get("balance"),
        "transactions": user_transactions
    }

@app.get("/accounts")
async def get_all_accounts():
    counter_data, counter_time = await call_counter_service("/accounts")
    await update_metrics(count_time=counter_time)
    return counter_data

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