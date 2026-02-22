import asyncio
import datetime
import os
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Читаємо URL сервісів зі змінних оточення (з фолбеком на локальні адреси)
LOGGING_SERVICE_URL = os.getenv("LOGGING_SERVICE_URL", "http://logging-service:8000")
COUNTER_SERVICE_URL = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8000")

# Глобальні змінні для метрик та клієнта
metrics = {"logging_time_sec": 0.0, "counter_time_sec": 0.0}
metrics_lock = asyncio.Lock()
http_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    # Встановлюємо таймаут, щоб запити не висіли вічно
    http_client = httpx.AsyncClient(timeout=15.0)
    yield
    if http_client:
        await http_client.aclose()

app = FastAPI(lifespan=lifespan)

# --- Моделі ---
class TransactionRequest(BaseModel):
    user_Id: str
    amount: float

# --- Допоміжні функції для безпечного оновлення часу ---
async def update_metrics(logging_elapsed: float = 0.0, counter_elapsed: float = 0.0):
    async with metrics_lock:
        metrics["logging_time_sec"] += logging_elapsed
        metrics["counter_time_sec"] += counter_elapsed

# --- Ендпоінти ---

@app.post("/transaction")
async def process_transaction(req: TransactionRequest):
    # Генеруємо transaction_ID та timestamp згідно з вимогами [cite: 19]
    tx_id = str(uuid.uuid4())
    tx_timestamp = datetime.datetime.now().isoformat()
    
    # Формуємо повідомлення [cite: 20]
    payload = {
        "transaction_Id": tx_id,
        "user_Id": req.user_Id,
        "amount": req.amount,
        "timestamp": tx_timestamp
    }

    # Паралельні запити до обох сервісів [cite: 21]
    log_task = http_client.post(f"{LOGGING_SERVICE_URL}/log", json=payload)
    counter_task = http_client.post(f"{COUNTER_SERVICE_URL}/update", json=payload)
    
    responses = await asyncio.gather(log_task, counter_task, return_exceptions=True)

    # Перевіряємо, чи не впав якийсь із запитів
    for resp in responses:
        if isinstance(resp, Exception):
            raise HTTPException(status_code=500, detail="Microservice communication error")
        resp.raise_for_status()

    log_response, counter_response = responses

    # Витягуємо час виконання кожного запиту (через .elapsed з httpx)
    await update_metrics(
        logging_elapsed=log_response.elapsed.total_seconds(),
        counter_elapsed=counter_response.elapsed.total_seconds()
    )

    # Повертаємо клієнту {transaction_ID, balance} [cite: 24, 57, 58]
    return {
        "transaction_Id": tx_id,
        "balance": counter_response.json().get("balance")
    }


@app.get("/user/{user_Id}")
async def get_user_details(user_Id: str):
    # Зверни увагу: ми просимо логер віддати ТІЛЬКИ логи конкретного юзера! [cite: 61]
    # Це головна перевага твоєї архітектури над кодом однокласника.
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

    # Повертаємо {balance, <transacactions>} [cite: 61, 81]
    return {
        "balance": counter_response.json().get("balance"),
        "transactions": log_response.json()
    }


@app.get("/accounts")
async def get_all_accounts():
    # Повертає значення балансу всіх клієнтів [cite: 62]
    response = await http_client.get(f"{COUNTER_SERVICE_URL}/accounts")
    response.raise_for_status()
    
    await update_metrics(counter_elapsed=response.elapsed.total_seconds())
    
    return response.json()


# --- Ендпоінти для метрик [cite: 94] ---

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