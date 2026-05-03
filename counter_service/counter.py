import os
import json
import asyncio
import asyncpg
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer

DB_URL = os.getenv("DATABASE_URL")
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_BALANCE_UPDATES_TOPIC")
KAFKA_GROUP = os.getenv("KAFKA_BALANCE_UPDATES_GROUP", "counter-group")

db_pool = None
kafka_task = None

async def consume_kafka():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP,
        enable_auto_commit=False, 
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for attempt in range(1, 31):
        try:
            await consumer.start()
            print(f"Successfully connected to Kafka on attempt {attempt}!", flush=True)
            break
        except Exception as e:
            print(f"Attempt {attempt}: Waiting for Kafka. {e}", flush=True)
            await asyncio.sleep(2)
    else:
        print("Failed to connect to Kafka after 30 attempts.", flush=True)
        return

    try:
        async for msg in consumer:
            payload = msg.value
            u_id = payload.get("user_id") or payload.get("user_Id")
            amount = payload.get("amount")

            if not u_id or amount is None:
                await consumer.commit() 
                continue

            try:
                query = '''
                    INSERT INTO accounts (user_id, balance)
                    VALUES ($1, $2)
                    ON CONFLICT (user_id)
                    DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
                    RETURNING balance;
                '''
                async with db_pool.acquire() as conn:
                    new_balance = await conn.fetchval(query, u_id, float(amount))
                    print(f"Processed tx. User: {u_id}, New Balance: {new_balance}", flush=True)
                
                await consumer.commit()

            except Exception as e:
                print(f"DB Error processing message: {e}. Retrying later.", flush=True)
                await asyncio.sleep(1.0) 

    finally:
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, kafka_task
    
    db_pool = await asyncpg.create_pool(dsn=DB_URL, min_size=1, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                user_id TEXT PRIMARY KEY,
                balance DOUBLE PRECISION NOT NULL DEFAULT 0.0
            )
        ''')
    print("Database pool initialized", flush=True)
    
    kafka_task = asyncio.create_task(consume_kafka())
    yield
    
    print("Shutting down counter-service...", flush=True)
    if kafka_task:
        kafka_task.cancel()
        try:
            await kafka_task
        except asyncio.CancelledError:
            pass
    if db_pool:
        await db_pool.close()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "service": "counter-service"}

@app.get("/user/{user_Id}")
async def get_balance(user_Id: str):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not ready")
        
    async with db_pool.acquire() as conn:
        val = await conn.fetchval("SELECT balance FROM accounts WHERE user_id = $1", user_Id)
    return {"balance": val if val is not None else 0.0}

@app.get("/accounts")
async def get_accounts():
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not ready")
        
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, balance FROM accounts")
    return {r['user_id']: r['balance'] for r in rows}