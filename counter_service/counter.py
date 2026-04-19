import os
import json
import asyncio
import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer

DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@postgres-db:5432/counterdb")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8500")
INSTANCE_ID = os.getenv("INSTANCE_ID", "counter-service-1")
SELF_ADDRESS = os.getenv("SELF_ADDRESS", "http://counter-service:8001")

db_pool = None
kafka_task = None

async def fetch_config_value(key: str) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{CONFIG_SERVER_URL}/config/{key}")
        response.raise_for_status()
        return response.json()["value"]

async def consume_balance_updates():
    bootstrap_servers = await fetch_config_value("kafka.bootstrap.servers")
    topic_name = await fetch_config_value("kafka.balance_updates.topic")
    group_id = await fetch_config_value("kafka.balance_updates.group_id")

    consumer = AIOKafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset="earliest"
    )

    connected = False
    while not connected:
        try:
            await consumer.start()
            connected = True
            print("Successfully connected to Kafka Broker!", flush=True)
        except Exception as e:
            print(f"Waiting for Kafka to be ready. Error: {e}", flush=True)
            await asyncio.sleep(3)

    try:
        async for msg in consumer:
            payload = msg.value
            user_id = payload.get("user_Id")
            amount = payload.get("amount")

            if not user_id or amount is None:
                continue

            print(f"Consumed message from Kafka: user={user_id}, amount={amount}", flush=True)

            query = '''
                INSERT INTO accounts (user_id, balance)
                VALUES ($1, $2)
                ON CONFLICT (user_id)
                DO UPDATE SET balance = accounts.balance + EXCLUDED.balance;
            '''
            async with db_pool.acquire() as conn:
                await conn.execute(query, user_id, float(amount))
                
    finally:
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, kafka_task
    
    try:
        db_pool = await asyncpg.create_pool(dsn=DB_URL, min_size=5, max_size=20)
        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    user_id TEXT PRIMARY KEY,
                    balance DOUBLE PRECISION NOT NULL DEFAULT 0.0
                )
            ''')
        print("Data base initialized", flush=True)
    except Exception as e:
        print(f"Error initializing database: {e}", flush=True)
        raise e

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{CONFIG_SERVER_URL}/register",
                json={
                    "service_name": "counter-service",
                    "instance_id": INSTANCE_ID,
                    "address": SELF_ADDRESS
                }
            )
        print("Successfully registered in Config Server", flush=True)
    except Exception as e:
        print(f"Failed to register in Config Server: {e}", flush=True)

    kafka_task = asyncio.create_task(consume_balance_updates())

    yield 

    if kafka_task is not None:
        kafka_task.cancel()
    if db_pool is not None:
        await db_pool.close()

app = FastAPI(lifespan=lifespan)


@app.get("/user/{user_Id}")
async def get_user_balance(user_Id: str):
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    query = 'SELECT balance FROM accounts WHERE user_id = $1;'
    
    async with db_pool.acquire() as conn:
        balance = await conn.fetchval(query, user_Id)

    return {"balance": balance if balance is not None else 0.0}

@app.get("/accounts")
async def get_all_accounts():
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    query = 'SELECT user_id, balance FROM accounts;'
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
        
    accounts_snapshot = {row['user_id']: row['balance'] for row in rows}
        
    return accounts_snapshot