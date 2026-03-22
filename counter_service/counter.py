import os
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

DB_URL = os.getenv("DB_URL", "postgresql://lab_user:lab_password@postgres-db:5432/counter_db")

db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    
    try:
        db_pool = await asyncpg.create_pool(dsn=DB_URL, min_size=5, max_size=20)
        
        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    user_id TEXT PRIMARY KEY,
                    balance DOUBLE PRECISION NOT NULL DEFAULT 0.0
                )
            ''')
        print("Data base initialized ", flush=True)
    except Exception as e:
        print(f"Error initializing database: {e}", flush=True)
        raise e

    yield 

    if db_pool is not None:
        await db_pool.close()

app = FastAPI(lifespan=lifespan)

class TransactionPayload(BaseModel):
    transaction_Id: str
    user_Id: str
    amount: float
    timestamp: str

@app.post("/update")
async def process_balance_update(payload: TransactionPayload):
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Database pool is not initialized")

    query = '''
        INSERT INTO accounts (user_id, balance)
        VALUES ($1, $2)
        ON CONFLICT (user_id)
        DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
        RETURNING balance;
    '''
    
    async with db_pool.acquire() as conn:
        new_balance = await conn.fetchval(query, payload.user_Id, payload.amount)

    return {"balance": new_balance}


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