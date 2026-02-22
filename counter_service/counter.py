import asyncio
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

balances = {} 
balance_lock = asyncio.Lock()

class TransactionPayload(BaseModel):
    transaction_Id: str
    user_Id: str
    amount: float
    timestamp: str

@app.post("/update")
async def process_balance_update(payload: TransactionPayload):
    async with balance_lock:
        if payload.user_Id not in balances:
            balances[payload.user_Id] = 0.0
        
        balances[payload.user_Id] += payload.amount
        current_balance = balances[payload.user_Id]
        
    return {"balance": current_balance}


@app.get("/user/{user_Id}")
async def get_user_balance(user_Id: str):
    async with balance_lock:
        current_balance = balances.get(user_Id, 0.0)
    
    return {"balance": current_balance}


@app.get("/accounts")
async def get_all_accounts():
    async with balance_lock:
        accounts_snapshot = balances.copy()
        
    return accounts_snapshot