import asyncio
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

logs_storage = {}
logs_lock = asyncio.Lock()

class LogMessage(BaseModel):
    transaction_Id: str
    user_Id: str
    amount: float
    timestamp: str


@app.post("/log")
async def save_log(msg: LogMessage):
    async with logs_lock:
        msg_dict = msg.model_dump() 
        logs_storage[msg.transaction_Id] = msg_dict

        
    return {"status": "success", "transaction_Id": msg.transaction_Id}


@app.get("/logs/{user_Id}")
async def fetch_user_logs(user_Id: str):
    async with logs_lock:
        user_transactions = [
            log_data for log_data in logs_storage.values() 
            if log_data["user_Id"] == user_Id
        ]
        
    return user_transactions

@app.get("/logs")
async def fetch_all_logs():
    async with logs_lock:
        all_logs_snapshot = logs_storage.copy()
        
    return all_logs_snapshot