import asyncio
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# Хеш-таблиця для зберігання балансів у пам'яті 
balances = {} 
# Блокування для захисту словника від конкурентних читань/записів
balance_lock = asyncio.Lock()

# Наша модель, яка співпадає з тим, що відправляє наш фасад
class TransactionPayload(BaseModel):
    transaction_Id: str
    user_Id: str
    amount: float
    timestamp: str

@app.post("/update")
async def process_balance_update(payload: TransactionPayload):
    # Отримуємо amount та user_Id з повідомлення [cite: 54]
    async with balance_lock:
        if payload.user_Id not in balances:
            balances[payload.user_Id] = 0.0
        
        # Кредитуємо або дебетуємо баланс [cite: 55]
        balances[payload.user_Id] += payload.amount
        current_balance = balances[payload.user_Id]
        
    return {"balance": current_balance}


@app.get("/user/{user_Id}")
async def get_user_balance(user_Id: str):
    # Блокуємо словник навіть для читання, щоб уникнути стану гонки
    async with balance_lock:
        current_balance = balances.get(user_Id, 0.0)
    
    # Повертаємо баланс рахунку [cite: 84]
    return {"balance": current_balance}


@app.get("/accounts")
async def get_all_accounts():
    async with balance_lock:
        # Робимо безпечну копію словника, щоб уникнути RuntimeError під час ітерації
        accounts_snapshot = balances.copy()
        
    # Повертаємо всі баланси [cite: 85]
    return accounts_snapshot