import asyncio
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# Локальна хеш-таблиця для збереження логів (transaction_Id -> message)
logs_storage = {}
# Блокування для безпечного запису та читання під навантаженням
logs_lock = asyncio.Lock()

# Модель, яка приймає дані від facade-service
class LogMessage(BaseModel):
    transaction_Id: str
    user_Id: str
    amount: float
    timestamp: str

# --- Ендпоінти ---

@app.post("/log")
async def save_log(msg: LogMessage):
    # Зберігаємо транзакцію, використовуючи transaction_ID у якості ключа
    async with logs_lock:
        # Перетворюємо об'єкт Pydantic у звичайний словник для збереження
        msg_dict = msg.model_dump() # у старіших версіях Pydantic це msg.dict()
        logs_storage[msg.transaction_Id] = msg_dict
        
        # Виводимо у консоль для відлагодження, як вимагається в завданні
        print(f"DEBUG [Logging Service]: Saved transaction -> {msg_dict}")
        
    return {"status": "success", "transaction_Id": msg.transaction_Id}


@app.get("/logs/{user_Id}")
async def fetch_user_logs(user_Id: str):
    # Повертаємо транзакції ТІЛЬКИ для конкретного клієнта
    async with logs_lock:
        # Проходимося по всіх збережених логах і відбираємо потрібні
        user_transactions = [
            log_data for log_data in logs_storage.values() 
            if log_data["user_Id"] == user_Id
        ]
        
    return user_transactions

@app.get("/logs")
async def fetch_all_logs():
    # Цей ендпоінт може знадобитися для загального дебагу системи
    async with logs_lock:
        # Безпечно копіюємо словник, щоб не зловити RuntimeError
        all_logs_snapshot = logs_storage.copy()
        
    return all_logs_snapshot