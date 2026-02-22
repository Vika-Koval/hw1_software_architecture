FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо файл і ОДНОЧАСНО перейменовуємо його на main.py всередині контейнера
COPY logging_service/logging.py main.py

# Запускаємо перейменований файл
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]