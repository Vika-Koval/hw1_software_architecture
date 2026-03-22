FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY logging_service/logging.py main.py

CMD exec uvicorn main:app --host 0.0.0.0 --port ${HTTP_PORT:-8000}