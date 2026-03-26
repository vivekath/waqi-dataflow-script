FROM python:3.12.12-slim

WORKDIR /app

COPY main.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "main.py"]
