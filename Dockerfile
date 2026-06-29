FROM python:3.12-slim

# non-root user
RUN useradd -m -u 10001 appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

USER appuser

EXPOSE 8080

# Single worker is fine: connections are per-request and short-lived.
# Bump --workers only if you parallelize ingestion across mailboxes.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
