FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV GCS_BUCKET=""
ENV INPUT_CSV=""
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/gcs-sa-key.json"

CMD ["python", "main.py"]
