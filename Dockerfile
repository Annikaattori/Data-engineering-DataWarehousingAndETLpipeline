FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

ARG REQUIREMENTS_FILE=requirements-worker.txt
COPY requirements-base.txt requirements-streamlit.txt requirements-worker.txt requirements-airflow.txt ./
RUN pip install --no-cache-dir -r ${REQUIREMENTS_FILE}

COPY src ./src
COPY visualization ./visualization
COPY data ./data
COPY dags ./dags
COPY keys ./keys
COPY README.md ./

CMD ["python", "-m", "src.data_processing.kafka_stream", "produce"]
