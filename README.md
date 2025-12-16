# FMI Weather Data Pipeline

This repository demonstrates an ELT pipeline for ingesting Finnish Meteorological Institute (FMI) observations and writing **hourly samples straight into BigQuery**. Historical backfills and live hourly ingestion append to the same table so the dataset stays continuous. The pipeline ships with Kafka-based buffering, Airflow orchestration, and a Streamlit dashboard that can operate entirely on bundled fixtures when `USE_SAMPLE_DATA=true`.

## Contents
- `src/data_processing/`: Python modules for FMI access, Kafka streaming, and transformations.
- `dags/`: Airflow DAGs for hourly ingestion and manual backfill.
- `visualization/`: Streamlit demo UI.
- `data/sample_observations.json`: Sample observations for offline testing.

## Data model and flow

### Hourly fact table
Rows land directly in a single BigQuery table (default `weather_hourly_samples`; configurable via `BIGQUERY_HOURLY_TABLE`). One row per station per hour with the following columns (mirrors `transformations.BIGQUERY_HOURLY_SCHEMA`):
- `station_id` (STRING, required)
- `timestamp` (TIMESTAMP, required) — floored to the start of the hour (UTC)
- `temperature` (FLOAT)
- `humidity` (FLOAT)
- `station_name` (STRING)
- `latitude` (FLOAT)
- `longitude` (FLOAT)
- `elevation` (FLOAT)
- `wind_speed` (FLOAT)

Uniqueness conceptually follows (`station_id`, `timestamp`). Deduplication is applied inside each batch; BigQuery receives append-only loads.

### Pipeline steps
1. **Sampling**: `FMIClient` down-samples to hourly resolution by flooring timestamps and keeping the latest observation within each hour. Sample fixtures follow the same rule for parity with live data.
2. **Kafka**: `ObservationProducer` publishes the latest hourly readings or a backfill covering the last 365 days. Messages are JSON-encoded.
3. **BigQuery load**: `ObservationConsumer` batches Kafka messages, converts them with `transformations.prepare_hourly_for_bigquery`, deduplicates on `(station_id, timestamp)`, and appends directly to the hourly table (`CONFIG.hourly_table`).
4. **Orchestration**: Airflow triggers the producer and consumer hourly. A separate manual backfill DAG can replay the last year.
5. **Visualisation**: `visualization/app.py` can read BigQuery or the bundled sample data.

## Environment configuration
Key environment variables:
- `FMI_API_KEY`: FMI API key (optional for demo when `USE_SAMPLE_DATA=true`).
- `USE_SAMPLE_DATA`: Set to `true` to use bundled sample observations instead of live FMI API calls.
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default `localhost:9092`).
- `KAFKA_TOPIC`: Kafka topic for observations (default `fmi_observations`).
- `FORECAST_PLACES`: Comma-separated list of place names to pull forecasts for (optional).
- `BIGQUERY_PROJECT`: BigQuery project ID (default `fmiweatherdatapipeline`).
- `BIGQUERY_DATASET`: Dataset where tables are stored (default `fmi_weather`).
- `BIGQUERY_HOURLY_TABLE`: Hourly table name (default `weather_hourly_samples`).
- `BIGQUERY_DAILY_TABLE`: Legacy daily table name (kept for compatibility but unused by the hourly sink).
- `BIGQUERY_API_KEY_PATH`: Path to the BigQuery API key or service account JSON file (default `keys/bigquery/api_key.json`).
- `STATION_WHITELIST`: Comma-separated list of station IDs for historical filtering (defaults to five Finnish stations).

Place your BigQuery API key or service account JSON file at `keys/bigquery/api_key.json` or point `BIGQUERY_API_KEY_PATH` to its location.

## Hourly cleaning rules
Before upload, `transformations.prepare_hourly_for_bigquery`:
1. Coerces schema to `BIGQUERY_HOURLY_SCHEMA` and forces timestamps to UTC.
2. Floors `timestamp` to the start of the hour.
3. Drops rows missing `station_id` or `timestamp`.
4. Deduplicates within the batch on `(station_id, timestamp)`, keeping the latest record per hour.

## Running locally

### Prerequisites
- Python 3.10+
- Kafka broker available at `localhost:9092` (configurable via env var `KAFKA_BOOTSTRAP_SERVERS`)
- Google Cloud credentials for BigQuery (if testing actual loads)

Install dependencies (UI, producer, and consumer services):
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-streamlit.txt
```

If you want to develop Airflow DAGs locally, install its dependencies separately to keep the heavier stack isolated from the Streamlit image:
```bash
pip install -r requirements-airflow.txt
```

### Producing and consuming observations
Run the producer once to send **hourly** observations into Kafka:
```bash
USE_SAMPLE_DATA=true python -m src.data_processing.kafka_stream produce --mode latest-hourly
```

Consume a batch and upload to BigQuery:
```bash
USE_SAMPLE_DATA=true python -m src.data_processing.kafka_stream consume --max-messages 200
```

Backfill the last 365 days of hourly data:
```bash
USE_SAMPLE_DATA=true python -m src.data_processing.kafka_stream produce --mode backfill-last-year-hourly
```
Run the consumer repeatedly (or with a high `--max-messages`) to drain the topic after a backfill.

### Airflow
Copy `dags/fmi_weather_dag.py` into your Airflow `dags/` folder. The primary DAG `fmi_weather_pipeline` schedules producer then consumer **hourly** (`@hourly`). A separate manual DAG `fmi_weather_backfill_hourly` runs the backfill producer followed by a high-volume consumer.

### Running with Docker

Start the stack and view logs:
```bash
docker compose up -d --build
docker compose ps
docker compose logs -f producer
docker compose logs -f consumer
```
Visit the UI at [http://localhost:8501](http://localhost:8501) after the command starts.
Run hourly modes manually through Docker:
- Latest hourly batch:
  ```bash
  docker compose run --rm producer python -m src.data_processing.kafka_stream produce --mode latest-hourly
  docker compose run --rm consumer python -m src.data_processing.kafka_stream consume --max-messages 500
  ```
- Backfill for the last 365 days:
  ```bash
  docker compose run --rm producer python -m src.data_processing.kafka_stream produce --mode backfill-last-year-hourly
  docker compose run --rm consumer python -m src.data_processing.kafka_stream consume --max-messages 2000
  ```

Hourly ingestion service (BigQuery only):
- The `hourly-ingestor` service will automatically backfill the last year to BigQuery if the hourly table is empty or missing recent data, then continue uploading fresh hourly data.
- Start it with Docker:
  ```bash
  docker compose up -d hourly-ingestor
  docker compose logs -f hourly-ingestor
  ```

Environment notes:
- The compose file sets `USE_SAMPLE_DATA=true` for the producer so it can run without FMI API access.
- For BigQuery loads, mount your service account key at `./keys/bigquery/api_key.json` (the container expects it at `/app/keys/bigquery/api_key.json`).

macOS tips:
- Docker Desktop for Mac supports the modern `docker compose` (with a space). If your setup only has the legacy plugin, use `docker-compose` instead.
- Run shell commands separately (e.g., first `git status`, then `docker compose ...`). Combining them on the same line (`git status docker compose ...`) will cause git to treat `docker` as an argument, leading to errors like `unknown option 'rm'`.

### Stopping the services
- Stop locally started Python commands (producer/consumer/Streamlit) with `Ctrl+C` in the terminal running them.
- Stop the Docker Compose stack with:
  ```bash
  docker compose down
  ```

## BigQuery validation queries

**Check for duplicates**
```sql
SELECT station_id, timestamp, COUNT(*) AS n
FROM `<project>.<dataset>.<hourly_table>`
GROUP BY station_id, timestamp
HAVING n > 1
ORDER BY n DESC;
```

**Row count from the last 365 days**
```sql
SELECT COUNT(*) AS rows
FROM `<project>.<dataset>.<hourly_table>`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY);
```

## Streamlit demo
The demo uses sample data by default.
```bash
USE_SAMPLE_DATA=true streamlit run visualization/app.py
```
