# FMI Weather Data Pipeline

This repository demonstrates an ELT pipeline for ingesting Finnish Meteorological Institute (FMI) observations and preparing daily and long-term tables for downstream analytics and visualisation.

The pipeline uses a Kafka producer/consumer pair to capture raw observations, stores them in BigQuery, and relies on Airflow orchestration. A Streamlit app is included to showcase the latest and long-term views with bundled offline sample data.

Live FMI access uses [`fmi-weather-client`](https://pypi.org/project/fmi-weather-client/) so that observations come from `observation_by_station_id` and forecasts can be pulled with `forecast_by_place_name`, matching the official usage examples from the library documentation. Forecast retrieval can target any supplied list of places and is filtered to the last three years when using the helper in this repository.

## Contents
- `src/data_processing/`: Python modules for FMI access, Kafka streaming, and transformations.
- `dags/`: Airflow DAG that chains producer and consumer tasks.
- `visualization/`: Streamlit demo UI.
- `data/sample_observations.json`: Sample observations for offline testing.

## Data model and DAG plan
**Observation schema** (mirrors `transformations.BIGQUERY_SCHEMA`):
- `station_id` (STRING, required)
- `station_name` (STRING)
- `latitude` (FLOAT)
- `longitude` (FLOAT)
- `elevation` (FLOAT)
- `timestamp` (TIMESTAMP, required)
- `temperature` (FLOAT)
- `humidity` (FLOAT)
- `wind_speed` (FLOAT)

**Tables**
- **Daily table**: `fmi_weather.weather` (dataset/table configurable via env vars). Receives deduped, schema-aligned rows for all stations in the batch.
- **Long-term tables**: one table per whitelisted station (`station_<id>` prefix from `CONFIG.long_term_table_prefix`), populated after each daily load for the configured five default stations.

**DAG steps**
1. **Ingestion**: `ObservationProducer` (`src/data_processing/kafka_stream.py`) calls `FMIClient.fetch_latest()` to pull the most recent observations using `fmi-weather-client`’s `observation_by_station_id` for each whitelisted station (or sample fixtures when `USE_SAMPLE_DATA=true`). Results are published to Kafka.
2. **Landing in BigQuery**: `ObservationConsumer` reads Kafka messages in batches, converts them with `observations_as_dataframe`, and writes to `fmiweatherdatapipeline.fmi_weather.weather` (project/dataset/table names overrideable via environment variables).
3. **Daily processing**: `transformations.prepare_for_bigquery` applies schema coercion, drops rows missing required fields, deduplicates on `(station_id, timestamp)`, and keeps column ordering stable for BigQuery uploads.
4. **Long-term history**: After the daily load, `BigQuerySink` appends the same batch to per-station long-term tables for the configured whitelist to support time-series visualisations.
5. **Orchestration**: `dags/fmi_weather_dag.py` triggers producer then consumer every 15 minutes with a single retry, matching the Kafka-first ingestion model used in the code.
6. **Visualisation**: `visualization/app.py` renders latest tables and long-term series (temperature and humidity) via Streamlit. It can run entirely on the bundled sample data when Kafka/BigQuery are unavailable.

## ETL/ELT architecture at a glance

```mermaid
flowchart LR
    subgraph Orchestration
        Airflow["AIRFLOW DAG (dags/fmi_weather_dag.py)"]
    end

    subgraph Ingestion
        FMI["FMI API (live data)"]
        Sample["Sample JSON (data/sample_observations.json)"]
        Producer["Kafka Producer (src/data_processing/kafka_stream.py)"]
    end

    subgraph Queue
        Kafka[("Kafka Topic fmi_observations")]
    end

    subgraph Processing
        Consumer["Kafka Consumer (src/data_processing/kafka_stream.py)"]
        Transform["Transformations (src/data_processing/transformations.py)"]
    end

    subgraph Storage
        BQ["BigQuery Dataset (fmi_weather)"]
        DailyTable["Daily Table (weather)"]
        LongTerm["Station Tables (station_id)"]
    end

    subgraph Visualization
        Streamlit["Streamlit App (visualization/app.py)"]
    end

    subgraph Deployment
        Docker["Docker Compose (docker-compose.yml)"]
        Keys["Service Account Key (keys/bigquery/api_key.json)"]
    end

    Airflow -->|triggers| Producer
    Airflow -->|triggers| Consumer
    FMI --> Producer
    Sample --> Producer
    Producer --> Kafka
    Kafka --> Consumer
    Consumer --> Transform
    Transform --> DailyTable
    Transform --> LongTerm
    DailyTable --> BQ
    LongTerm --> BQ
    BQ --> Streamlit
    Docker -. orchestrates .-> Producer
    Docker -. orchestrates .-> Consumer
    Docker -. orchestrates .-> Streamlit
    Keys -. auth .-> BQ
```

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

Place your BigQuery API key or service account JSON file at `keys/bigquery/api_key.json`
or point `BIGQUERY_API_KEY_PATH` to its location so the pipeline can authenticate
to the `fmiweatherdatapipeline` project.

### Environment configuration
Key environment variables:
- `FMI_API_KEY`: FMI API key (optional for demo when `USE_SAMPLE_DATA=true`).
- `USE_SAMPLE_DATA`: Set to `true` to use bundled sample observations instead of live FMI API calls.
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default `localhost:9092`).
- `KAFKA_TOPIC`: Kafka topic for observations (default `fmi_observations`).
- `FORECAST_PLACES`: Comma-separated list of place names to pull forecasts for when using the forecast helper (defaults to none, meaning you can supply places directly when calling `fetch_forecasts_last_three_years`).
- `BIGQUERY_PROJECT`: BigQuery project ID (default `fmiweatherdatapipeline`).
- `BIGQUERY_DATASET`: Dataset where tables are stored (default `fmi_weather`).
- `BIGQUERY_DAILY_TABLE`: Table for daily loads (default `weather`).
- `BIGQUERY_API_KEY_PATH`: Path to the BigQuery API key or service account JSON file (default `keys/bigquery/api_key.json`).
- `STATION_WHITELIST`: Comma-separated list of station IDs that should receive long-term tables (default includes five Finnish stations).

### Connecting to the FMI API
Live FMI access is handled via [`fmi-weather-client`](https://pypi.org/project/fmi-weather-client/), which wraps FMI's HTTP endpoints and exposes the documented helpers:

- `weather_by_place_name(place_name)` / `weather_by_coordinates(latitude, longitude)`
- `observation_by_station_id(fmi_station_id)` / `observation_by_place(place_name)`
- `forecast_by_place_name(place_name, timestep_hours=24, forecast_points=4)` / `forecast_by_coordinates(latitude, longitude, ...)`

In this repository, `FMIClient` uses `observation_by_station_id` to hydrate the Kafka producer and `forecast_by_place_name` for optional forecast pulls. To enable live calls instead of fixtures, set `FMI_API_KEY` and ensure your network can reach FMI. When running via Docker Compose, the environment variable is passed through to both producer and consumer containers, and the services use `SIGINT` plus a 20s grace period on shutdown so HTTP sessions are closed cleanly when you run `docker compose down`.

### How observations are cleaned before BigQuery
Before loading a batch into BigQuery, the consumer applies `transformations.prepare_for_bigquery` to the Pandas dataframe built from Kafka messages. The cleaning logic is deterministic so data is easy to reason about:

1. **Schema coercion** (`apply_bigquery_schema`)
   - Convert timestamps to UTC `datetime` and cast IDs to strings.
   - Ensure numeric columns (`temperature`, `humidity`, `wind_speed`) are floats, coercing invalid values to `NaN` so they can be filtered or inspected.
   - Reorder columns to match `BIGQUERY_SCHEMA`, filling any missing optional columns with `pd.NA` so uploads stay stable if upstream payloads change.
2. **Required-field filtering**
   - Drop any rows missing `station_id` or `timestamp` so BigQuery constraints are satisfied and join keys remain usable.
3. **Deduplication**
   - Drop duplicate rows sharing the same `(station_id, timestamp)` key to avoid inflating daily tables when Kafka retries or multiple producers overlap.
4. **Quality monitoring (optional helpers)**
   - `detect_missing_values` summarizes column-level null counts, and `detect_outliers` flags extreme numeric values via configurable z-scores. These are available for Airflow/Mage quality checks but do not block loads by default.

The resulting, ordered dataframe is what `BigQuerySink` writes to the daily table and per-station long-term tables.

### Producing and consuming observations
Run the producer once to send observations into Kafka:
```bash
USE_SAMPLE_DATA=true python -m src.data_processing.kafka_stream produce
```

Consume a batch and upload to BigQuery:
```bash
USE_SAMPLE_DATA=true python -m src.data_processing.kafka_stream consume --max-messages 200
```

### Airflow
Copy `dags/fmi_weather_dag.py` into your Airflow `dags/` folder. Set environment variables in Airflow for Kafka and BigQuery connectivity. The DAG schedules producer then consumer every 15 minutes.

### Streamlit demo
The demo uses sample data by default.
```bash
USE_SAMPLE_DATA=true streamlit run visualization/app.py
```

### Running with Docker

The repository includes a Docker Compose setup that starts Kafka, creates the
`fmi_observations` topic, and launches producer, consumer, and Streamlit
containers. Each service now builds against the requirements file that matches
its role (worker vs. Streamlit) to avoid Airflow dependencies in the UI image.

1. Build images and start the stack:
   ```bash
   docker compose up --build
   ```
2. Access the Streamlit demo at http://localhost:8501.

Environment notes:
- The compose file sets `USE_SAMPLE_DATA=true` for the producer so it can run
  without FMI API access.
- For BigQuery loads, mount your service account key at
 `./keys/bigquery/api_key.json` (the container expects it at
  `/app/keys/bigquery/api_key.json`).

### Stopping the services
- Stop locally started Python commands (producer/consumer/Streamlit) with `Ctrl+C` in the terminal running them.
- Stop the Docker Compose stack with:
  ```bash
  docker compose down
  ```
  This cleanly halts Kafka, producer, consumer, and Streamlit containers.

## Dev Runbook (Docker Compose)

This runbook lists the exact commands to run in common situations (normal run, after code changes, and troubleshooting).

### 1) Normal usage

#### Start the full stack (build + run)
```bash
docker compose up -d --build
```

#### Check status

```bash
docker compose ps
```

#### Follow all logs

```bash
docker compose logs -f
```

---

### 2) After code changes

#### A) Changed Streamlit UI (`visualization/…`)

Rebuild and recreate only Streamlit:

```bash
docker compose up -d --build --force-recreate streamlit
```

If it still looks like old code (cache):

```bash
docker compose build --no-cache streamlit
docker compose up -d --force-recreate streamlit
```

Logs:

```bash
docker compose logs -f streamlit
```

#### B) Changed worker code (`src/…`) affecting producer/consumer

```bash
docker compose up -d --build --force-recreate producer consumer
```

Logs:

```bash
docker compose logs -f producer
docker compose logs -f consumer
```

Only consumer changed:

```bash
docker compose up -d --build --force-recreate consumer
```

#### C) Changed only `docker-compose.yml`

Usually enough:

```bash
docker compose up -d --force-recreate
```

If build args / requirements / Dockerfile also changed:

```bash
docker compose up -d --build --force-recreate
```

---

### 3) Recommended dev workflow: infra as services, jobs as one-shots

Producer/consumer are often easiest to run as one-shot jobs during development.

#### Start only infrastructure

```bash
docker compose up -d broker init-topics
```

#### Run producer once

```bash
docker compose run --rm producer
```

#### Run consumer once (example: 5 messages)

```bash
docker compose run --rm consumer python -m src.data_processing.kafka_stream consume --max-messages 5
```

---

### 4) BigQuery checks

#### Confirm the key file exists inside the container

```bash
docker compose exec -T consumer ls -l /app/keys/bigquery/api_key.json
```

#### Count rows in the daily table

```bash
docker compose exec -T consumer python - <<'PY'
import pandas_gbq
q = "SELECT COUNT(*) AS n FROM `fmiweatherdatapipeline.fmi_weather.weather`"
print(pandas_gbq.read_gbq(q, project_id="fmiweatherdatapipeline"))
PY
```

#### List tables in the dataset

```bash
docker compose exec -T consumer python - <<'PY'
import pandas_gbq
q = """
SELECT table_name
FROM `fmiweatherdatapipeline.fmi_weather.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name
"""
print(pandas_gbq.read_gbq(q, project_id="fmiweatherdatapipeline"))
PY
```

---

### 5) Kafka checks

#### Read 5 messages from the topic (proof that data exists)

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:29092 \
  --topic fmi_observations \
  --from-beginning \
  --max-messages 5
```

#### Check consumer group offsets / lag

```bash
docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker:29092 \
  --describe \
  --group fmi-ingestion
```

#### If consumer prints "No messages read…", reset offsets to earliest

1. Stop consumer:

```bash
docker compose stop consumer
```

2. Reset offsets:

```bash
docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker:29092 \
  --group fmi-ingestion \
  --topic fmi_observations \
  --reset-offsets --to-earliest --execute
```

3. Run consumer again (one-shot):

```bash
docker compose run --rm consumer python -m src.data_processing.kafka_stream consume --max-messages 5
```

---

### 6) Troubleshooting quick fixes

#### A) "Container name already in use"

```bash
docker rm -f broker init-topics producer consumer streamlit 2>/dev/null || true
docker compose up -d
```

#### B) Port 9092 already allocated

Check what is using 9092:

```bash
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}"
```

Stop/remove the container that binds `0.0.0.0:9092->…`.

#### C) "the input device is not a TTY" (heredoc / piping)

Use `-T`:

```bash
docker compose exec -T consumer python - <<'PY'
print("ok")
PY
```

#### D) BuildKit snapshot/layer errors (e.g., "parent snapshot does not exist")

```bash
docker builder prune -af
docker compose build --no-cache
docker compose up -d --force-recreate
```

#### E) Producer shows "Restarting (0)"

Exit code 0 means it is a one-shot job and restart policy is re-running it. Prefer running it via:

```bash
docker compose run --rm producer
```

---

### 7) Clean start (project scope)

#### Remove containers, networks, volumes and rebuild from scratch

```bash
docker compose down --remove-orphans --volumes
docker compose build --no-cache
docker compose up -d --force-recreate
```

#### Full Docker cleanup (removes unused containers/images/volumes)

```bash
docker system prune -af --volumes
```

## Testing transformations with sample data
A pytest suite exercises the transformation utilities. Run:
```bash
python -m pip install pytest
USE_SAMPLE_DATA=true pytest
```

## Notes
- Live data uses `fmi-weather-client` helpers such as `observation_by_station_id` and `forecast_by_place_name`; the bundled sample observations remain available when `USE_SAMPLE_DATA=true`.
- Forecast retrieval is centralized in `FMIClient.fetch_forecasts_last_three_years`, which accepts any list of place names and filters returned timestamps to the trailing three years.
- BigQuery interactions depend on `pandas.DataFrame.to_gbq`; ensure the `pandas-gbq` extras are installed in your environment.
