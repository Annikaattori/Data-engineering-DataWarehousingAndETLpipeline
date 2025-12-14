# FMI Weather Data Pipeline

This repository demonstrates an ELT pipeline for ingesting Finnish Meteorological Institute (FMI) observations and preparing daily and long-term tables for downstream analytics and visualisation.

The pipeline uses a Kafka producer/consumer pair to capture raw observations, stores them in BigQuery, and relies on Airflow orchestration. A Streamlit app is included to showcase the latest and long-term views with bundled offline sample data.

## Contents
- `src/data_processing/`: Python modules for FMI access, Kafka streaming, and transformations.
- `dags/`: Airflow DAG that chains producer and consumer tasks.
- `visualization/`: Streamlit demo UI.
- `data/sample_observations.json`: Sample observations for offline testing.

## Data model and DAG plan
1. **Ingestion**: `ObservationProducer` (`src/data_processing/kafka_stream.py`) calls `FMIClient.fetch_latest()` to pull the most recent observations and publish them to Kafka.
2. **Landing in BigQuery**: `ObservationConsumer` reads Kafka messages in batches and appends them to `fmiweatherdatapipeline.fmi_weather.weather` (project, dataset, and table configurable via environment variables).
3. **Daily processing**: 
   - Dedupe incoming rows on `(station_id, timestamp)`.
   - Run lightweight quality checks for missing values and outliers (functions in `transformations.py`).
   - Persist deduped daily data.
4. **Long-term history**: After each batch load, append records for selected stations into individual `station_<id>` tables for easier time-series access.
5. **Orchestration**: `dags/fmi_weather_dag.py` schedules ingestion every 15 minutes with basic retry settings.
6. **Visualisation**: `visualization/app.py` renders latest tables and long-term series (temperature and humidity) via Streamlit.

## Running locally

### Prerequisites
- Python 3.10+
- Kafka broker available at `localhost:9092` (configurable via env var `KAFKA_BOOTSTRAP_SERVERS`)
- Google Cloud credentials for BigQuery (if testing actual loads)

Install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
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
- `BIGQUERY_PROJECT`: BigQuery project ID (default `fmiweatherdatapipeline`).
- `BIGQUERY_DATASET`: Dataset where tables are stored (default `fmi_weather`).
- `BIGQUERY_DAILY_TABLE`: Table for daily loads (default `weather`).
- `BIGQUERY_API_KEY_PATH`: Path to the BigQuery API key or service account JSON file (default `keys/bigquery/api_key.json`).
- `STATION_WHITELIST`: Comma-separated list of station IDs that should receive long-term tables (default includes five Finnish stations).

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
containers.

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

## Testing transformations with sample data
A pytest suite exercises the transformation utilities. Run:
```bash
python -m pip install pytest
USE_SAMPLE_DATA=true pytest
```

## Notes
- Parsing of FMI XML responses is simplified by expecting GeoJSON-like input. For production use, replace `_parse_response` in `fmi_client.py` with a full WFS parser or use the official FMI Python client to generate JSON payloads.
- BigQuery interactions depend on `pandas.DataFrame.to_gbq`; ensure the `pandas-gbq` extras are installed in your environment.
