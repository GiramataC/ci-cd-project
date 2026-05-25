# Mini Data Platform

End-to-end data platform using Docker Compose: MinIO → Airflow → PostgreSQL → Metabase.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- Python 3.11+ (for running the data generator locally)

## Quick start

```bash
# 1. Start the full stack (first run takes ~3–5 min to pull images)
docker compose up -d

# 2. Watch Airflow initialise
docker logs airflow_init -f

# 3. Generate sample data and upload it to MinIO
pip install -r data_generator/requirements.txt
python data_generator/generate.py

# 4. Trigger the pipeline manually (or let the daily schedule fire it)
docker exec airflow_webserver airflow dags trigger mini_data_pipeline

# 5. Watch the run
docker exec airflow_webserver airflow dags state mini_data_pipeline $(date +%Y-%m-%dT%H:%M:%S)
```

## Service URLs

| Service          | URL                        | Credentials          |
|------------------|----------------------------|----------------------|
| Airflow          | http://localhost:8080       | admin / admin        |
| MinIO console    | http://localhost:9001       | minio / minio123     |
| Metabase         | http://localhost:3000       | set on first login   |
| PostgreSQL       | localhost:5432              | admin / admin        |

## Connecting Metabase to PostgreSQL

1. Open http://localhost:3000 and complete the setup wizard.
2. When asked to add a database, choose **PostgreSQL** with:
   - Host: `postgres`
   - Port: `5432`
   - Database: `analytics`
   - Username: `admin` / Password: `admin`
3. The `sales` table will appear automatically after the pipeline runs.

## Data flow

```
generate.py  →  MinIO (sales-data bucket)
                    ↓  Airflow DAG (mini_data_pipeline)
                    ↓    extract  → download latest CSV from MinIO
                    ↓    transform → drop nulls, add profit column
                    ↓    load     → insert into postgres.analytics.sales
                                          ↓
                                     Metabase dashboard
```

## Project structure

```
├── dags/                   # Airflow DAG definitions
│   └── pipeline.py
├── data_generator/         # Synthetic data producer
│   ├── generate.py         # Uploads CSV to MinIO
│   └── requirements.txt
├── config/
│   └── init.sql            # Postgres table + index creation
├── docker-compose.yml
├── .github/workflows/
│   └── main.yml            # CI lint → build → integration test
└── README.md
```

## Stopping the stack

```bash
docker compose down          # keep volumes
docker compose down -v       # destroy all data
```