# NYC 311 Service-Request Pipeline

Work in progress. Goal: an orchestrated ELT pipeline (API -> raw Parquet -> DuckDB/warehouse -> dbt -> Airflow) that answers: which complaint types and boroughs miss their resolution targets, and can we trust the data?

## Day 1
python ingestion/ingest_311.py --date 2026-11-10
