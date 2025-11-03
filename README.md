# Airflow Homework

## Overview
An Airflow DAG orchestrates an end-to-end pipeline with parallel ingest/transform, merges two related datasets, loads the result into PostgreSQL, performs analysis, and cleans intermediates.

## Datasets
- Population (US states, by year, ages): https://raw.githubusercontent.com/jakevdp/data-USstates/master/state-population.csv  
- Areas (US states, square miles): https://raw.githubusercontent.com/jakevdp/data-USstates/master/state-areas.csv

## Pipeline
- **ingest (parallel)**: download population, areas  
- **transform (parallel)**: filter 2012 total population; normalize areas; map state abbreviations → full names  
- **merge**: join by state, compute `density = population / area`  
- **load_to_postgres**: write `public.state_density`  
- **analyze**: plot Top-10 density to `include/outputs/top10_density.png`  
- **cleanup**: remove intermediates under `include/data/{raw,processed}`

Design notes: only file paths are passed via XCom; tasks parallelize within `TaskGroup`s.

## Run
```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose build
docker compose up airflow-init
docker compose up -d
# UI: http://localhost:8080  (user: airflow / pass: airflow)


