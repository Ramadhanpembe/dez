# Data Engineering Zoomcamp: Hands-on Implementations

My working implementations of the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp): a batch and streaming data platform built step by step, from containerised ingestion and infrastructure-as-code to warehousing, transformation, orchestration, and stream processing.

Where the course covers one orchestrator (Kestra), I also implemented the same orchestration layer in **Apache Airflow**, in two setups, to compare the tools directly.

---

## Contents

- [What's covered](#whats-covered)
- [Repository structure](#repository-structure)
- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Acknowledgements](#acknowledgements)

---

## What's covered

| # | Module | Directory | Tools |
|---|--------|-----------|-------|
| 1 | Containerized ingestion pipeline | [`pipeline/`](./pipeline) | Docker, Python, PostgreSQL, NYC Taxi Dataset |
| 2 | Infrastructure as code | [`terraform/`](./terraform) | Terraform, GCP |
| 3 | Orchestration (course tool) | [`kestra/`](./kestra) | Kestra |
| 4 | Orchestration (extension, Docker) | [`airflow-docker/`](./airflow-docker) | Apache Airflow, Docker Compose |
| 5 | Orchestration (extension, Astro) | [`airflow-astro/`](./airflow-astro) | Apache Airflow, Astro CLI |
| 6 | Data warehouse | [`warehouse/`](./warehouse) | BigQuery (GCP) |
| 7 | Analytics engineering | [`dbt-local/`](./dbt-local) | dbt |
| 8 | Batch processing | [`batch-processing/`](./batch-processing) | Apache Spark |
| 9 | Stream processing | [`streaming/`](./streaming) | Kafka, Redpanda, Flink |

---

## Repository structure

```text
dez/
├── pipeline/           # Dockerised ingestion pipeline
├── terraform/          # GCP infrastructure (buckets, datasets)
├── kestra/             # Kestra flows
├── airflow-docker/     # Airflow on Docker Compose
├── airflow-astro/      # Airflow via the Astro CLI
├── warehouse/          # BigQuery warehouse work
├── dbt-local/          # dbt project, run locally
├── batch-processing/   # Spark jobs
├── streaming/          # Kafka / Redpanda / Flink
└── README.md
```

Each module is self-contained, with its own dependencies and run instructions. You can start with any of them.

---

## Architecture

The modules follow the stages of a typical data platform:

```text
 Source data ──▶ Ingestion ──▶ Data lake / Warehouse ──▶ Transformation ──▶ Analytics
                 (pipeline)     (terraform, warehouse)       (dbt)

          Orchestration: Kestra · Airflow (Docker) · Airflow (Astro)

 Batch:     Spark                        (batch-processing)
 Streaming: Kafka / Redpanda → Flink     (streaming)
```

<!-- TODO: replace with a real diagram (Mermaid or an image) showing how YOUR modules connect. A true diagram beats a generic one. -->

---

## Tech stack

- **Languages:** Python, SQL
- **Containers:** Docker, Docker Compose
- **Infrastructure:** Terraform, Google Cloud Platform
- **Orchestration:** Kestra, Apache Airflow
- **Warehousing and transformation:** BigQuery, dbt
- **Batch:** Apache Spark
- **Streaming:** Apache Kafka, Redpanda, Apache Flink

---

## Acknowledgements

Built while following the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by [DataTalks.Club](https://datatalks.club/). Course material and datasets belong to their respective authors.

---

## Author

**Ramadhan Pembe**
