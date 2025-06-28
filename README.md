# Data Ingestion Pipeline on GCP (Batch & Streaming)

This project demonstrates a robust data ingestion and processing pipeline built on **Google Cloud Platform (GCP)** using **Apache Beam**, **Dataflow**, **Pub/Sub**, and **BigQuery**. The pipeline ingests and processes data from the **Yelp dataset (JSON format)** with support for both **batch loads** and **real-time streaming**.

---

# Use Case

## Batch Mode:
- Reads Yelp JSON data from **Google Cloud Storage (GCS)**.
- Validates and transforms the records using **Apache Beam**.
- Loads clean data into **BigQuery**.
- Malformed or invalid data is logged into an **error table**.

## Streaming Mode:
- JSON records are published to a **Pub/Sub** topic in real-time.
- A **streaming pipeline** reads data from Pub/Sub.
- Validates, transforms, and loads records into **BigQuery**.
- Malformed data is routed to a separate **error table**.

---

# Project Flow

### Initial Setup:
- Create GCP **Service Account** with required roles.
- Install and configure **Google Cloud SDK**.
- Set up Python environment with Beam and GCP libraries.

### Yelp Dataset Integration:
- Download and upload Yelp JSON files to **Cloud Storage**.
- Upload **BigQuery-compatible schema** files to GCS.

### Batch Ingestion Pipeline:
- Triggered via **Cloud Composer** (Airflow) or manually.
- Reads data from GCS, processes with Beam on Dataflow.
- Loads clean records to **BigQuery** and invalid to **BQ error table**.

### Streaming Ingestion Pipeline:
- Publishes data to **Pub/Sub** topic.
- Dataflow streaming job reads from topic and validates.
- Inserts clean records into **BigQuery**.
- Invalid messages go to an **error table**.

---

# Dataset Usage

## Batch Load:
Yelp JSON data stored in Cloud Storage is processed and loaded into BigQuery using Apache Beam running on Dataflow.

## Streaming Load:
Yelp JSON data is published to Pub/Sub and consumed by a Beam streaming pipeline running on Dataflow, then loaded into BigQuery.

---

# Orchestration and Scalability

This pipeline is fully orchestrated using **Google Cloud Composer**, which leverages **Apache Airflow** to schedule, trigger, and monitor both batch and streaming jobs. The modular DAGs enable easy parameterization and reusability of code for different datasets or workflows.

---
# Visualization and Reporting
The ingested data in BigQuery is visualized through an interactive and dynamic dashboard built using Looker Studio (formerly Data Studio).
The single-page dashboard includes:

Key Metrics: Total businesses, total reviews, average star ratings.

Geo Maps: Business distribution by city/state.

Time Series Charts: Review trends over time.

Bar & Pie Charts: Category-wise and rating-wise breakdowns.

Engagement Metrics: Average “useful”, “funny”, and “cool” scores.

Interactive Filters: Filter by city, state, rating, category, or date.

Business Details Table: Filterable table showing review and rating metrics per business.

This dashboard enables stakeholders to derive meaningful insights from both historical and real-time Yelp review data.

---

# Technologies Used

- **Apache Beam (Python SDK)**
- **Google Cloud Dataflow**
- **Google Cloud Pub/Sub**
- **Google BigQuery**
- **Google Cloud Storage**
- **Google Cloud SDK**
- **Python 3.x**
- **Cloud Composer (Apache Airflow)**
- **Vertex AI / Generative AI**




