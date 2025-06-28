# Yelp Data Ingestion Pipeline on Google Cloud Platform (GCP)

This project demonstrates an end-to-end batch and streaming data ingestion pipeline built on **Google Cloud Platform (GCP)** using **Apache Beam**, **Cloud Dataflow**, **Cloud Pub/Sub**, and **BigQuery**. The pipeline processes Yelp dataset JSON files and supports both scheduled batch ingestion and real-time streaming ingestion.

---

## 🚀 Use Case

- **Batch Ingestion**: Load structured JSON data from **Google Cloud Storage (GCS)** into **BigQuery** after validation and transformation.
- **Streaming Ingestion**: Real-time ingestion of Yelp data from **Pub/Sub**, transform and load it into **BigQuery**.

---

## 🔧 Architecture Overview

```mermaid
graph TD
    A[GCS (Yelp JSON Files)] --> B[Dataflow (Apache Beam - Batch)]
    B --> C[BigQuery (Valid Data)]
    B --> D[BigQuery (Error Table)]

    E[Pub/Sub Topic] --> F[Dataflow (Apache Beam - Streaming)]
    F --> C
    F --> D
