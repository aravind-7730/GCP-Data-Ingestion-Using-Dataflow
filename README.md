# Data Ingestion Pipeline on GCP (Batch & Streaming)
This project demonstrates a robust data ingestion and processing pipeline built on Google Cloud Platform (GCP) using Apache Beam, Dataflow, Pub/Sub, and BigQuery. The pipeline ingests and processes data from the Yelp dataset (JSON format) with support for both batch loads and real-time streaming.

# Use Case 
# Batch Mode:
Reads Yelp JSON data from Cloud Storage and loads it directly into BigQuery after validating and transforming the records.

# Streaming Mode:
Data is first published to a Pub/Sub topic in real-time. A streaming pipeline consumes from Pub/Sub, validates the messages, and writes the processed data to BigQuery.

# Project Flow
Initial Setup:

GCP Service Account creation

Google Cloud SDK setup

Python environment with required Beam and GCP libraries

Yelp Dataset Integration:

JSON data is downloaded and uploaded to Cloud Storage

A BigQuery-compatible schema file is also stored in GCS

Batch Ingestion Pipeline:

Triggered manually or on schedule

Reads JSON records from GCS

Validates and parses the data using Apache Beam

Loads clean records directly into BigQuery

Invalid records are logged to an error table

Streaming Ingestion Pipeline:

JSON events are published to Pub/Sub (manually or from external producers)

The Beam streaming pipeline reads from the Pub/Sub topic

Validates, parses, and writes records to BigQuery

Malformed data is sent to a separate error table

# Dataset Usage
Batch Load:
Yelp JSON data stored in Cloud Storage is processed and loaded directly into BigQuery using Apache Beam via Dataflow (no Pub/Sub in the path).

Streaming Load:
Yelp JSON data is published to Pub/Sub, from where it's consumed by a streaming Apache Beam pipeline and then loaded into BigQuery.



# Technologies Used
Apache Beam (Python SDK)

Google Cloud Dataflow

Google Cloud Pub/Sub

Google BigQuery

Google Cloud Storage

Google Cloud SDK

Python 3.x

Vertex AI / Generative AI models
