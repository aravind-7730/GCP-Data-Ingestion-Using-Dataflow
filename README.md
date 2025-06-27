# GCP-Data-Ingestion-Using-Dataflow

# Project Description
The agenda of the project involves Data ingestion and processing pipeline on Google cloud platform with real-time streaming and batch loads. Yelp dataset, which is used for academics and research purposes is used. We first create a service account on GCP followed by downloading Google Cloud SDK(Software developer kit). Then, Python software and all other dependencies are downloaded and connected to the GCP account for further processes. Then, the Yelp dataset is downloaded in JSON format, is connected to Cloud SDK following connections to Cloud storage which is then connected  and Yelp dataset JSON stream is published to PubSub topic. PubSub outputs are Apache Beam and connecting to Google Dataflow. Google BigQuery receives the structured data from workers. 

# Usage of Dataset:
Here we are going to use Yelp data in JSON format in the following ways:

Yelp dataset File: In Yelp dataset File, JSON file is connected to Cloud storage Fuse or Cloud SDK to the Google cloud storage which stores the incoming raw data followed by connections to  Apache Beam - Dataflow
Yelp dataset Stream: In Yelp dataset Stream, JSON Streams are published to Google PubSub topic for real-time data ingestion followed by connections to Apache beam for further processing.
