import json
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator

with open("/home/airflow/gcs/dags/config/dataflow_config.json") as f:
    config = json.load(f)
    streaming = config["streaming"]
    common = config["common"]

with DAG(
    dag_id="dataflow_streaming_job",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dataflow", "streaming"],
) as dag:

    run_streaming = DataflowStartPythonJobOperator(
        task_id="run_streaming_dataflow",
        py_file=streaming["py_file"],
        job_name=common["job_name_prefix"] + "-streaming-{{ ds_nodash }}",
        options={
            "input_path": streaming["input_path"],
            "schema_path": streaming["schema_path"],
            "table": streaming["table"],
            "error_table": streaming["error_table"],
            "pubsub_topic": streaming["pubsub_topic"],
            "pubsub_input": streaming["pubsub_input"]
        },
        dataflow_default_options={
            "project": common["project_id"],
            "runner": "DataflowRunner",
            "temp_location": common["temp_location"],
            "staging_location": common["staging_location"],
            "region": common["region"]
        },
        project_id=common["project_id"],
        gcp_conn_id="google_cloud_default"
    )
