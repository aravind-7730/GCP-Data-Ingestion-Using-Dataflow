import json
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator

# Load batch and common configs from JSON
with open("/home/airflow/gcs/dags/config/dataflow_config.json") as f:
    config = json.load(f)
    batch = config["batch"]
    common = config["common"]

# Define DAG
with DAG(
    dag_id="dataflow_batch_job",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dataflow", "batch"],
) as dag:

    run_batch = DataflowStartPythonJobOperator(
        task_id="run_batch_dataflow",
        py_file=batch["py_file"],
        job_name=common["job_name_prefix"] + "-batch-{{ ds_nodash }}",
        options={
            "input_path": batch["input_path"],
            "schema_path": batch["schema_path"],
            "table": batch["table"],
            "error_table": batch["error_table"]
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
