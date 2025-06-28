import argparse
import json
import logging
import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp import bigquery_tools
from google.cloud import storage
from apache_beam import DoFn, pvalue

# Schema used for logging parsing errors into BigQuery
ERROR_SCHEMA_DICT = {
    'fields': [
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'error_message', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ]
}

# Reads and returns BigQuery schema from a GCS JSON file
def read_schema_from_gcs(file_path: str) -> dict:
    bucket_name = file_path.split('/')[2]
    blob_name = '/'.join(file_path.split('/')[3:])
    storage_client = storage.Client()
    blob = storage_client.bucket(bucket_name).blob(blob_name)
    schema_string = blob.download_as_string().decode('utf-8')
    schema_data = json.loads(schema_string)

    if isinstance(schema_data, list):
        return {"fields": schema_data}
    elif isinstance(schema_data, dict) and "fields" in schema_data:
        return schema_data
    else:
        raise ValueError("Invalid schema format")

# Converts raw values into proper BigQuery-compatible formats
def parse_value(value, field_type, field_name="<unknown>"):
    try:
        if field_type == 'INTEGER':
            return int(value)
        elif field_type in ['FLOAT', 'NUMERIC', 'BIGNUMERIC']:
            return float(value)
        elif field_type == 'BOOLEAN':
            if isinstance(value, bool):
                return value
            return str(value).lower() == 'true'
        elif field_type == 'TIMESTAMP':
            if isinstance(value, (int, float)):
                dt = datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
            elif isinstance(value, str):
                dt = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
            else:
                raise ValueError(f"Unsupported timestamp value: {value}")
            return dt.isoformat().replace('+00:00', 'Z')  # Convert to RFC3339
        elif field_type == 'DATETIME':
            if isinstance(value, str):
                dt = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            else:
                raise ValueError(f"Unsupported DATETIME value: {value}")
        else:
            return str(value)
    except Exception as e:
        raise ValueError(f"Field '{field_name}' error: {e}")

# Parses JSON strings and validates data types against BigQuery schema
class ParseMessage(DoFn):
    OUTPUT_ERROR_TAG = 'error'

    def __init__(self, schema_dict):
        self.schema_dict = schema_dict
        self.table_schema = None

    def setup(self):
        self.table_schema = bigquery_tools.parse_table_schema_from_json(json.dumps(self.schema_dict))
        self.field_map = {f.name: f for f in self.table_schema.fields}

    def process(self, element):
        error_id = abs(hash(element))
        try:
            data = json.loads(element)

            # Flatten nested "body" object if present
            if "body" in data and isinstance(data["body"], dict):
                data = data["body"]

            output = {}
            for field in self.table_schema.fields:
                val = data.get(field.name)
                if val is None and field.mode == 'REQUIRED':
                    raise ValueError(f"Missing required field: {field.name}")
                if field.mode == 'REPEATED':
                    output[field.name] = [parse_value(v, field.type, field.name) for v in val]
                else:
                    output[field.name] = parse_value(val, field.type, field.name)
            yield output
        except Exception as e:
            yield pvalue.TaggedOutput(self.OUTPUT_ERROR_TAG, {
                'id': error_id,
                'error_message': str(e),
                'timestamp': datetime.datetime.utcnow().isoformat() + "Z"
            })

# Pipeline argument definitions
class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_path', type=str, help="GCS path for input JSON file")
        parser.add_argument('--schema_path', type=str, required=True)
        parser.add_argument('--table', type=str, required=True)
        parser.add_argument('--error_table', type=str, required=True)
        parser.add_argument('--pubsub_topic', type=str, help="Pub/Sub topic to write to")
        parser.add_argument('--pubsub_input', type=str, help="Pub/Sub topic to read from")

# Main pipeline execution
def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args_list = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args_list)
    dataflow_opts = pipeline_options.view_as(DataflowOptions)

    # Enable streaming mode for pipeline
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Load and parse schemas for main and error tables
    schema_dict = read_schema_from_gcs(dataflow_opts.schema_path)
    main_schema_bq = bigquery_tools.parse_table_schema_from_json(json.dumps(schema_dict))
    error_schema_bq = bigquery_tools.parse_table_schema_from_json(json.dumps(ERROR_SCHEMA_DICT))

    parse_fn = ParseMessage(schema_dict)

    with beam.Pipeline(options=pipeline_options) as p:

        # Batch load: GCS to Pub/Sub
        if dataflow_opts.input_path and dataflow_opts.pubsub_topic:
            (
                p
                | "Read from GCS" >> beam.io.ReadFromText(dataflow_opts.input_path)
                | "To Pub/Sub JSON" >> beam.Map(lambda x: x.strip().encode('utf-8'))
                | "Write to Pub/Sub" >> beam.io.WriteToPubSub(topic=dataflow_opts.pubsub_topic)
            )

        # Streaming load: Pub/Sub to BigQuery
        if dataflow_opts.pubsub_input:
            pubsub_msgs = (
                p
                | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=dataflow_opts.pubsub_input).with_output_types(bytes)
                | "Decode Bytes" >> beam.Map(lambda x: x.decode('utf-8'))
            )

            parsed = pubsub_msgs | "Parse with Schema" >> beam.ParDo(parse_fn).with_outputs(ParseMessage.OUTPUT_ERROR_TAG, main='valid')

            # Write valid records to BigQuery
            parsed.valid | "Write Valid to BQ" >> WriteToBigQuery(
                table=dataflow_opts.table,
                schema=main_schema_bq,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )

            # Write failed/error records to error table
            parsed[ParseMessage.OUTPUT_ERROR_TAG] | "Write Errors to BQ" >> WriteToBigQuery(
                table=dataflow_opts.error_table,
                schema=error_schema_bq,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )

# Entry point
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
