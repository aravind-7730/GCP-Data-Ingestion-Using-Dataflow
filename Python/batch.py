import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import storage
from apache_beam.io.gcp import bigquery_tools
import datetime
import random
import uuid

# Define the schema for the error table to capture failed records
ERROR_SCHEMA_DICT = {
    'fields': [
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'error_message', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ]
}

# Read and parse BigQuery schema JSON from GCS
def read_schema_from_gcs(file_path: str) -> dict:
    try:
        bucket_name = file_path.split('/')[2]
        blob_name = '/'.join(file_path.split('/')[3:])
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        schema_string = blob.download_as_string().decode('utf-8')
        schema_data = json.loads(schema_string)

        # Normalize schema to dictionary format with a "fields" key
        if isinstance(schema_data, list):
            return {"fields": schema_data}
        elif isinstance(schema_data, dict) and "fields" in schema_data:
            return schema_data
        else:
            raise ValueError(f"Invalid schema format: {type(schema_data)}")
    except Exception as e:
        logging.error(f"Error reading or parsing schema from GCS {file_path}: {e}", exc_info=True)
        raise

# Convert string values from JSON to appropriate BigQuery types
def parse_value(value, field_schema_type, field_name="<unknown>"):
    if value is None:
        return None
    try:
        if field_schema_type == 'INTEGER':
            return int(value)
        elif field_schema_type in ['FLOAT', 'NUMERIC', 'BIGNUMERIC']:
            return float(value)
        elif field_schema_type == 'BOOLEAN':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                if value.lower() == 'true':
                    return True
                elif value.lower() == 'false':
                    return False
                else:
                    raise ValueError(f"expects BOOLEAN ('true'/'false'), got string '{value}'")
            raise ValueError(f"expects BOOLEAN, got type {type(value)}")
        elif field_schema_type == 'STRING':
            return str(value)
        elif field_schema_type == 'TIMESTAMP':
            if isinstance(value, (int, float)):
                # Convert epoch timestamp to ISO format
                return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc).isoformat()
            return str(value)
        elif field_schema_type == 'DATE':
            return str(value)
        return value
    except (ValueError, TypeError) as e:
        raise ValueError(f"Field '{field_name}': type {field_schema_type}, error parsing value '{str(value)[:50]}': {e}")

# Beam DoFn class to parse and validate each JSON record
class ParseMessage(beam.DoFn):
    OUTPUT_ERROR_TAG = 'error'  # Side output tag for invalid/error records

    def __init__(self, schema_dict_for_parsing):
        self.schema_input_dict = schema_dict_for_parsing
        self.table_schema = None
        self.field_schemas_map = {}

    def setup(self):
        # Parse schema for use within the DoFn
        if not isinstance(self.schema_input_dict, dict) or "fields" not in self.schema_input_dict:
            raise TypeError("Schema input to ParseMessage must be a dictionary with a 'fields' key at setup.")
        try:
            json_string_to_parse = json.dumps(self.schema_input_dict)
            self.table_schema = bigquery_tools.parse_table_schema_from_json(json_string_to_parse)
            self.field_schemas_map = {field.name: field for field in self.table_schema.fields}
        except Exception as e:
            logging.error(f"Failed to parse schema in ParseMessage.setup(): {e}", exc_info=True)
            raise

    # Recursive parser for nested RECORD types
    def _parse_record(self, record_data, record_field_schemas, parent_field_name="<record>"):
        if not isinstance(record_data, dict):
            raise ValueError(f"Field '{parent_field_name}': Expected dict for RECORD, got {type(record_data)}")

        parsed_record = {}
        for sub_field_schema in record_field_schemas:
            sub_field_name = sub_field_schema.name
            full_sub_field_name = f"{parent_field_name}.{sub_field_name}"
            sub_value = record_data.get(sub_field_name)

            if sub_value is None:
                if sub_field_schema.mode == 'REQUIRED':
                    raise ValueError(f"REQUIRED field '{full_sub_field_name}' is missing in nested record.")
                parsed_record[sub_field_name] = None
                continue

            # Handle nested records or repeated sub-fields
            if sub_field_schema.type == 'RECORD':
                parsed_record[sub_field_name] = self._parse_record(sub_value, sub_field_schema.fields, full_sub_field_name)
            elif sub_field_schema.mode == 'REPEATED':
                if isinstance(sub_value, str) and sub_field_name == 'categories':
                    processed_value = [cat.strip() for cat in sub_value.split(',') if cat.strip()]
                elif isinstance(sub_value, list):
                    if sub_field_schema.fields:
                        processed_value = [self._parse_record(item, sub_field_schema.fields, f"{full_sub_field_name}[{i}]") for i, item in enumerate(sub_value)]
                    else:
                        processed_value = [parse_value(item, sub_field_schema.type, f"{full_sub_field_name}[{i}]") for i, item in enumerate(sub_value)]
                else:
                    raise ValueError(f"REPEATED field '{full_sub_field_name}' expects list, got {type(sub_value)}")
                parsed_record[sub_field_name] = processed_value
            else:
                parsed_record[sub_field_name] = parse_value(sub_value, sub_field_schema.type, full_sub_field_name)
        return parsed_record

    def process(self, element):
        line_str = element
        error_id_val = abs(hash(line_str))

        if self.table_schema is None:
            yield beam.pvalue.TaggedOutput(self.OUTPUT_ERROR_TAG, {
                'id': error_id_val,
                'error_message': "InternalError: DoFn Schema not initialized on worker.",
                'timestamp': datetime.datetime.utcnow().isoformat() + "Z"
            })
            return
        try:
            line_dict = json.loads(line_str)
            output_row = {}

            for field_schema in self.table_schema.fields:
                field_name = field_schema.name
                value = line_dict.get(field_name)

                if value is None:
                    if field_schema.mode == 'REQUIRED':
                        raise ValueError(f"REQUIRED field '{field_name}' is missing from input JSON.")
                    output_row[field_name] = None
                    continue

                # Handle repeated, nested, and simple field types
                if field_schema.mode == 'REPEATED':
                    if field_name == 'categories' and isinstance(value, str):
                        output_row[field_name] = [cat.strip() for cat in value.split(',') if cat.strip()]
                    elif isinstance(value, list):
                        if field_schema.fields:
                            output_row[field_name] = [self._parse_record(item, field_schema.fields, f"{field_name}[{i}]") for i, item in enumerate(value)]
                        else:
                            output_row[field_name] = [parse_value(item, field_schema.type, f"{field_name}[{i}]") for i, item in enumerate(value)]
                    else:
                        raise ValueError(f"REPEATED field '{field_name}' expects a list")
                elif field_schema.type == 'RECORD':
                    output_row[field_name] = self._parse_record(value, field_schema.fields, field_name)
                else:
                    output_row[field_name] = parse_value(value, field_schema.type, field_name)

            yield output_row

        except (json.JSONDecodeError, ValueError) as e:
            # Capture data validation or JSON parsing errors
            yield beam.pvalue.TaggedOutput(self.OUTPUT_ERROR_TAG, {
                'id': error_id_val,
                'error_message': f"{type(e).__name__}: {str(e)}",
                'timestamp': datetime.datetime.utcnow().isoformat() + "Z"
            })
        except Exception as e:
            # Catch-all for unexpected errors
            yield beam.pvalue.TaggedOutput(self.OUTPUT_ERROR_TAG, {
                'id': error_id_val,
                'error_message': f"UnexpectedError: {str(e)}",
                'timestamp': datetime.datetime.utcnow().isoformat() + "Z"
            })

# Define command-line arguments for the pipeline
class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_path', type=str, required=True)
        parser.add_argument('--table', type=str, required=True)
        parser.add_argument('--error_table', type=str, required=True)
        parser.add_argument('--schema_path', type=str, required=True)

# Main pipeline function
def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args_list = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args_list)
    custom_options = pipeline_options.view_as(DataflowOptions)

    main_table_schema_object_for_sink = None
    schema_dict_for_dofn = None
    error_table_schema_object_for_sink = None

    try:
        # Load schema from GCS for BigQuery writing and JSON validation
        schema_dict_from_gcs = read_schema_from_gcs(custom_options.schema_path)
        schema_dict_for_dofn = schema_dict_from_gcs

        main_table_schema_object_for_sink = bigquery_tools.parse_table_schema_from_json(
            json.dumps(schema_dict_from_gcs)
        )

        error_table_schema_object_for_sink = bigquery_tools.parse_table_schema_from_json(
            json.dumps(ERROR_SCHEMA_DICT)
        )
    except Exception as e:
        logging.error(f"CRITICAL: Failed to load or parse schema initially: {e}", exc_info=True)
        raise

    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read raw JSON lines from GCS
        lines = p | 'ReadFromGCS' >> beam.io.ReadFromText(custom_options.input_path)

        # Parse and validate records against schema
        parsed_data = lines | 'ParseAndValidateJson' >> beam.ParDo(
            ParseMessage(schema_dict_for_dofn)
        ).with_outputs(ParseMessage.OUTPUT_ERROR_TAG, main='valid_rows')

        valid_rows = parsed_data.valid_rows
        error_rows = parsed_data[ParseMessage.OUTPUT_ERROR_TAG]

        # Write successfully parsed data to BigQuery table
        valid_rows | 'WriteValidToBigQuery' >> WriteToBigQuery(
            table=custom_options.table,
            schema=main_table_schema_object_for_sink,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )

        # Write failed records (errors) to BigQuery error table
        error_rows | 'WriteErrorsToBigQuery' >> WriteToBigQuery(
            table=custom_options.error_table,
            schema=error_table_schema_object_for_sink,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
