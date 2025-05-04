# Import necessary modules from Airflow and Python
from __future__ import annotations

import pendulum
import requests
import json
import os  # Import os for file path manipulation

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
# Import the Google Cloud Storage and BigQuery client libraries
from google.cloud import storage
from google.cloud import bigquery
# Import for explicit service account credentials
from google.oauth2 import service_account
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# Define the path to the Google Service Account key file inside the container
SERVICE_ACCOUNT_KEY_PATH = "/opt/airflow/credentials/service-account.json"

# Define the GDC API endpoints
GDC_FILES_ENDPT = "https://api.gdc.cancer.gov/files"
GDC_CASES_ENDPT = "https://api.gdc.cancer.gov/cases"

# Define BigQuery details
BIGQUERY_PROJECT_ID = "rock-extension-450317-c9"
BIGQUERY_DATASET_ID = "gdc_data"
BIGQUERY_TABLE_ID = "methylation_metadata"

# Define Google Cloud Storage details for staging
GCS_BUCKET_NAME = "gdc_data_stage"
GCS_STAGING_PATH_TEMPLATE = "gdc_methylation_metadata/{{ ds }}/gdc_methylation_metadata_{{ run_id }}.jsonl"

def query_gdc_api(endpoint: str, method: str = "GET", params: dict | None = None, data: dict | None = None, headers: dict | None = None) -> list:
    """
    Generic function to query the GDC API.

    Args:
        endpoint (str): The API endpoint URL.
        method (str): HTTP method ('GET' or 'POST').
        params (dict, optional): URL parameters for GET requests. Defaults to None.
        data (dict, optional): Request body data for POST requests. Defaults to None.
        headers (dict, optional): Request headers. Defaults to None.

    Returns:
        list: A list of hits from the API response, or an empty list if an error occurs or no hits.
    """
    try:
        if method.upper() == "GET":
            response = requests.get(endpoint, params=params, headers=headers)
        elif method.upper() == "POST":
            response = requests.post(endpoint, json=data, headers=headers)
        else:
            print(f"Unsupported HTTP method: {method}")
            return []

        response.raise_for_status()
        response_json = response.json()
        return response_json.get("data", {}).get("hits", [])

    except requests.exceptions.RequestException as e:
        print(f"Error querying {endpoint}: {e}")
        return []
    except json.JSONDecodeError:
        print(f"Error decoding JSON response from {endpoint}")
        return []

def extract_and_upload_gdc_methylation_data(**context):
    """
    Airflow task function to extract DNA Methylation file info and associated case/sample metadata from GDC.
    Uploads the extracted data as newline-delimited JSON to Google Cloud Storage.
    Pushes the GCS object path to XCom.
    """
    primary_site = ["Lung"]
    race = ["white"]
    gender = ["female"]
    max_files_to_retrieve = "5"

    # --- Step 1: Find DNA Methylation Files and Collect Case IDs ---
    file_filters = {
        "op": "and",
        "content": [
            {
                "op": "in",
                "content": {
                    "field": "files.data_category",
                    "value": ["DNA Methylation"]
                }
            },
        ]
    }

    file_fields = "file_id,file_name,cases.case_id,cases.samples.sample_id,cases.samples.sample_type,cases.samples.submitter_id"

    file_params = {
        "filters": json.dumps(file_filters),
        "fields": file_fields,
        "format": "JSON",
        "size": max_files_to_retrieve
    }

    print("Task: Querying GDC files endpoint for DNA Methylation data...")
    file_results = query_gdc_api(GDC_FILES_ENDPT, method="GET", params=file_params)
    if not file_results:
        print("Task: No DNA Methylation files found matching the criteria.")
        context['ti'].xcom_push(key='gcs_object_path', value=None)
        return []

    print(f"Task: Found {len(file_results)} DNA Methylation files.")

    # Extract unique case IDs and file-to-case/sample mapping
    case_uuid_list = []
    file_case_sample_map = {}

    for file_entry in file_results:
        file_id = file_entry["file_id"]
        if file_entry.get("cases"):
            case_id = file_entry["cases"][0]["case_id"]
            case_uuid_list.append(case_id)

            file_case_sample_map[file_id] = {
                "case_id": case_id,
                "file_name": file_entry.get("file_name"),
                "samples": file_entry.get("cases")[0].get("samples", [])
            }

    unique_case_uuid_list = list(set(case_uuid_list))
    print(f"Task: Found {len(unique_case_uuid_list)} unique cases associated with these files.")

    # --- Step 2: Retrieve Detailed Case Metadata ---
    case_metadata_map = {}
    if unique_case_uuid_list:
        case_fields = "case_id,submitter_id,demographic.gender,demographic.race,demographic.ethnicity,demographic.year_of_birth,diagnoses.primary_diagnosis,diagnoses.tissue_or_organ_of_origin,samples.sample_id,samples.submitter_id,samples.sample_type,samples.composition,samples.days_to_collection,samples.portions.analytes.analyte_id,samples.portions.analytes.analyte_type,samples.portions.analytes.aliquots.aliquot_id"

        case_filters = {
            "op": "in",
            "content": {
                "field": "case_id",
                "value": unique_case_uuid_list
            }
        }

        print("Task: Querying GDC cases endpoint for associated sample metadata...")
        case_results = query_gdc_api(GDC_CASES_ENDPT, method="POST", data={"filters": case_filters, "fields": case_fields, "format": "JSON", "size": len(unique_case_uuid_list)})

        if case_results is not None:
            case_metadata_map = {case['case_id']: case for case in case_results}
            print(f"Task: Retrieved metadata for {len(case_metadata_map)} cases.")
        else:
            print("Task: Failed to retrieve case metadata.")

    # --- Step 3: Combine File and Case/Sample Metadata ---
    combined_data = []

    for file_id, file_info in file_case_sample_map.items():
        case_id = file_info["case_id"]
        case_metadata = case_metadata_map.get(case_id)

        if case_metadata:
            entry = {
                "file_id": file_id,
                "file_name": file_info.get("file_name"),
                "case_id": case_id,
                "case_submitter_id": case_metadata.get("submitter_id"),
                "gender": case_metadata.get("demographic", {}).get("gender"),
                "race": case_metadata.get("demographic", {}).get("race"),
                "ethnicity": case_metadata.get("demographic", {}).get("ethnicity"),
                "year_of_birth": case_metadata.get("demographic", {}).get("year_of_birth"),
                "primary_diagnosis": case_metadata.get("diagnoses", [{}])[0].get("primary_diagnosis"),
                "tissue_or_organ_of_origin": case_metadata.get("diagnoses", [{}])[0].get("tissue_or_organ_of_origin"),
                "samples": []
            }

            for sample in case_metadata.get("samples", []):
                sample_entry = {
                    "sample_id": sample.get("sample_id"),
                    "sample_submitter_id": sample.get("submitter_id"),
                    "sample_type": sample.get("sample_type"),
                    "composition": sample.get("composition"),
                    "days_to_collection": sample.get("days_to_collection"),
                    "analytes": []
                }
                for portion in sample.get("portions", []):
                    for analyte in portion.get("analytes", []):
                        analyte_entry = {
                            "analyte_id": analyte.get("analyte_id"),
                            "analyte_type": analyte.get("analyte_type"),
                            "aliquots": []
                        }
                        for aliquot in analyte.get("aliquots", []):
                            aliquot_entry = {
                                "aliquot_id": aliquot.get("aliquot_id")
                            }
                            analyte_entry["aliquots"].append(aliquot_entry)
                        sample_entry["analytes"].append(analyte_entry)
                entry["samples"].append(sample_entry)

            combined_data.append(entry)

    # --- Step 4: Upload the Combined Data to GCS ---
    if not combined_data:
        print("Task: No data to upload to GCS.")
        context['ti'].xcom_push(key='gcs_object_path', value=None)
        return

    gcs_object_path = GCS_STAGING_PATH_TEMPLATE.replace("{{ ds }}", context['ds']).replace("{{ run_id }}", context['dag_run'].run_id)

    print(f"Task: Uploading combined metadata to gs://{GCS_BUCKET_NAME}/{gcs_object_path}")

    try:
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_KEY_PATH)
        storage_client = storage.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_object_path)

        jsonl_data = "\n".join([json.dumps(record) for record in combined_data])
        blob.upload_from_string(jsonl_data, content_type="application/json")

        print(f"Task: Data uploaded successfully to gs://{GCS_BUCKET_NAME}/{gcs_object_path}")
        context['ti'].xcom_push(key='gcs_object_path', value=gcs_object_path)

    except Exception as e:
        print(f"Task: Error uploading data to GCS: {e}")
        raise e

def load_gcs_to_bigquery_using_client(**context):
    """
    Airflow task function to load data from GCS to BigQuery using the google-cloud-bigquery client library.
    Pulls the GCS object path from XCom.
    """
    gcs_object_path = context['ti'].xcom_pull(task_ids='extract_and_upload_gdc_methylation_data', key='gcs_object_path')

    if gcs_object_path is None:
        print("Task: No GCS object path found in XCom. Skipping BigQuery load.")
        return

    uri = f"gs://{GCS_BUCKET_NAME}/{gcs_object_path}"
    table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"

    print(f"Task: Loading data from {uri} to BigQuery table {table_id}")

    try:
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_KEY_PATH)
        bigquery_client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField("file_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("file_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("case_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("case_submitter_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("race", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("ethnicity", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("year_of_birth", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("primary_diagnosis", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("tissue_or_organ_of_origin", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("samples", "RECORD", mode="REPEATED",
                                    fields=[
                                        bigquery.SchemaField("sample_id", "STRING", mode="NULLABLE"),
                                        bigquery.SchemaField("sample_submitter_id", "STRING", mode="NULLABLE"),
                                        bigquery.SchemaField("sample_type", "STRING", mode="NULLABLE"),
                                        bigquery.SchemaField("composition", "STRING", mode="NULLABLE"),
                                        bigquery.SchemaField("days_to_collection", "INTEGER", mode="NULLABLE"),
                                        bigquery.SchemaField("analytes", "RECORD", mode="REPEATED",
                                                            fields=[
                                                                bigquery.SchemaField("analyte_id", "STRING", mode="NULLABLE"),
                                                                bigquery.SchemaField("analyte_type", "STRING", mode="NULLABLE"),
                                                                bigquery.SchemaField("aliquots", "RECORD", mode="REPEATED",
                                                                                    fields=[
                                                                                        bigquery.SchemaField("aliquot_id", "STRING", mode="NULLABLE"),
                                                                                    ]
                                                                                    ),
                                                            ]
                                                            ),
                                    ]
                                    ),
            ],
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        load_job = bigquery_client.load_table_from_uri(
            uri,
            table_id,
            location="us-east1",
            job_config=job_config,
        )

        print(f"Task: BigQuery load job started: {load_job.job_id}")
        load_job.result()
        print(f"Task: BigQuery load job completed. Loaded {load_job.output_rows} rows.")

    except Exception as e:
        print(f"Task: Error during BigQuery load job: {e}")
        raise e

# Define the DAG
with DAG(
    dag_id="gdc_methylation_extraction_load_pipeline_client",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["gdc", "elt", "methylation", "extraction", "bigquery", "load", "gcs", "client"],
    doc_md="""
    ### GDC DNA Methylation Extraction and BigQuery Load DAG (using BigQuery Client)

    This DAG extracts DNA methylation file information and associated sample metadata
    from the G Genomic Data Commons (GDC) API based on specified criteria (e.g., primary site, race, gender).
    The extracted data is uploaded to a Google Cloud Storage bucket and then loaded into a Google BigQuery table
    using the google-cloud-bigquery client library within a Python task.
    """,
) as dag:
    extract_and_upload_task = PythonOperator(
        task_id="extract_and_upload_gdc_methylation_data",
        python_callable=extract_and_upload_gdc_methylation_data,
    )

    load_to_bigquery_client_task = PythonOperator(
        task_id="load_gcs_to_bigquery_using_client",
        python_callable=load_gcs_to_bigquery_using_client,
    )

    run_dbt = KubernetesPodOperator(
        namespace='default',
        image='gcr.io/rock-extension-450317-c9/dbt-core:latest',
        cmds=["dbt"],
        arguments=["run"],
        name="run-dbt",
        task_id="run_dbt_task",
        is_delete_operator_pod=False,  # Changed to True for cleanup
    )

    extract_and_upload_task >> load_to_bigquery_client_task >> run_dbt