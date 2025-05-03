import json
import logging
import requests
import traceback
import datetime
from datetime import timedelta, datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator as BQCreateTable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'gcp_conn_id': 'google_cloud_default',
}

def transform_twitter_data(input_path, output_path):
    """
    Reads the JSON‐lines file at LOCAL_FILE_PATH,
    extracts the required fields, and writes a CSV
    with headers matching SCHEMA_FIELDS names.
    """
    import csv
    from datetime import datetime

    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', newline='', encoding='utf-8') as outfile:

        fieldnames = ['tweet_id', 'created_at', 'text',
                      'user_id', 'user_name',
                      'retweet_count', 'favorite_count', 'hashtags']
        writer = csv.DictWriter(
            outfile,
            fieldnames=fieldnames,
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()

        for line in infile:
            record = json.loads(line)
            tags = record.get('entities', {}).get('hashtags', [])
            hashtag_texts = ",".join(h.get('text', "") for h in tags)

            dt = datetime.strptime(
                record['created_at'],
                '%a %b %d %H:%M:%S %z %Y'
            ).isoformat()

            writer.writerow({
                'tweet_id': str(record.get('id')),
                'created_at': dt,
                'text': record.get('text'),
                'user_id': str(record.get('user', {}).get('id')),
                'user_name': record.get('user', {}).get('screen_name'),
                'retweet_count': record.get('retweet_count', 0),
                'favorite_count': record.get('favorite_count', 0),
                'hashtags': hashtag_texts,
            })

with DAG(
    'twitter_data_pipeline_with_hashtags',
    default_args=default_args,
    description='Ingest Twitter CSV into GCS then BigQuery',
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=['twitter', 'gcs', 'bigquery'],
) as dag:

    # ─── GCP CONFIGURATION ──────────────────────────────────────────────────────
    PROJECT_ID      = 'twitter-trend-analysis-457900'      # ← your project
    DATASET_NAME    = 'twitter_trends'                     # ← dataset in BigQuery
    TABLE_NAME      = 'tweets'                             # ← table in that dataset
    BUCKET_NAME     = 'twitter_trends'                     # ← your GCS bucket
    TXT_FILE_NAME = 'twitter_data.txt'  # raw JSON-lines file in dags/
    CSV_FILE_NAME = 'twitter_data.csv'  # transformed CSV filename
    # Local paths inside container
    LOCAL_FILE_PATH = f'/opt/airflow/dags/{TXT_FILE_NAME}'  
    TRANSFORMED_FILE_PATH = f'/opt/airflow/dags/{CSV_FILE_NAME}'
    SLACK_WEBHOOK = 'https://hooks.slack.com/services/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'

    # Define the schema: adjust names/types to match your CSV
    SCHEMA_FIELDS = [
        {'name': 'tweet_id',      'type': 'STRING',    'mode': 'REQUIRED'},
        {'name': 'created_at',    'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'text',          'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'user_id',       'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'user_name',     'type': 'STRING',    'mode': 'NULLABLE'},
        {'name': 'retweet_count', 'type': 'INTEGER',   'mode': 'NULLABLE'},
        {'name': 'favorite_count','type': 'INTEGER',   'mode': 'NULLABLE'},
        {'name': 'hashtags',      'type': 'STRING',    'mode': 'NULLABLE'},
    ]

    # ─── TASK 1: Transform Twitter data ────────────────────────────────────────────
    transform_task = PythonOperator(
        task_id='transform_twitter_data',
        python_callable=transform_twitter_data,
        op_kwargs={
            'input_path': LOCAL_FILE_PATH,
            'output_path': TRANSFORMED_FILE_PATH,
        },
    )

    # ─── TASK 2: Upload CSV to GCS ───────────────────────────────────────────────
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src=TRANSFORMED_FILE_PATH,
        dst=CSV_FILE_NAME,
        bucket=BUCKET_NAME,
        mime_type='text/csv',
    )

    # ─── TASK 3: Create BigQuery Dataset (if not exists) ────────────────────────
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        location='US',
    )

    # ─── TASK 4: Create BigQuery Table (if not exists) ─────────────────────────
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_bq_table',
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields=SCHEMA_FIELDS,
    )

    # ─── TASK 4: Load from GCS into BigQuery ────────────────────────────────────
    load_data = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[CSV_FILE_NAME],
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}',
        schema_fields=SCHEMA_FIELDS,
        source_format='CSV',
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        max_bad_records=100,
    )

    # ─── TASK 6: Create table for hashtag counts ─────────────────────────────────
    create_hashtag_table = BQCreateTable(
        task_id='create_hashtag_table',
        dataset_id=DATASET_NAME,
        table_id='hashtag_counts',
        schema_fields=[
            {'name': 'hashtag', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'count',   'type': 'INTEGER','mode': 'REQUIRED'},
            {'name': 'timestamp','type': 'TIMESTAMP','mode': 'REQUIRED'},
        ],
        project_id=PROJECT_ID,
        location='US',
    )

    # ─── TASK 7: Extract and count hashtags ───────────────────────────────────────
    analyze_hashtags = BigQueryInsertJobOperator(
        task_id='analyze_hashtags',
        configuration={
            'query': {
                'query': f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_NAME}.hashtag_counts`
                WITH extracted_hashtags AS (
                  SELECT REGEXP_EXTRACT_ALL(LOWER(text), r'(#[a-z0-9_]+)') AS hashtags
                  FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
                  WHERE text IS NOT NULL
                ),
                flattened_hashtags AS (
                  SELECT hashtag
                  FROM extracted_hashtags, UNNEST(hashtags) AS hashtag
                )
                SELECT
                  hashtag,
                  COUNT(*) AS count,
                  CURRENT_TIMESTAMP() AS timestamp
                FROM flattened_hashtags
                GROUP BY hashtag
                ORDER BY count DESC
                """,
                'useLegacySql': False,
            }
        },
        location='US',
    )

# ─── SLACK NOTIFICATIONS ────────────────────────────────────────────────────
def send_slack_notification(webhook_url, message, **context):
    """
    Send a notification to Slack via webhook_url, including an optional stacktrace and DAG/task info.
    """
    # Grab the exception (if any) and format the traceback
    tb = context.get('exception')
    if tb:
        # Full traceback as a string
        full_tb = ''.join(traceback.format_exception(type(tb), tb, tb.__traceback__))
        # Wrap in triple backticks for Slack code formatting
        tb_block = f"\n```{full_tb}```"
    else:
        tb_block = ""

    ti = context.get('ti')
    if ti:
        dag_id = ti.dag_id
        task_id = ti.task_id
        exec_date = context.get('execution_date') or ti.execution_date
        log_url = ti.log_url
        link_block = f"\n*DAG*: {dag_id}\n*Task*: {task_id}\n*Execution*: {exec_date}\n*Logs*: {log_url}"
    else:
        link_block = ""

    payload = {
        "text": f"{message}\nTimestamp: {datetime.datetime.utcnow():%Y-%m-%d %H:%M:%S UTC}{link_block}{tb_block}"
    }
    try:
        response = requests.post(webhook_url, json=payload, timeout=5)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to send Slack notification: {e}")

notify_slack_success = PythonOperator(
    task_id='notify_slack_success',
    python_callable=send_slack_notification,
    op_kwargs={
        'webhook_url': f'{SLACK_WEBHOOK}',
        'message': f":white_check_mark: DAG {dag.dag_id} completed successfully."
    },
    trigger_rule='all_success',
    provide_context=True,
)

notify_slack_failure = PythonOperator(
    task_id='notify_slack_failure',
    python_callable=send_slack_notification,
    op_kwargs={
        'webhook_url': SLACK_WEBHOOK,
        'message': f":x: DAG {dag.dag_id} failed. Please check logs."
    },
    trigger_rule='one_failed',
    provide_context=True,
)

# ─── SET DEPENDENCIES ───────────────────────────────────────────────────────
transform_task >> upload_to_gcs >> create_dataset >> create_table \
>> load_data >> create_hashtag_table >> analyze_hashtags >> index_to_es \
>> [notify_slack_success, notify_slack_failure]

