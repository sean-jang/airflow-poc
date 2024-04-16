from datetime import datetime, timedelta

from airflow import DAG
from airflow.macros import ds_add
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python import PythonOperator

from tools.extract import S3Extractor
from tools.transform import ChartReceiptsTransformer
from tools.load import S3Loader


with DAG(
    dag_id="elt_goodocRDS_chartReceipts",
    schedule="30 17 * * *",
    start_date=datetime(2024, 1, 31),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    
    bronze = SqlToS3Operator(
        task_id="extract_and_load_to_bronze",
        query="SELECT * FROM chartReceipts WHERE SUBSTR(DATE_ADD(createdAt, interval 9 hour), 1, 10) = '{{ next_ds }}'",
        s3_bucket="goodoc-sync-rds-data",
        s3_key="bronze/goodoc-rds/chartReceipts/date_id={{ next_ds }}/0.csv",
        replace=True,
        sql_conn_id="goodoc-rds",
        pd_kwargs={"index": False, "encoding": "utf-8"}
    )

    bronze_sensor = S3KeySensor(
        task_id="bronze_sensor",
        bucket_key="bronze/goodoc-rds/chartReceipts/date_id={{ next_ds }}/0.csv",
        bucket_name="goodoc-sync-rds-data",
        mode="poke",
        poke_interval=180,
        timeout=600
    )

    extract_bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=S3Extractor().extract,
        op_kwargs={
            "bucket": "goodoc-sync-rds-data",
            "key": "bronze/goodoc-rds/chartReceipts/date_id={{ next_ds }}/0.csv"
        }
    )

    transform_to_gold = PythonOperator(
        task_id="transform_to_gold",
        python_callable=ChartReceiptsTransformer.transform,
        op_kwargs={
            "df_task_id": "extract_bronze",
            "next_ds": "{{ next_ds }}"
        }
    )

    load = PythonOperator(
        task_id="load_gold",
        python_callable=S3Loader().load,
        op_kwargs={
            "df_task_id": "transform_to_gold",
            "bucket": "goodoc-sync-rds-data",
            "key": "gold/daily_key_metrics/date_id={{ next_ds }}/chartReceipts_summary.csv",
        }
    )

    bronze >> bronze_sensor >> extract_bronze >> transform_to_gold >> load