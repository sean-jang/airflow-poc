from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


with DAG (
    dag_id='load_bronze_chartReceipts',
    schedule='30 16 * * *',
    start_date=datetime(2024, 1, 31),
    max_active_runs=1,
    catchup=False
) as dag:
    
    extract_load = SqlToS3Operator(
        task_id='append_table',
        query="SELECT * FROM chartReceipts WHERE SUBSTR(DATE_ADD(createdAt, interval 9 hour), 1, 10) = '{{ next_ds }}'",
        s3_bucket='goodoc-sync-rds-data',
        s3_key='bronze/goodoc-rds/chartReceipts/date_id={{ next_ds }}/0.csv',
        replace=True,
        sql_conn_id='goodoc-rds',
        pd_kwargs={'index': False, 'encoding': 'utf-8'}
    )

    trigger_bronze_to_gold=TriggerDagRunOperator(
        task_id='trigger_bronze_to_gold',
        trigger_dag_id='transform_to_gold_daily_chartReceipts'
    )
    
    extract_load >> trigger_bronze_to_gold