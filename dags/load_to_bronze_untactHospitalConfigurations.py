from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


with DAG (
    dag_id='load_bronze_untactHospitalConfigurations',
    schedule='20 15 * * *',
    start_date=datetime(2024, 1, 31),
    catchup=False
) as dag:
    
    extract_load=SqlToS3Operator(
        task_id='load_table_to_s3',
        query='SELECT * FROM untactHospitalConfigurations',
        s3_bucket='goodoc-sync-rds-data',
        s3_key='bronze/goodoc-rds/untactHospitalConfigurations/date_id={{ next_ds }}/0.csv',
        replace=True,
        sql_conn_id='goodoc-rds',
        pd_kwargs={'index': False, 'encoding': 'utf-8'}
    )

    extract_load