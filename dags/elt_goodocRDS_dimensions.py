from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python import PythonOperator

from tools.extract import S3Extractor
from tools.transform import ReceiptHCTransformer, ReceiptSATransformer
from tools.transform import ReceiptsTransformer, ChartReceiptsTransformer
from tools.transform import TreatmentsTransformer, AppointmentsTransformer
from tools.load import S3Loader
from tools.query import QueryBuilder


TRANSFORMERS = {
    "receiptServiceAlliances": ReceiptSATransformer,
    "receiptHospitalConfigurations": ReceiptHCTransformer,
    "untactHospitalConfigurations": None,
    "appointmentHospitalConfigurations": None
}

with DAG(
    dag_id="elt_goodocRDS_dimensions",
    schedule="40 17 * * *",
    start_date=datetime(2024, 1, 31),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 5,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    
    tasks = dict()
    for table in TRANSFORMERS.keys():
        
        bronze = SqlToS3Operator(
            task_id=f"{table}_extract_and_load_to_bronze",
            query=f"{QueryBuilder.get_query(table, '{{ next_ds }}')}",
            s3_bucket="goodoc-sync-rds-data",
            s3_key=f"bronze/goodoc-rds/{table}/date_id={{{{ next_ds }}}}/0.csv",
            replace=True,
            sql_conn_id="goodoc-rds",
            pd_kwargs={"index": False, "encoding": "utf-8"}
        )

        if not TRANSFORMERS[table]:
            tasks[table] = bronze
            continue

        bronze_sensor = S3KeySensor(
            task_id=f"{table}_bronze_sensor",
            bucket_key=f"bronze/goodoc-rds/{table}/date_id={{{{ next_ds }}}}/0.csv",
            bucket_name="goodoc-sync-rds-data",
            mode="poke",
            poke_interval=180,
            timeout=600
        )

        extract_bronze = PythonOperator(
            task_id=f"{table}_extract_bronze",
            python_callable=S3Extractor().extract,
            op_kwargs={
                "bucket": "goodoc-sync-rds-data",
                "key": f"bronze/goodoc-rds/{table}/date_id={{{{ next_ds }}}}/0.csv"
            }
        )

        transform_to_gold = PythonOperator(
            task_id=f"{table}_transform_to_gold",
            python_callable=TRANSFORMERS[table].transform,
            op_kwargs={
                "df_task_id": f"{table}_extract_bronze",
                "next_ds": "{{ next_ds }}"
            }
        )

        load = PythonOperator(
            task_id=f"{table}_load_gold",
            python_callable=S3Loader().load,
            op_kwargs={
                "df_task_id": f"{table}_transform_to_gold",
                "bucket": "goodoc-sync-rds-data",
                "key": f"gold/daily_key_metrics/date_id={{{{ next_ds }}}}/{table}_summary.csv",
            }
        )

        tasks[table] = bronze >> bronze_sensor >> extract_bronze >> transform_to_gold >> load
    
    tasks['untactHospitalConfigurations'] >> tasks['appointmentHospitalConfigurations'] \
    >> tasks['receiptHospitalConfigurations'] >> tasks['receiptServiceAlliances']
                  
                  