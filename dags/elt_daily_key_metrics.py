from datetime import datetime, timedelta

from airflow import DAG
from airflow.macros import ds_add
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException

from tools.extract import S3Extractor
from tools.transform import DailyKeyMetricsTransformer
from tools.load import S3Loader
from tools.query import QueryBuilder


def insert_into_daily_key_metrics(next_ds, **context):
    TABLE_NAME = 'daily_key_metrics'
    INSERT_COLUMNS = [
        "date_id", "dau_aos", 
        "dau_ios", "mobileAppointments_requests_web", 
        "mobileAppointments_requests_web_users", "mobileAppointments_requests_app", 
        "mobileAppointments_requests_app_users", "mobileAppointments_requests_completed", 
        "mobileAppointments_hospitals_active", "newInstall_tabletToMobile", 
        "reInstall_tabletToMobile", "reengagement_tabletToMobile", 
        "newInstall_organic_aos", "newInstall_nonorganic_aos", 
        "newInstall_organic_ios", "newInstall_nonorganic_ios",
        "receipt_requests_dummy", "dau_pc", 
        "dau_mobile", "mobileReceipt_hospitals_available", 
        "receipt_hospitals_install", "receipt_hospitals_new", 
        "receipt_hospitals_bounced", "receipt_hospitals_active", 
        "receipt_requests_tablet", "mobileReceipt_requests_app", 
        "mobileReceipt_requests_app_users", "mobileReceipt_requests_app_completed", 
        "mobileReceipt_hospitals_active", "untact_requests_web", 
        "untact_requests_web_users", "untact_requests_app", 
        "untact_requests_app_users", "untact_requests_completed", 
        "untact_doctors_active", "untact_doctors_completed", 
        "newInstall_total", "receipt_hospitals_activeRate", "receipt_requests_total",
        "dau_web", "dau_app", 
        "mobileReceipt_requests_total", "mobileReceipt_requests_total_users", 
        "mobileReceipt_requests_completed", "mobileReceipt_requests_completedRate", 
        "mobileReceipt_requests_avgPerHospitals", "mobileAppointments_requests_total", 
        "mobileAppointments_requests_total_users", "mobileAppointments_requests_completedRate", 
        "untact_requests_total", "untact_requests_total_users", 
        "untact_requests_completedRate", "keyEvents_requests", 
        "keyEvents_requests_ofDAU", "keyEvents_requests_users", 
        "keyEvents_requests_completed", "keyEvents_requests_completedRate"
    ]

    conn = MySqlHook(mysql_conn_id='goodoc-rds').get_conn()
    cursor = conn.cursor()
    
    delete_query = QueryBuilder.delete_from_table(table=TABLE_NAME,
                                                  partition_id=next_ds)
    cursor.execute(delete_query)
    
    df = context["ti"].xcom_pull(task_ids="transform_to_daily_key_metrics")
    insert_values = tuple(list(df.values[0]))
    insert_query = QueryBuilder.insert_into_table(table=TABLE_NAME,
                                                  columns=INSERT_COLUMNS,
                                                  values=insert_values)
    
    print(insert_query)
    cursor.execute(insert_query)
    conn.commit()

    conn.close()


with DAG (
    dag_id="elt_daily_key_metrics",
    start_date=datetime(2024, 1, 31),
    schedule="55 23 * * *",
    catchup=False
) as dag:
    
    extract_gold = PythonOperator(
        task_id="extract_gold",
        python_callable=S3Extractor().extract_joined_objects,
        op_kwargs={
            "bucket": "goodoc-sync-rds-data",
            "prefix": "gold/daily_key_metrics/date_id={{ next_ds }}",
            "join": "left",
            "on": "date_id"
        }
    )

    transform = PythonOperator(
        task_id="transform_to_daily_key_metrics",
        python_callable=DailyKeyMetricsTransformer.transform,
        op_kwargs={
            "df_task_id": "extract_gold"   
        }
    )

    insert = PythonOperator(
        task_id="insert_daily_key_metrics",
        python_callable=insert_into_daily_key_metrics
    )

    extract_gold >> transform >> insert
    