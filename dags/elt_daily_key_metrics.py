import io
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.macros import ds_add
from airflow.models.variable import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException

def merge_daily_summary(ds, **context):
    next_ds = ds_add(ds, 1)
    s3_session = boto3.Session(region_name='ap-northeast-2')
    s3_client = s3_session.client('s3')
    keys = s3_client.list_objects(Bucket='goodoc-sync-rds-data',
                                  Prefix=f'gold/daily_key_metrics/date_id={next_ds}')
    keys = [i['Key'] for i in keys['Contents']]

    df_merge = pd.DataFrame()
    for s3_key in keys:
        response = s3_client.get_object(Bucket='goodoc-sync-rds-data',
                                        Key=s3_key)
        
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object {s3_key} response. Status - {status}")
            df = pd.read_csv(response.get("Body"))
            if df_merge.empty:
                df_merge = df.copy()
            else:
                df_merge = pd.merge(df_merge, df, how='left', on='date_id')

        else:
            raise AirflowException(f"Unsuccessful S3 get_object {s3_key} response. Status - {status}")
    
    df_merge.fillna(0, inplace=True)
    df_merge['newInstall_total'] = (
        df_merge['newInstall_organic_aos']
        + df_merge['newInstall_nonorganic_aos']
        + df_merge['newInstall_organic_ios']
        + df_merge['newInstall_nonorganic_ios']
    )
    df_merge['receipt_hospitals_activeRate'] = np.round(
        (df_merge['receipt_hospitals_active']/df_merge['receipt_hospitals_install']) * 100, 2
    )
    df_merge['receipt_requests_total'] = (
        df_merge['receipt_requests_tablet'] + df_merge['receipt_requests_dummy']
    )
    df_merge['dau_web'] = df_merge['dau_pc'] + df_merge['dau_mobile']
    df_merge['dau_app'] = df_merge['dau_aos'] + df_merge['dau_ios']
    df_merge['mobileReceipt_requests_total'] = (
        0  # df_merge['mobileReceipt_requests_web'] 
        + df_merge['mobileReceipt_requests_app']
    )
    df_merge['mobileReceipt_requests_total_users'] = (
        0  # df_merge['mobileReceipt_requests_web_users'] 
        + df_merge['mobileReceipt_requests_app_users']
    )
    df_merge['mobileReceipt_requests_completed'] = (
        0  # df_merge['mobileReceipt_requests_web_completed']
        + df_merge['mobileReceipt_requests_app_completed']
    )
    df_merge['mobileReceipt_requests_completedRate'] = np.round(
        (df_merge['mobileReceipt_requests_completed']/df_merge['mobileReceipt_requests_total']) * 100, 2
    )
    df_merge['mobileReceipt_requests_avgPerHospitals'] = np.round(
        df_merge['mobileReceipt_requests_total']/df_merge['mobileReceipt_hospitals_active'], 1
    )
    df_merge['mobileAppointments_requests_total'] = (
        0  # df_merge['mobileAppointments_requests_web'] 
        + df_merge['mobileAppointments_requests_app']
    )
    df_merge['mobileAppointments_requests_total_users'] = (
        0  # df_merge['mobileAppointments_requests_web_users'] 
        + df_merge['mobileAppointments_requests_app_users']
    )
    df_merge['mobileAppointments_requests_completedRate'] = np.round(
        (df_merge['mobileAppointments_requests_completed']/df_merge['mobileAppointments_requests_total']) * 100, 2
    )
    df_merge['untact_requests_total'] = (
        df_merge['untact_requests_web'] 
        + df_merge['untact_requests_app']
    )
    df_merge['untact_requests_total_users'] = (
        df_merge['untact_requests_web_users'] 
        + df_merge['untact_requests_app_users']
    )
    df_merge['untact_requests_completedRate'] = np.round(
        (df_merge['untact_requests_completed']/df_merge['untact_requests_total']) * 100, 2
    )
    df_merge['keyEvents_requests'] = (
        df_merge['mobileReceipt_requests_total']
        + df_merge['mobileAppointments_requests_total']
        + df_merge['untact_requests_total']
    )
    df_merge['keyEvents_requests_ofDAU'] = (
        np.round((df_merge['keyEvents_requests'] / (df_merge['dau_app']+df_merge['dau_web'])) * 100, 2)
    )
    df_merge['keyEvents_requests_users'] = (
        df_merge['mobileReceipt_requests_total_users']
        + df_merge['mobileAppointments_requests_total_users']
        + df_merge['untact_requests_total_users']
    )
    df_merge['keyEvents_requests_completed'] = (
        df_merge['mobileReceipt_requests_completed']
        + df_merge['mobileAppointments_requests_completed']
        + df_merge['untact_requests_completed']
    )
    df_merge['keyEvents_requests_completedRate'] = np.round(
        (df_merge['keyEvents_requests_completed']/df_merge['keyEvents_requests']) * 100, 2
    )
    context['ti'].xcom_push(key='merged_daily_key_metrics', value=df_merge)


def insert_daily_summary(ds, **context):
    df_merge = context['ti'].xcom_pull(key='merged_daily_key_metrics')
    next_ds = ds_add(ds, 1)

    conn = MySqlHook(mysql_conn_id='goodoc-rds').get_conn()
    cursor = conn.cursor()
    
    delete_query = f"DELETE FROM daily_key_metrics WHERE date_id='{next_ds}';"
    cursor.execute(delete_query)
    
    insert_query = """
        INSERT INTO daily_key_metrics (
            date_id, dau_aos, 
            dau_ios, mobileAppointments_requests_web, 
            mobileAppointments_requests_web_users, mobileAppointments_requests_app, 
            mobileAppointments_requests_app_users, mobileAppointments_requests_completed, 
            mobileAppointments_hospitals_active, newInstall_tabletToMobile, 
            reInstall_tabletToMobile, reengagement_tabletToMobile, 
            newInstall_organic_aos, newInstall_nonorganic_aos, 
            newInstall_organic_ios, newInstall_nonorganic_ios,
            receipt_requests_dummy, dau_pc, 
            dau_mobile, mobileReceipt_hospitals_available, 
            receipt_hospitals_install, receipt_hospitals_new, 
            receipt_hospitals_bounced, receipt_hospitals_active, 
            receipt_requests_tablet, mobileReceipt_requests_app, 
            mobileReceipt_requests_app_users, mobileReceipt_requests_app_completed, 
            mobileReceipt_hospitals_active, untact_requests_web, 
            untact_requests_web_users, untact_requests_app, 
            untact_requests_app_users, untact_requests_completed, 
            untact_doctors_active, untact_doctors_completed, 
            newInstall_total, receipt_hospitals_activeRate, receipt_requests_total,
            dau_web, dau_app, 
            mobileReceipt_requests_total, mobileReceipt_requests_total_users, 
            mobileReceipt_requests_completed, mobileReceipt_requests_completedRate, 
            mobileReceipt_requests_avgPerHospitals, mobileAppointments_requests_total, 
            mobileAppointments_requests_total_users, mobileAppointments_requests_completedRate, 
            untact_requests_total, untact_requests_total_users, 
            untact_requests_completedRate, keyEvents_requests, 
            keyEvents_requests_ofDAU, keyEvents_requests_users, 
            keyEvents_requests_completed, keyEvents_requests_completedRate
        ) VALUES {0};
    """.format(tuple(list(df_merge.values[0])))
    cursor.execute(insert_query)
    conn.commit()

    conn.close()


with DAG (
    dag_id='elt_daily_key_metrics',
    start_date=datetime(2024, 1, 31),
    schedule='55 23 * * *',
    catchup=False
) as dag:
    
    sensor_1 = ExternalTaskSensor(
        task_id='sensor_appsflyer',
        external_dag_id='transform_to_gold_daily_appsflyer',
        external_task_id='load',
        execution_date_fn=lambda x: x + timedelta(minutes=5),
        timeout=3600,
        poke_interval=300,
        mode='reschedule'
    )

    sensor_2 = ExternalTaskSensor(
        task_id='sensor_GA',
        external_dag_id='etl_to_bronze_daily_goodocWeb',
        external_task_id='load',
        execution_date_fn=lambda x: x - timedelta(minutes=5),
        timeout=3600,
        poke_interval=300,
        mode='reschedule'
    )

    sensor_3 = ExternalTaskSensor(
        task_id='sensor_amplitude',
        external_dag_id='transform_to_gold_daily_appDAU',
        external_task_id='elt',
        execution_date_fn=lambda x: x - timedelta(hours=5, minutes=50),
        timeout=3600,
        poke_interval=300,
        mode='reschedule'
    )

    sensor_4 = ExternalTaskSensor(
        task_id='sensor_chartReceipts',
        external_dag_id='transform_to_gold_daily_chartReceipts',
        external_task_id='bronze_to_gold',
        execution_date_fn=lambda x: x + timedelta(minutes=5),
        timeout=3600,
        poke_interval=300,
        mode='reschedule'
    )

    sensor_5 = ExternalTaskSensor(
        task_id='sensor_receiptHospitalConfigurations',
        external_dag_id='transform_to_gold_daily_receiptHospitalConfigurations',
        external_task_id='load',
        execution_date_fn=lambda x: x + timedelta(minutes=5),
        timeout=3600,
        poke_interval=300,
        mode='reschedule'
    )

    sensor_6 = ExternalTaskSensor(
        task_id='sensor_appointments',
        external_dag_id='transform_to_gold_daily_appointments',
        external_task_id='load',
        execution_date_fn=lambda x: x + timedelta(minutes=5),
        timeout=3600,
        poke_interval=300,
        mode='reschedule'
    )

    sensor_7 = ExternalTaskSensor(
        task_id='sensor_receiptServiceAlliances',
        external_dag_id='transform_to_gold_daily_receiptServiceAlliances',
        external_task_id='load',
        execution_date_fn=lambda x: x + timedelta(minutes=5),
        timeout=3600,
        poke_interval=300,
        mode='reschedule'
    )

    sensor_8 = ExternalTaskSensor(
        task_id='sensor_receipts',
        external_dag_id='transform_to_gold_daily_receipts',
        external_task_id='load',
        execution_date_fn=lambda x: x + timedelta(minutes=5),
        timeout=3600,
        poke_interval=300,
        mode='reschedule'
    )

    sensor_9 = ExternalTaskSensor(
        task_id='sensor_treatments',
        external_dag_id='transform_to_gold_daily_treatments',
        external_task_id='load',
        execution_date_fn=lambda x: x + timedelta(minutes=5),
        timeout=3600,
        poke_interval=300,
        mode='reschedule'
    )

    merge = PythonOperator(
        task_id='merge_daily_summary',
        python_callable=merge_daily_summary
    )
    
    insert = PythonOperator(
        task_id='insert_daily_summary',
        python_callable=insert_daily_summary
    )

[
    sensor_1, sensor_2, sensor_3, sensor_4, sensor_5,
    sensor_6, sensor_7, sensor_8, sensor_9
] >> merge >> insert