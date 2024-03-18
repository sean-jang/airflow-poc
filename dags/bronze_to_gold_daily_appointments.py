
import io
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.exceptions import AirflowException


@dag(
    start_date=datetime(2024,1,31),
)
def transform_to_gold_daily_appointments():
    @task()
    def extract(**context):
        ti: TaskInstance = context["task_instance"]
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]
        s3_session = boto3.Session(region_name='ap-northeast-2')
        s3_client = s3_session.client('s3')

        response = s3_client.get_object(Bucket='goodoc-sync-rds-data',
                                        Key=f'bronze/goodoc-rds/appointments/date_id={execution_date}/0.csv')
        
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df = pd.read_csv(response.get("Body"))
            return df
        else:
            raise AirflowException(f"Unsuccessful S3 get_object response. Status - {status}")
    
    @task()
    def transform(df: pd.DataFrame, **context):
        ti: TaskInstance = context["task_instance"]
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]
        df['status'] = df['status'].map(
            lambda x: 1 if x in ['completed', 'confirmed'] else 0
        )
        df_summary = pd.DataFrame(
            {
                'date_id': execution_date,
                'mobileAppointments_requests_web': np.nan,
                'mobileAppointments_requests_web_users': np.nan,
                'mobileAppointments_requests_app': len(df),
                'mobileAppointments_requests_app_users': df['userId'].nunique(),
                'mobileAppointments_requests_completed': df['status'].sum(),
                'mobileAppointments_hospitals_active': df['hospitalId'].nunique()
            }, index=[0]
        )
        return df_summary

    
    @task()
    def load(df_summary: pd.DataFrame, **context):
        ti: TaskInstance = context["task_instance"]
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]
        s3_session = boto3.Session(region_name='ap-northeast-2')
        s3_client = s3_session.client('s3')

        with io.StringIO() as csv_buffer:
            df_summary.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(Bucket='goodoc-sync-rds-data',
                                            Key=f'gold/daily_key_metrics/date_id={execution_date}/appointments_summary.csv',
                                            Body=csv_buffer.getvalue())
        
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                raise AirflowException(f"Unsuccessful S3 put_object response. Status - {status}")

    extract_bronze = extract()
    transform_to_daily = transform(extract_bronze)
    load_to_s3_gold = load(transform_to_daily)
transform_to_gold_daily_appointments()