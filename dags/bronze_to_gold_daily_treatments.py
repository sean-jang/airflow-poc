
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
def transform_to_gold_daily_treatments():
    @task()
    def extract(**context):
        ti: TaskInstance = context["task_instance"]
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]
        s3_session = boto3.Session(region_name='ap-northeast-2')
        s3_client = s3_session.client('s3')

        response = s3_client.get_object(Bucket='goodoc-sync-rds-data',
                                        Key=f'bronze/goodoc-rds/treatments/date_id={execution_date}/0.csv')
        
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
            lambda x: 1 if x in (['completion', 'paid', 'paiderror', 'orderprescription']) else 0
        )
        df['productType'] = df['productType'].map(
            lambda x: 'app' if x == 'mobile' else 'web'
        )
        df_1 = df.groupby('productType').agg(
            {
                'id': 'size',
                'status': 'sum',
                'patientId': 'nunique',
                'doctorId': 'nunique'
            }
        )
        try:
            df_1.loc['web', 'id']
        except KeyError:
            df_1.loc['web'] = {'id': 0, 'status': 0, 'patientId': 0, 'doctorId': 0}
            pass

        df_summary = pd.DataFrame(
            {
                'date_id': execution_date,
                'untact_requests_web': df_1.loc['web', 'id'],
                'untact_requests_web_users': df_1.loc['web', 'patientId'],
                'untact_requests_app': df_1.loc['app', 'id'],
                'untact_requests_app_users': df_1.loc['app', 'patientId'],
                'untact_requests_completed': df_1['status'].sum(),
                'untact_doctors_active': df_1['doctorId'].sum(),
                'untact_doctors_completed': df[df.status==1]['doctorId'].nunique()
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
                                            Key=f'gold/daily_key_metrics/date_id={execution_date}/treatments_summary.csv',
                                            Body=csv_buffer.getvalue())
        
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                raise AirflowException(f"Unsuccessful S3 put_object response. Status - {status}")

    extract_bronze = extract()
    transform_to_daily = transform(extract_bronze)
    load_to_s3_gold = load(transform_to_daily)
transform_to_gold_daily_treatments()