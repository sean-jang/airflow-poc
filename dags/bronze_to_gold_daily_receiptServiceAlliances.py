import io
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance


@dag(
    start_date=datetime(2024,1,31),
)
def transform_to_gold_daily_receiptServiceAlliances():
    @task()
    def extract(**context):
        ti: TaskInstance = context["task_instance"]
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]
        s3_session = boto3.Session(
            region_name='ap-northeast-2')
        s3_client = s3_session.client('s3')

        response = s3_client.get_object(Bucket='goodoc-sync-rds-data',
                                        Key=f'bronze/goodoc-rds/receiptServiceAlliances/date_id={execution_date}/0.csv')
        
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
        df['installedAt'] = df['installedAt'].map(
            lambda x: datetime.strftime(
                datetime.strptime(x[:19], '%Y-%m-%d %H:%M:%S') + timedelta(hours=9), '%Y-%m-%d %H:%M:%S'
            )[:10] if x and str(x) != 'nan' else np.nan 
        )
        df['withdrawalRequestedAt'] = df['withdrawalRequestedAt'].map(
            lambda x: datetime.strftime(
                datetime.strptime(x[:19], '%Y-%m-%d %H:%M:%S') + timedelta(hours=9), '%Y-%m-%d %H:%M:%S'
            )[:10] if x and str(x) != 'nan' else np.nan 
        )
        df_summary = pd.DataFrame(
            {
                'date_id': execution_date, 
                'receipt_hospitals_install': df[df.status == 'S3']['hospitalId'].nunique(), 
                'receipt_hospitals_new': df[
                    (df.status == 'S3') & (df.installedAt == execution_date)
                ]['hospitalId'].nunique(), 
                'receipt_hospitals_bounced': df[
                    (df.status == 'W1') & (df.withdrawalRequestedAt == execution_date)
                ]['hospitalId'].nunique()
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
                                            Key=f'gold/daily_key_metrics/date_id={execution_date}/receiptServiceAlliances_summary.csv',
                                            Body=csv_buffer.getvalue())
        
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                raise AirflowException(f"Unsuccessful S3 put_object response. Status - {status}")
    
    extract_bronze = extract()
    transform_to_gold = transform(extract_bronze)
    load_gold_daily = load(transform_to_gold)
transform_to_gold_daily_receiptServiceAlliances()