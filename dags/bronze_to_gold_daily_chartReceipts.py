
import io
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow import DAG
from airflow.macros import ds_add
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException


def extract_load(ds, **context):
    s3_session = boto3.Session(region_name='ap-northeast-2')
    s3_client = s3_session.client('s3')
    response = s3_client.get_object(Bucket='goodoc-sync-rds-data',
                                    Key=f'bronze/goodoc-rds/chartReceipts/date_id={ds}/0.csv')
    
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_csv(response.get("Body"))

        df_summary = pd.DataFrame(
            {'date_id': ds, 'receipt_requests_dummy': len(df)}, index=[0]
        )
        
        with io.StringIO() as csv_buffer:
            df_summary.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(Bucket='goodoc-sync-rds-data',
                                            Key=f'gold/daily_key_metrics/date_id={ds}/chartReceipts_summary.csv',
                                            Body=csv_buffer.getvalue())
        
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                raise AirflowException(f"Unsuccessful S3 put_object response. Status - {status}")
    
    else:
        raise AirflowException(f"Unsuccessful S3 get_object response. Status - {status}")


with DAG (
    dag_id='transform_to_gold_daily_chartReceipts',
    start_date=datetime(2024,1,31)
) as dag:    
    extract_load = PythonOperator(
        task_id='bronze_to_gold',
        python_callable=extract_load,
        default_args={
            'retries': 3,
            'retry_delay': timedelta(minutes=3),
            'max_active_runs': 1
        }
    )

extract_load
