import json
import io
from datetime import datetime, timedelta

import boto3
import gzip
import pandas as pd
from airflow import DAG
from airflow.macros import ds_add
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


def get_app_dau(ds, **context):
    s3_session = boto3.Session(region_name='ap-northeast-2')
    s3_client = s3_session.client('s3')
    
    next_ds = ds_add(ds, 1)
    aos = list()
    ios = list()
    for hour in range(0, 24):
        if hour >= 15:
            s3_key = f'bronze/goodoc-production-1-amplitude/346433/346433_{ds}_{hour}#0.json.gz'
        else:
            s3_key = f'bronze/goodoc-production-1-amplitude/346433/346433_{next_ds}_{hour}#0.json.gz'
        
        print(s3_key)
        response = s3_client.get_object(
            Bucket='goodoc-sync-rds-data',
            Key=s3_key
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            body = response.get("Body").read()
            with gzip.open(io.BytesIO(body), 'rb') as f:
                file_content = f.readlines()
                for line in file_content:
                    json_data = json.loads(line)
                    os_name = json_data['platform']
                    
                    if os_name == 'iOS':
                        ios.append(json_data['device_id'])
                    elif os_name == 'Android':
                        aos.append(json_data['device_id'])
        else:
            raise AirflowException (f"Unsuccessful S3 get_object response. Status - {status}")
    
    df_summary = pd.DataFrame(
        {
            'date_id': next_ds,
            'dau_aos': len(set(aos)),
            'dau_ios': len(set(ios))
        }, index=[0]
    )

    with io.StringIO() as csv_buffer:
        df_summary.to_csv(csv_buffer, index=False)
        response = s3_client.put_object(Bucket='goodoc-sync-rds-data',
                                        Key=f'gold/daily_key_metrics/date_id={next_ds}/appDAU_summary.csv',
                                        Body=csv_buffer.getvalue())
    
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            raise AirflowException(f"Unsuccessful S3 put_object response. Status - {status}")


with DAG(
    dag_id='transform_to_gold_daily_appDAU',
    start_date=datetime(2024, 1, 31),
    schedule='5 18 * * *',
    catchup=False
) as dag:
    elt = PythonOperator(
        task_id='elt',
        python_callable=get_app_dau
    )
elt