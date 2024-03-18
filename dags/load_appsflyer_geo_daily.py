import requests
from datetime import datetime, timedelta
from io import StringIO

import boto3
import pandas as pd
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException


def get_appsflyer_geo_daily(app_id, reattr, next_ds, **context):
    """
    Get appsflyer geo daily aggregation data with appsflyer pullAPI.
    """
    retarget_yn = True if reattr == 'retarget' else False
    url = f"https://hq1.appsflyer.com/api/agg-data/export/app/{app_id}/geo_by_date_report/v5?from={next_ds}&to={next_ds}&reattr={retarget_yn}&timezone=Asia%2FSeoul"
    headers = {
        "accept": "text/csv",
        "authorization": "Bearer eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.AcS42JGhmSDcvU-5fBUwpCTIB5qqic4OfrRiNpnw-qOs-NUbp34Z2w.V0Y4BbYj67F4u0SE.Kn3UdjGywSKS7Yxv2YAik5YjACsWaYb4j-aAJJVwIAI-9_R6S5CzibnuPIOZvL5uw8Ox8pZFVh9-gon-vmso0sIlEFX4Jgz9fTsMWBf1pKPic5XCOjBXyiPJ2F7wqeioLSHSq1LZlApZE5OneZU77BCjI55bx-WrgkCsVKpgfsW88wAIDvsoVA84IVXoYmU7SFvBwodCOJLt388r4n0F5X3YkEl0wHuSJ3vfQrxhVz7QKWlGs9yjZf-hP0jxeKl4fNGPN1yvWcJT6p3P3t3RSzcZikZlsHoKNiGKbpwVPO447cv04otjphrDIQzUmjMIcpYpugiaO4aJ1RvIKWxgWEmVUsYvSp-Lrp-BUUOuXHyerjMLtnb9PhEZknSdRP9pnZsZSCwUC1tLCxzQOWpksDWPHi9qoH_O4h-vRSJcfT79P_kZGiod07J065Uq5kQgS-EAiLi45SH5wAExNv1c1G4bThZ8Cq5PqxqtqNihVe9nnfpoPpDkgkkj65jDjImeDVTv3PNMhXtwZ8aHkcZ3licv.ZOUeFWyACVakXiWiUfa0SA"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise AirflowException("Appsflyer geo daily api failed")
    else:
        df_agg = pd.read_csv(StringIO(response.text), sep=',')
        context['ti'].xcom_push(key=f'{next_ds}_{app_id}_{reattr}', value=df_agg)

def load_appsflyer_geo_daily_to_s3(app_id, reattr, next_ds, **context):
    """
    Load appsflyer geo daily aggregation data to S3 bucket.
    """
    df = context['ti'].xcom_pull(key=f'{next_ds}_{app_id}_{reattr}')
    s3_session = boto3.Session(region_name='ap-northeast-2')
    s3_client = s3_session.client('s3')

    with StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        s3_response = s3_client.put_object(
            Bucket='goodoc-sync-rds-data',
            Key=f'bronze/appsflyer/date_id={next_ds}/{app_id}_{reattr}.csv',
            Body = csv_buffer.getvalue()
        )


with DAG (
    dag_id='load_appsflyer_geo_daily',
    start_date=datetime(2024, 1, 31),
    schedule='30 15 * * *',
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=3),
        'max_active_runs': 5
    }
) as dag:

    tasks = dict()
    app_ids = Variable.get('appsflyer_app_id', deserialize_json=True)
    for os, id in app_ids.items():
        for retarget_yn in [True, False]:
            reattr = 'retarget' if retarget_yn else 'install'
            extract = PythonOperator(
                task_id=f'extract_{os}_{reattr}',
                python_callable=get_appsflyer_geo_daily,
                op_kwargs={
                    'app_id': id,
                    'reattr': reattr
                }
            )
            tasks[f'extract_{os}_{reattr}'] = extract

            load = PythonOperator(
                task_id=f'load_{os}_{reattr}',
                python_callable=load_appsflyer_geo_daily_to_s3,
                op_kwargs={
                    'app_id': id,
                    'reattr': reattr
                }
            )
            tasks[f'load_{os}_{reattr}'] = load
    
    trigger_bronze_to_gold=TriggerDagRunOperator(
        task_id='trigger_bronze_to_gold',
        trigger_dag_id='transform_to_gold_daily_appsflyer'
    )


tasks['extract_ios_retarget'] >> tasks['load_ios_retarget'] \
>> tasks['extract_ios_install'] >> tasks['load_ios_install'] \
>> tasks['extract_aos_retarget'] >> tasks['load_aos_retarget'] \
>> tasks['extract_aos_install'] >> tasks['load_aos_install'] \
>> trigger_bronze_to_gold