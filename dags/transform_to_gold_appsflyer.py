import io
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.exceptions import AirflowException


@dag(start_date=datetime(2024,1,31))
def transform_to_gold_appsflyer():
    @task()
    def extract(**context):
        ti: TaskInstance = context['task_instance']
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]

        s3_session = boto3.Session(region_name='ap-northeast-2')
        s3_client = s3_session.client('s3')
        keys = s3_client.list_objects(Bucket='goodoc-sync-rds-data',
                                      Prefix=f'bronze/appsflyer/date_id={execution_date}')

        df_merge = list()
        keys = [i['Key'] for i in keys['Contents']]
        for s3_key in keys:
            app_id, category = s3_key.split('/')[-1].split('_')
            category = category.split('.')[0]
            response = s3_client.get_object(Bucket='goodoc-sync-rds-data',
                                            Key=s3_key)
            
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 get_object {s3_key} response. Status - {status}")
                df = pd.read_csv(response.get("Body"))
                df['App ID'] = app_id
                if category == 'install':
                    df['Conversion Type'] = 'install'
                    df['Conversions'] = 0
                else:
                    df['Installs'] = 0
                
                df = df[df.Country=='KR'][
                    ['Date', 'App ID', 'Media Source (pid)', 
                    'Installs', 'Conversion Type', 'Conversions']
                    ]
                df_merge.append(df)
            
            else:
                raise AirflowException(f"Unsuccessful S3 get_object {s3_key} response. Status - {status}")

        df_merge = pd.concat(df_merge)

        return df_merge
    
    @task()
    def transform(df_merge: pd.DataFrame, **context):
        ti: TaskInstance = context['task_instance']
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]
        df_merge['Media Source (pid)'].fillna('nonorganic', inplace=True)
        df_agg = df_merge.groupby(
            ['Date', 'App ID', 'Conversion Type', 'Media Source (pid)']
        )[['Installs', 'Conversions']].sum().reset_index()

        df_t2m = df_agg[df_agg['Media Source (pid)']=='tablet'].groupby(
            ['Conversion Type']
        )[['Installs', 'Conversions']].sum()

        df_installs = df_agg[df_agg['Conversion Type']=='install']
        df_installs['Media Source (pid)'] = df_installs['Media Source (pid)'].map(
            lambda x: 'nonorganic' if x != 'Organic' else 'organic'
        )
        df_installs['App ID'] = df_installs['App ID'].map(
            lambda x: 'ios' if x[:2] == 'id' else 'aos'
        )
        df_installs = df_installs.groupby(
            ['App ID', 'Media Source (pid)']
        )['Installs'].sum().reset_index()

        df_summary = pd.DataFrame(
            {
                'date_id': execution_date,
                'newInstall_tabletToMobile': df_t2m.loc['install', 'Installs'],
                'reInstall_tabletToMobile': df_t2m.loc['re-attribution', 'Conversions'],
                'reengagement_tabletToMobile': df_t2m.loc['re-engagement', 'Conversions'],
                'newInstall_organic_aos': df_installs[
                    (df_installs['App ID']=='aos') & (df_installs['Media Source (pid)']=='organic')
                ]['Installs'].values[0],
                'newInstall_nonorganic_aos': df_installs[
                    (df_installs['App ID']=='aos') & (df_installs['Media Source (pid)']=='nonorganic')
                ]['Installs'].values[0],
                'newInstall_organic_ios': df_installs[
                    (df_installs['App ID']=='ios') & (df_installs['Media Source (pid)']=='organic')
                ]['Installs'].values[0],
                'newInstall_nonorganic_ios': df_installs[
                    (df_installs['App ID']=='ios') & (df_installs['Media Source (pid)']=='nonorganic')
                ]['Installs'].values[0],
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
                                            Key=f'gold/daily_key_metrics/date_id={execution_date}/appsflyer_summary.csv',
                                            Body=csv_buffer.getvalue())
        
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                raise AirflowException(f"Unsuccessful S3 put_object response. Status - {status}")
            
    extract_bronze = extract()
    transform_to_daily = transform(extract_bronze)
    load_to_s3_gold = load(transform_to_daily)
transform_to_gold_appsflyer()