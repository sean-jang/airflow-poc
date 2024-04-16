import io
from datetime import datetime

import boto3
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.macros import ds_add
from airflow.models.variable import Variable
from airflow.models.taskinstance import TaskInstance
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


@dag(start_date=datetime(2024,1,31), schedule='50 23 * * *', catchup=False)
def etl_GA_goodocWeb():
    @task()
    def extract(**context):
        ti: TaskInstance = context["task_instance"]
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]
        next_ds = ds_add(execution_date, 1)

        scope = "https://www.googleapis.com/auth/analytics.readonly"
        credentials = Credentials.from_service_account_file(
            Variable.get('ga_key_file_location'), scopes=[scope]
        )
        ga4_service = build("analyticsdata", "v1beta", credentials=credentials)
        response = (
            ga4_service.properties()
            .runReport(
                property=f"properties/{Variable.get('ga_goodoc_web_view_id')}",
                body={
                    "dimensions": [{"name": "deviceCategory"}],
                    "metrics":[{"name": "active1DayUsers"}],
                    "dateRanges":[{"startDate": next_ds,"endDate": next_ds}],
                    "metricAggregations":["TOTAL"]
                }
            )
            .execute()
        )

        return response
    
    @task()
    def transform(response, **context):
        ti: TaskInstance = context["task_instance"]
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]
        next_ds = ds_add(execution_date, 1)
        
        result_dict = dict()
        for row in response['rows']:
            dimension = row['dimensionValues'][0]['value']
            metric = row['metricValues'][0]['value']
            result_dict[dimension] = metric
        
        df_summary = pd.DataFrame(
            {
                'date_id': next_ds,
                'dau_pc': result_dict['desktop'],
                'dau_mobile': result_dict['mobile']
            }, index=[0]
        )
        
        return df_summary
    
    @task()
    def load(df_summary: pd.DataFrame, **context):
        ti: TaskInstance = context["task_instance"]
        execution_date = datetime.strftime(ti.execution_date, '%Y-%m-%d %H:%M:%S')[:10]
        next_ds = ds_add(execution_date, 1)
        s3_session = boto3.Session(region_name='ap-northeast-2')
        s3_client = s3_session.client('s3')

        with io.StringIO() as csv_buffer:
            df_summary.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(Bucket='goodoc-sync-rds-data',
                                            Key=f'gold/daily_key_metrics/date_id={next_ds}/goodocWeb_summary.csv',
                                            Body=csv_buffer.getvalue())
        
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                raise AirflowException(f"Unsuccessful S3 put_object response. Status - {status}")
    
    extract_google_analytics_api = extract()
    transform_to_daily = transform(extract_google_analytics_api)
    load_to_s3_gold = load(transform_to_daily)

etl_GA_goodocWeb()