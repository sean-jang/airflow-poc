import io
import logging

import boto3
import pandas as pd
from airflow.exceptions import AirflowException
from airflow.macros import ds_add
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class CustomS3Hook(S3Hook):
    DEFAULT_REGION = 'ap-northeast-2'

    def __init__(self,conn_id='aws_access'):
        super().__init__(conn_id)
    
    def extract_from_s3(self,
                        execution_date: str,
                        key: str,
                        bucket='amplitude-export-test-116807',
                        **kwargs) -> pd.DataFrame  :
        
        # next_ds = ds_add(execution_date, 1)
        next_ds = '2024-02-20'
        response = self.get_key(
            key=f'airflow-test/bronze/goodoc-rds/{key}/date_id={next_ds}/0.csv',
            bucket_name=bucket
        )
        df = pd.read_csv(response.get()["Body"])
        
        if df.empty:
            raise AirflowException(f"Unsuccessful S3 get_object {key} response")
        else:
            return df
        
    
    def load_dataframe_to_s3(self,
                             df: pd.DataFrame,
                             execution_date: str,
                             key: str,
                             bucket='amplitude-export-test-116807',
                             **kwargs):
        
        # next_ds = ds_add(execution_date, 1)
        next_ds = '2024-02-20'

        with io.BytesIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            self.load_bytes(
                key=f'gold/daily_key_metrics/date_id={next_ds}/{key}_summary.csv',
                bucket_name=bucket,
                bytes_data=csv_buffer.getvalue(),
                replace=True
            )
    
    def get_objects_list(self, 
                         execution_date, 
                         prefix, 
                         bucket='amplitude-export-test-116807', 
                         **kwargs) -> list :
        
        # next_ds = ds_add(execution_date, 1)
        next_ds = '2024-02-20'
        keys = self.list_keys(
            bucket_name=bucket,
            prefix=prefix
        )
        
        return keys



## Initial
class S3Hook(BaseHook):
    DEFAULT_CONN = 's3'
    DEFAULT_REGION = 'ap-northeast-2'

    def __init__(self,conn_id='aws_access'):
        super().__init__()
        self._conn_id = conn_id
    
    def get_conn(self):
        config = self.get_connection(self._conn_id)
        s3_session = boto3.Session(region_name=self.DEFAULT_REGION)
        s3_client = s3_session.client(self.DEFAULT_CONN)

        return s3_client
    
    def extract_from_s3(self,
                        execution_date: str,
                        key: str,
                        bucket='amplitude-export-test-116807',
                        **kwargs) -> pd.DataFrame  :
        
        next_ds = ds_add(execution_date, 1)
        s3_client = self.get_conn()
        response = s3_client.get_object(Bucket=bucket,
                                        Key=f'airflow-test/bronze/goodoc-rds/{key}/date_id={next_ds}/0.csv')        
        
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object {key} response. Status - {status}")
            df = pd.read_csv(response.get("Body"))
            
            return df
        
        else:
            raise AirflowException(f"Unsuccessful S3 get_object {key} response. Status - {status}")
    
    def load_dataframe_to_s3(self,
                             df: pd.DataFrame,
                             execution_date: str,
                             key: str,
                             bucket='amplitude-export-test-116807',
                             **kwargs):
        
        next_ds = ds_add(execution_date, 1)
        s3_client = self.get_conn()

        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(Bucket=bucket,
                                            Key=f'gold/daily_key_metrics/date_id={next_ds}/{key}_summary.csv',
                                            Body=csv_buffer.getvalue())
            
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object {key} response. Status - {status}")
            else:
                raise AirflowException(f"Unsuccessful S3 put_object {key} response. Status - {status}")
    
    def get_objects_list(self, 
                     execution_date, 
                     prefix, 
                     bucket='amplitude-export-test-116807', 
                     **kwargs) -> list :
        
        next_ds = ds_add(execution_date, 1)
        s3_client = self.get_conn()
        keys = s3_client.get_object(Bucket=bucket, Prefix=prefix)
        
        return keys

