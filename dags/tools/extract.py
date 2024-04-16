from abc import ABC, abstractmethod

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hook.s3 import S3Hook


class Extractor(ABC):
    @abstractmethod
    def extract(self, bucket: str, key: str) -> pd.DataFrame : 
        ...


class S3Extractor(Extractor):
    def __init__(self, conn_id: str):
        self.client = S3Hook(conn_id)
    
    def extract(self, bucket: str, key: str) -> pd.DataFrame:
        response = self.client.get_key(
            bucket=bucket,
            key=key
        )
        df = pd.read_csv(response.get()["Body"])

        if df.empty:
            raise AirflowException(f"Unsuccessful S3 get_object {key} response. Empty data")
        else:
            return df
    
    def extract_joined_objects(self, 
                               bucket: str, 
                               prefix: str, 
                               join: str, 
                               on: str) -> pd.DataFrame:
        objects_list = self.client.list_keys(
            bucket_name=bucket,
            prefix=prefix
        )

        df_merge = pd.DataFrame()
        for key in objects_list:
            df_key = self.extract(bucket, key)
            if df_merge.empty:
                df_merge = df_key.copy()
            else:
                df_merge = pd.merge(df_merge, df_key, how=join, on=on)

        return df_merge

