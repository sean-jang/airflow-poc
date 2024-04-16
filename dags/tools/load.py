import io
from abc import ABC, abstractmethod

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class Loader(ABC):
    @abstractmethod
    def load(self, df_task_id: str, key: str, bucket: str, **context) -> None:
        ...


class S3Loader(Loader):
    def __init__(self, conn_id: str):
        self.client = S3Hook(conn_id)

    def load(self, df_task_id: str, key: str, bucket: str, **context):
        df = context["ti"].xcom_pull(task_ids=df_task_id)
        with io.BytesIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            self.s3_client.load_bytes(
                key=key,
                bucket_name=bucket,
                bytes_data=csv_buffer.getvalue(),
                replace=True
            )

            print(f"Successful S3 put_object {key} response.")