import io
from abc import ABC, abstractmethod

import pandas as pd
from airflow.providers.amazon.aws.hook.s3 import S3Hook


class Loader(ABC):
    @abstractmethod
    def load(self, df: pd.DataFrame) -> None:
        ...


class S3Loader(Loader):
    def __init__(self, conn_id: str):
        self.client = S3Hook(conn_id)

    def load(self, df: pd.DataFrame, key: str, bucket: str):
        with io.BytesIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            self.s3_client.load_bytes(
                key=key,
                bucket_name=bucket,
                bytes_data=csv_buffer.getvalue(),
                replace=True
            )

            print(f"Successful S3 put_object {key} response.")