import io

import boto3
import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def extract_dataframe_from_s3(conn_id: str,
                              key: str,
                              bucket: str) -> pd.DataFrame  :
    
    s3_client = S3Hook(conn_id)
    response = s3_client.get_key(
        key=key,
        bucket_name=bucket
    )
    df = pd.read_csv(response.get()["Body"])
    
    if df.empty:
        raise AirflowException(f"Unsuccessful S3 get_object {key} response. Empty data")
    else:
        return df
        
    
def load_dataframe_to_s3(conn_id: str,
                         df: pd.DataFrame,
                         key: str,
                         bucket: str) -> None:

    s3_client = S3Hook(conn_id)
    with io.BytesIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        s3_client.load_bytes(
            key=key,
            bucket_name=bucket,
            bytes_data=csv_buffer.getvalue(),
            replace=True
        )

        print(f"Successful S3 put_object {key} response.")