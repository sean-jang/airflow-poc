from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from tools.s3_hook import extract_dataframe_from_s3, load_dataframe_to_s3


def etl_goodoc_rds(next_ds, **context):

    df_s = extract_dataframe_from_s3(
        conn_id=context["conn_id"],
        key=context["get_key"],
        bucket=context["bucket"]
    )
    load_dataframe_to_s3(
        conn_id=context["conn_id"],
        df=df_s,
        key=context["put_key"],
        bucket=context["bucket"]
    )


with DAG(
    dag_id='etl_test',
    start_date=datetime(2024, 3, 21),
    schedule='@once',
    catchup=False
) as dag:
    
    etl = PythonOperator(
        task_id='etl',
        python_callable=etl_goodoc_rds,
        op_kwargs={
            "conn_id": "aws_access",
            "bucket": "amplitude-export-test-116807",
            "get_key": "airflow-test/bronze/goodoc-rds/treatments/date_id=2024-02-20/0.csv",
            "put_key": "airflow-test/gold/daily_key_metrics/date_id=2024-02-20/treatments_summary.csv"
        }
    )

    etl
