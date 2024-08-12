import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import Any

import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

from airflow import DAG
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {
    "start_date": datetime(2019, 4, 1),
    "end_date": datetime(2019, 4, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_s3_to_postgres",
    default_args=default_args,
    description=(
        "ETL process to extract CSV files from S3, transform data and load "
        "them into a PostgreSQL instance"
    ),
    schedule="0 3 * * *",
)


S3_BUCKET_NAME = "alg-data-public"
POSTGRES_DB = "algolia_wh"
POSTGRES_USER = "alglia_user"
POSTGRES_PASSWORD = "algolia_pwd"
POSTGRES_HOST = "warehouse"
POSTGRES_PORT = "5432"


def extract_csv_from_s3(ds: str, **context: dict[Any, Any]) -> None:
    """
    Extract CSV files from the public S3 bucket.
    """
    csv_file_name = f"{ds}.csv"
    logger.info(f"Extracting {csv_file_name} from S3 bucket {S3_BUCKET_NAME}")

    s3_client = boto3.client(
        "s3",
        config=Config(signature_version=UNSIGNED),
    )

    try:
        csv_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=csv_file_name)
        csv_data = csv_obj["Body"].read().decode("utf-8")
        context["ti"].xcom_push(key="csv_data", value=csv_data)
        logger.info(f"Successfully extracted {csv_file_name} from S3")
    except s3_client.exceptions.NoSuchKey:
        logger.warning(
            f"The file {csv_file_name} does not exist in S3 bucket "
            f"{S3_BUCKET_NAME}."
        )
        raise AirflowSkipException(
            f"The file {csv_file_name} does not exist in S3 bucket "
            f"{S3_BUCKET_NAME}."
        )
    except ClientError as e:
        logger.error(f"Client error while accessing S3: {str(e)}")
        raise AirflowException(f"Client error: {str(e)}")


def transform_data(**context: dict[Any, Any]) -> None:
    """
    Transform the CSV data by removing application_id null rows and create
    the column has_specific_prefix.
    """
    logger.info("Transforming data")

    csv_data = context["ti"].xcom_pull(key="csv_data", task_ids="extract_csv_from_s3")
    df = pd.read_csv(StringIO(csv_data))

    df = df[df["application_id"].notna()]
    df["has_specific_prefix"] = df["index_prefix"].apply(lambda x: x != "shopify_")

    context["ti"].xcom_push(key="transformed_data", value=df.to_csv(index=False))
    logger.info("Data transformation complete")


def load_data_to_postgres(**context: dict[Any, Any]) -> None:
    """
    Load valid rows to a PostgreSQL instance only if they have not already
    been inserted.
    """
    logger.info("Loading data to PostgreSQL")
    transformed_data = context["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")
    df = pd.read_csv(StringIO(transformed_data))

    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    query = "SELECT * FROM shopify_data"

    try:
        df_table = pd.read_sql(query, engine)
        combined_df = pd.concat([df_table, df])
        duplicates = combined_df[combined_df.duplicated(keep=False)]
        if duplicates.empty:
            df.to_sql("shopify_data", engine, if_exists="append", index=False)
            logger.info("No duplicates found, rows inserted successfully")
        else:
            logger.info("Duplicates found, no rows inserted")
    except Exception:
        logger.info("First insert in shopify_data table")
        df.to_sql("shopify_data", engine, if_exists="append", index=False)


extract_task = PythonOperator(
    task_id="extract_csv_from_s3",
    python_callable=extract_csv_from_s3,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_data_to_postgres,
    dag=dag,
)

extract_task >> transform_task >> load_task
