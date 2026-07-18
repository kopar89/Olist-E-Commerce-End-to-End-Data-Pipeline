from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import sqlalchemy as sql
import boto3
import io

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123'
    )

BUCKET = 'olist-data'

def upload_to_minio(table_name: str, file_name: str):
    df = pd.read_csv(file_name)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    client = get_minio_client()
    client.put_object(
        Bucket=BUCKET,
        Key=f'raw/{table_name}.parquet',
        Body=buffer,
        ContentLength=buffer.getbuffer().nbytes
    )
    print(f'Uploaded {table_name} to MinIO: raw/{table_name}.parquet')
    return len(df)  

def load_to_postgres(table_name: str):
    client = get_minio_client()
    response = client.get_object(
        Bucket=BUCKET,
        Key=f'raw/{table_name}.parquet'
    )
    
    buffer = io.BytesIO(response['Body'].read())
    df = pd.read_parquet(buffer)
    
    engine = sql.create_engine(
                              "postgresql://olist_user:password@postgres-olist:5432/olist"
                              )
    
    with engine.connect() as conn:
        conn.execute(sql.text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(sql.text(f"DROP TABLE IF EXISTS raw.{table_name} CASCADE"))
        conn.commit()
    
    for i, chunk in enumerate(
        (df[i:i+10000] for i in range(0, len(df), 10000))
    ):
        chunk.to_sql(
            name=table_name,
            con=engine,
            schema='raw',
            if_exists='replace' if i == 0 else 'append',
            index=False
        )
    
    engine.dispose()
    print(f'Loaded {table_name} to PostgreSQL from MinIO')


args = {
    'owner': 'geak',
    'start_date': datetime(2026, 7, 14),
    'email': ['write your email address'],
    'email_on_failure': True,
}

data = {
    'customers': '/opt/airflow/dags/olist_customers_dataset.csv',
    'geolocation': '/opt/airflow/dags/olist_geolocation_dataset.csv',
    'order_items': '/opt/airflow/dags/olist_order_items_dataset.csv',
    'order_payments': '/opt/airflow/dags/olist_order_payments_dataset.csv',
    'order_reviews': '/opt/airflow/dags/olist_order_reviews_dataset.csv',
    'orders': '/opt/airflow/dags/olist_orders_dataset.csv',
    'products': '/opt/airflow/dags/olist_products_dataset.csv',
    'sellers': '/opt/airflow/dags/olist_sellers_dataset.csv',
    'category_translations': '/opt/airflow/dags/product_category_name_translation.csv',
}

with DAG(
    dag_id='ET4_minio',
    description='CSV -> Parquet -> MinIO -> PostgreSQL -> dbt',
    default_args=args,
    schedule='@daily',
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    upload_tasks = {}
    load_tasks = {}

    for table_name, file_path in data.items():
        upload_tasks[table_name] = PythonOperator(
            task_id=f'upload_{table_name}_to_minio',
            python_callable=upload_to_minio,
            op_args=[table_name, file_path]
        )

        load_tasks[table_name] = PythonOperator(
            task_id=f'load_{table_name}_to_postgres',
            python_callable=load_to_postgres,
            op_args=[table_name]
        )

        upload_tasks[table_name] >> load_tasks[table_name]

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="cd /opt/airflow/olist_dbt && /home/airflow/.local/bin/dbt run --profiles-dir /opt/airflow/olist_dbt"
                          )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="cd /opt/airflow/olist_dbt && /home/airflow/.local/bin/dbt test --profiles-dir /opt/airflow/olist_dbt"
                           )

    list(load_tasks.values()) >> dbt_run >> dbt_test