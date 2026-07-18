from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import sqlalchemy as sql


def extract(bd_name: str, file_name: str):
    cursor = sql.create_engine("postgresql://olist_user:password@postgres-olist:5432/olist")
    with cursor.connect() as conn:
        conn.execute(sql.text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(sql.text(f"DROP TABLE IF EXISTS raw.{bd_name} CASCADE"))
        conn.commit()
    
    for i, chunk in enumerate(pd.read_csv(file_name, chunksize=10000)):
        chunk.to_sql(
            name=bd_name,
            con=cursor,
            schema="raw",
            if_exists="replace" if i == 0 else "append",  
            index=False
        )
    cursor.dispose()


args = {
    "owner": "geak",
    "start_date": datetime(2026, 7, 14)
}


with DAG(dag_id="ET3", description="EXTRACT_DATA", default_args=args, schedule="@hourly", catchup=False, is_paused_upon_creation=False) as Dag_1:


    save = {}
    data = {"customers" : "/opt/airflow/dags/olist_customers_dataset.csv",
            "geolocation" : "/opt/airflow/dags/olist_geolocation_dataset.csv",
            "order_items" : "/opt/airflow/dags/olist_order_items_dataset.csv",
            "order_payments" : "/opt/airflow/dags/olist_order_payments_dataset.csv",
            "order_reviews" : "/opt/airflow/dags/olist_order_reviews_dataset.csv",
            "orders" : "/opt/airflow/dags/olist_orders_dataset.csv",
            "products" : "/opt/airflow/dags/olist_products_dataset.csv",
            "sellers" : "/opt/airflow/dags/olist_sellers_dataset.csv",
            "category_translations" : "/opt/airflow/dags/product_category_name_translation.csv", 
            }


    for i, j in data.items():
        save[i] = PythonOperator(task_id="load_" + i,
                                 python_callable=extract,
                                 op_args=[i, j])

    dbt_runer = BashOperator(task_id="dbt_run",
                             bash_command="cd /opt/airflow/olist_dbt && /home/airflow/.local/bin/dbt run --profiles-dir /opt/airflow/olist_dbt"
                            )
    
    dbt_test = BashOperator(task_id="dbt_test",
                            bash_command="cd /opt/airflow/olist_dbt && /home/airflow/.local/bin/dbt test --profiles-dir /opt/airflow/olist_dbt"
                           )

    [save["customers"], save["geolocation"], save["order_items"], save["order_payments"], save["order_reviews"], 
    save["orders"], save["products"], save["sellers"], save["category_translations"]] >> dbt_runer >> dbt_test