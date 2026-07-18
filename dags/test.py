"""
from airflow.providers.standard.operators.python import PythonOperator

save = {}

def extr(bd_name: str, file_name: str):
    cursor = sql.create_engine("postgresql://olist_user:password@172.19.0.9:5432/olist")
    with cursor.connect() as conn:
        conn.execute(sql.text("CREATE SCHEMA IF NOT EXISTS raw"))
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
"""

"""
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
"""
            

"""
for i, j in data.items():
    save[i] = PythonOperator(task_id="load_" + i,
                        python_callable=extr,
                        op_args=[i, j])
"""


#print(list(data.values()))


import pandas as pd
df = pd.DataFrame([["A", 1], ["B", 2], ["C", 3], ["D", 4]], 
                  columns=["col_A", "col_B"])
df["col_C"] = df.col_B.cumsum()

print(df)
