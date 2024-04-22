from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'read_api_and_store',
    default_args=default_args,
    description='A DAG to read data from API in Airflow and store it in Snowflake',
    schedule_interval=timedelta(days=1),
)

def fetch_data_from_api(**kwargs):
    api_endpoint = "https://www.arbeitnow.com/api/job-board-api"
    response = requests.get(api_endpoint)
    if response.status_code == 200:
        return response.json()["data"]
    else:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

def store_data_in_snowflake(data, **kwargs):
    snowflake_conn_id = "snowflake-conn"
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                for item in data:
                    tags_str = json.dumps(item.get("tags", []))
                    job_types_str = json.dumps(item.get("job_types", []))
                    cursor.execute("""
                    INSERT INTO APIJOBS (slug, company_name, title, description, remote, url, tags, job_types, location, created_at)
                    SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s, %s
                """, (
                    item["slug"],
                    item["company_name"],
                    item["title"],
                    item["description"],
                    item["remote"],  
                    item["url"],
                    tags_str,
                    job_types_str,
                    item["location"],
                    datetime.fromtimestamp(item["created_at"])  
                ))
                kwargs['ti'].log.info("Data loaded successfully into Snowflake.")
            except Exception as e:
                kwargs['ti'].log.error(f"Error loading data into Snowflake: {e}")
            conn.commit()

fetch_data_task = PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=fetch_data_from_api,
    provide_context=True,
    dag=dag,
)

store_data_task = PythonOperator(
    task_id='store_data_in_snowflake',
    python_callable=store_data_in_snowflake,
    op_args=[],
    op_kwargs={'data': fetch_data_task.output},
    provide_context=True,
    dag=dag,
)

fetch_data_task >> store_data_task
