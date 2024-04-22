import re
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils import timezone
from snowflake.connector import connect
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def suggest_courses_for_jobs(**kwargs):
    snowflake_conn_id = 'snowflake-conn'
    snowflake_hook = BaseHook.get_hook(snowflake_conn_id)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Compile the regular expression pattern to match parentheses and surrounding whitespace
        pattern = re.compile(r'\s*\([^()]*m\/w\/d[^()]*\)\s*|\s*\([^()]+\)\s*|\s*m\/w\/d\s*')

        cursor.execute("""
            INSERT INTO JOBSCOURSES (job_title, course_title)
            SELECT DISTINCT 
                REGEXP_REPLACE(j.TITLE, %s, ''),
                c."Course Title"
            FROM APIJOBS j
            JOIN COURSES c
            ON (LOWER(j.TITLE) IN (LOWER(c."Course Title"), LOWER(j.DESCRIPTION), LOWER(c."What you will learn"), LOWER(c."Skill gain"))
                OR LOWER(j.DESCRIPTION) IN (LOWER(c."Course Title"), LOWER(j.TITLE), LOWER(c."What you will learn"), LOWER(c."Skill gain"))
                OR LOWER(c."Course Title") IN (LOWER(j.TITLE), LOWER(j.DESCRIPTION), LOWER(c."What you will learn"), LOWER(c."Skill gain"))
                OR LOWER(c."What you will learn") IN (LOWER(j.TITLE), LOWER(j.DESCRIPTION), LOWER(c."Course Title"), LOWER(c."Skill gain"))
                OR LOWER(c."Skill gain") IN (LOWER(j.TITLE), LOWER(j.DESCRIPTION), LOWER(c."Course Title"), LOWER(c."What you will learn")))
        """, (pattern.pattern,))

        conn.commit()
    except Exception as e:
        print("Error:", e)
    finally:
        cursor.close()
        conn.close()

# Define your DAG
with DAG('suggest_courses_for_jobs_dag', default_args=default_args, schedule_interval=None, start_date=timezone.datetime(2024, 3, 31)) as dag:
    
    suggest_courses_task = PythonOperator(
        task_id='suggest_courses_for_jobs',
        python_callable=suggest_courses_for_jobs,
        provide_context=True,
    )

suggest_courses_task
