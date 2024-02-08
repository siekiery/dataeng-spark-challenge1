from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator

"""
More elegant approach to running spark application using airflow.
This version uses SparkSubmitOperator to run spark script and handle connection to spark cluster.
It requires setup of spark_local connection in airflow based on spark.conf

Data is loaded to AWS S3 bucket.
"""

default_args = {
    'owner': 'pc',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(days=1),
    'depends_on_past': False,
    'email_on_failure': False,
    'catchup': False
}

dag = DAG('interview_challenge_dag',
          default_args=default_args,
          description="""ETL steps for enriching challenge dataset.""",
          schedule_interval='0 0 1 * *'  # Once per month
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

task_spark_etl = SparkSubmitOperator(
    task_id="task_spark_etl",
    application="ETL_S3.py",
    conn_id="spark_local",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator \
>> task_spark_etl \
>> end_operator
