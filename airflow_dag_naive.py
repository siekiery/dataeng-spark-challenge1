from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, regexp_replace, trim, to_date, when, coalesce

"""
This is a naive* approach to running spark scripts from airflow. The objective is to demonstrate airflow usage while 
dividing each ETL step to different task.

All airflow_*() callables wrap around ETL functions defined in ETL.py

Data is loaded to local directory.

*naive - PythonOperators are used; SparkSession object and Dataframe is passed explicitly between tasks. 
airflow_dag_sparky.py demonstrates improved approach in terms of spark handling.
"""

# Initialize variables
input_filepath = 'data.csv'
abbr_filepath = 'state_abbreviations.csv'
output_name = 'enriched'


def airflow_create_spark_session():
    from ETL import create_spark_session
    spark = create_spark_session()
    logging.info("SparkSession created.")

    return spark


def airflow_read_spark_df_from_csv(**context):
    from ETL import read_spark_df_from_csv
    spark = context['task_instance'].xcom_pull(task_ids='task_create_spark_session')
    filepath = context['params']['filepath']

    logging.info("Reading input data")
    df = read_spark_df_from_csv(spark, filepath)
    logging.info("Data successfuly read to DataFrame.")

    return df


def airflow_process_str_cleaning(**context):
    from ETL import process_str_cleaning
    spark = context['task_instance'].xcom_pull(task_ids='task_create_spark_session')
    df = context['task_instance'].xcom_pull(task_ids='task_read_spark_df_from_csv')

    df = process_str_cleaning(df)
    logging.info("String cleaning process completed.")

    return df


def airflow_process_code_swap(**context):
    from ETL import process_code_swap
    spark = context['task_instance'].xcom_pull(task_ids='task_create_spark_session')
    df = context['task_instance'].xcom_pull(task_ids='task_process_str_cleaning')
    abbr_filepath = context['params']['abbr_filepath']

    df = process_code_swap(df, abbr_filepath)
    logging.info("State code swap process completed.")

    return df


def airflow_process_date_offset(**context):
    from ETL import process_date_offset
    spark = context['task_instance'].xcom_pull(task_ids='task_create_spark_session')
    df = context['task_instance'].xcom_pull(task_ids='task_process_code_swap')

    df = process_date_offset(df)
    logging.info("Date offset process completed.")

    return df


def airflow_save_to_csv_and_parquet(**context):
    from ETL import save_to_csv_and_parquet
    spark = context['task_instance'].xcom_pull(task_ids='task_create_spark_session')
    df = context['task_instance'].xcom_pull(task_ids='task_process_date_offset')
    output_name = context['params']['output_name']

    logging.info("Saving enriched data to CSV and SNAPPY.PARQUET")
    save_to_csv_and_parquet(df, output_name)
    logging.info("Enriched data saved successfully.")


def airflow_qc_check(**context):
    from ETL import qc, bio_test, state_test, date_test1, date_test2
    spark = context['task_instance'].xcom_pull(task_ids='task_create_spark_session')
    df = context['task_instance'].xcom_pull(task_ids='task_process_date_offset')

    qc_check = qc(df)
    logging.info("QC check completed.")


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
          description="ETL steps for enriching challenge dataset.",
          schedule_interval='0 0 1 * *'  # Once per month
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

task_create_spark_session = PythonOperator(
    task_id='task_create_spark_session',
    python_callable=airflow_create_spark_session,
    dag=dag
)

task_read_spark_df_from_csv = PythonOperator(
    task_id='task_read_spark_df_from_csv',
    python_callable=airflow_read_spark_df_from_csv,
    params={'filepath': input_filepath},
    provide_context=True,
    dag=dag
)

task_process_str_cleaning = PythonOperator(
    task_id='task_process_str_cleaning',
    python_callable=airflow_process_str_cleaning,
    provide_context=True,
    dag=dag
)

task_process_code_swap = PythonOperator(
    task_id='task_process_code_swap',
    python_callable=airflow_process_code_swap,
    params={'abbr_filepath': abbr_filepath},
    provide_context=True,
    dag=dag
)

task_process_date_offset = PythonOperator(
    task_id='task_process_date_offset',
    python_callable=airflow_process_date_offset,
    provide_context=True,
    dag=dag
)

task_save_to_csv_and_parquet = PythonOperator(
    task_id='task_save_to_csv_and_parquet',
    python_callable=airflow_save_to_csv_and_parquet,
    params={'output_name': output_name},
    provide_context=True,
    dag=dag
)

task_qc_check = PythonOperator(
    task_id='task_qc_check',
    python_callable=airflow_qc_check,
    provide_context=True,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator \
>> task_create_spark_session \
>> task_read_spark_df_from_csv \
>> task_process_str_cleaning \
>> task_process_code_swap \
>> task_process_date_offset \
>> task_qc_check \
>> task_save_to_csv_and_parquet \
>> end_operator
