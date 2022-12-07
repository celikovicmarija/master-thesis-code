import os
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from utilities.pyspark_utils import get_keys_and_constants
from utilities.sql_scripts import create_real_estate_table

keys = get_keys_and_constants()

upload_bucket_name = keys.upload_bucket_name
mysql_jar = keys.mysql_connector_jar
scraper = '/opt/airflow/dags/real_estate_scraper'
data_location = '/opt/airflow/data/raw_data/scraper'

oglasi_file = 'oglasi'

default_args = {
    'owner': 'Marija Celikovic',
    'depends_on_past': False,
    'schedule_interval': '@weekly',
    'start_date': datetime(2022, 10, 11),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'dagrun_timeout': timedelta(minutes=60)
}


def upload_file_to_S3(filename: str, key: str, bucket_name: str) -> None:
    print(os.getcwd())
    import logging
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    logging.info(f"loaded {filename} to s3 bucket:{bucket_name} as {key}")


def delete_a_csv_file(path_to_file: str) -> None:
    if os.path.exists(path_to_file):
        os.remove(path_to_file)
        print("The file: {} is deleted!".format(path_to_file))
    else:
        print("The file: {} does not exist!".format(path_to_file))


with DAG(
        dag_id='real_estate_etl_dag_oglasi',
        schedule_interval='@daily',
        start_date=datetime(2022, 3, 1),
        default_args=default_args,
        catchup=False
) as dag:
    create_real_estate_table = MySqlOperator(
        task_id='create_real_estate_table',
        sql=create_real_estate_table,
        mysql_conn_id='mysql_db_local',
        database='real_estate_db'
    )

    scrape_oglasi = BashOperator(
        task_id='scrape_oglasi',
        bash_command=f"cd {scraper} && scrapy crawl oglasi -o {data_location}/new/{oglasi_file}"
                     f".csv -s CSV_SEP=';'",
        dag=dag)

    pyspark_oglasi = SparkSubmitOperator(
        task_id='pyspark_oglasi',
        conn_id='spark_local',
        application='dags/oglasi_spark.py',
        driver_class_path=mysql_jar,
        jars=mysql_jar,
        total_executor_cores=2,
        name='My_Spark',
        dag=dag
    )
    find_foreign_keys_oglasi = SparkSubmitOperator(
        task_id='find_foreign_keys_oglasi',
        conn_id='spark_local',
        application='dags/find_reference_data.py',
        application_args=['oglasi'],
        jars=mysql_jar,
        total_executor_cores=4,
        name='My_Spark',
        dag=dag,
    )

    upload_to_S3_oglasi_clean = PythonOperator(
        task_id='upload_oglasi_clean_to_S3',
        python_callable=upload_file_to_S3,
        op_kwargs={
            'filename': f'{data_location}/processed/oglasi.csv',
            'key': f'{oglasi_file[:-4]}-clean-{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            'bucket_name': upload_bucket_name
        },
        dag=dag
    )
    delete_local_oglasi_clean = PythonOperator(
        task_id='delete_local_oglasi_clean',
        python_callable=delete_a_csv_file,
        op_kwargs={
            'path_to_file': f'{data_location}/processed/oglasi.csv'
        },
        dag=dag
    )
    save_oglasi_to_database = SparkSubmitOperator(
        application='dags/load_to_db_pyspark.py',
        driver_class_path=mysql_jar,
        conn_id="spark_local",
        task_id="oglasi_to_database_park_to_jdbc_job",
        application_args=['oglasi']
    )
    upload_to_S3_oglasi_raw = PythonOperator(
        task_id='upload_oglasi_raw_to_S3',
        python_callable=upload_file_to_S3,
        op_kwargs={
            'filename': f'{data_location}/new/oglasi.csv',
            'key': f'{oglasi_file[:-4]}-raw-{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            'bucket_name': upload_bucket_name
        },
        dag=dag
    )
    delete_local_oglasi_raw = PythonOperator(
        task_id='delete_local_oglasi_raw',
        python_callable=delete_a_csv_file,
        op_kwargs={
            'path_to_file': f'{data_location}/new/oglasi.csv'
        },
        dag=dag
    )
    upload_to_S3_oglasi_ready_for_db = PythonOperator(
        task_id='upload_to_S3_oglasi_ready_for_db',
        python_callable=upload_file_to_S3,
        op_kwargs={
            'filename': f'{data_location}/ready_for_db/oglasi.csv',
            'key': f'{oglasi_file[:-4]}-clean-{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            'bucket_name': upload_bucket_name
        },
        dag=dag
    )
    delete_local_oglasi_ready_for_db = PythonOperator(
        task_id='delete_local_oglasi_clean_ready_for_db',
        python_callable=delete_a_csv_file,
        op_kwargs={
            'path_to_file': f'{data_location}/ready_for_db/oglasi.csv'
        },
        dag=dag
    )

    scrape_oglasi >> pyspark_oglasi
    pyspark_oglasi >> find_foreign_keys_oglasi
    create_real_estate_table >> save_oglasi_to_database

    pyspark_oglasi >> upload_to_S3_oglasi_raw
    upload_to_S3_oglasi_raw >> delete_local_oglasi_raw

    find_foreign_keys_oglasi >> save_oglasi_to_database
    find_foreign_keys_oglasi >> upload_to_S3_oglasi_clean
    upload_to_S3_oglasi_clean >> delete_local_oglasi_clean

    save_oglasi_to_database >> upload_to_S3_oglasi_ready_for_db
    upload_to_S3_oglasi_ready_for_db >> delete_local_oglasi_ready_for_db
