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

###############
upload_bucket_name = keys.upload_bucket_name
scraper = '/opt/airflow/dags/real_estate_scraper'
nekretnine_file = 'nekretnine.csv'
oglasi_file = 'oglasi.csv'

default_args = {
    'owner': 'Marija Celikovic',
    'depends_on_past': False,
    'schedule_interval': '@weekly',
    'start_date': datetime(2022, 10, 11),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def upload_file_to_S3(filename: str, key: str, bucket_name: str) -> None:
    """
    Uploads a local file to s3.
    """
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
        dag_id='real_estate_etl_dag',
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
    scrape_nekretnine = BashOperator(
        task_id='scrape_nekretnine',
        bash_command=f"cd {scraper} && scrapy crawl nekretnine -o /opt/airflow/data/raw_data/scraper/new/nekretnine"
                     f".csv -s CSV_SEP=';'",
        dag=dag)

    scrape_oglasi = BashOperator(
        task_id='scrape_oglasi',
        bash_command=f"cd {scraper} && scrapy crawl oglasi -o /opt/airflow/data/raw_data/scraper/new/oglasi"
                     f".csv -s CSV_SEP=';'",
        dag=dag)

    pyspark_oglasi = SparkSubmitOperator(
        task_id='pyspark_oglasi',
        conn_id='spark_local',
        application='dags/oglasi_spark.py',
        driver_class_path="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        jars="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        total_executor_cores=2,
        name='My_Spark',
        dag=dag
    )
    #
    pyspark_nekretnine = SparkSubmitOperator(
        task_id='pyspark_nekretnine',
        conn_id='spark_local',
        application='dags/nekretnine_spark.py',
        driver_class_path="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        jars="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        total_executor_cores=2,
        name='My_Spark',
        dag=dag
    )
    add_new_oglasi_to_db = SparkSubmitOperator(
        task_id='add_oglasi_to_db',
        conn_id='spark_local',
        application='dags/load_to_db_pyspark.py',
        application_args=['oglasi'],
        jars="/opt/spark/jars/mysql-connector-java-8.0.30.jar",
        total_executor_cores=4,
        name='My_Spark',
        dag=dag,
    )
    add_new_nekretnine_to_db = SparkSubmitOperator(
        task_id='add_nekretnine_to_db',
        conn_id='spark_local',
        application='dags/load_to_db_pyspark.py',
        application_args=['nekretnine'],
        jars="/opt/spark/jars/mysql-connector-java-8.0.30.jar",
        total_executor_cores=4,
        name='My_Spark',
        dag=dag,
    )
    upload_to_S3_oglasi_clean = PythonOperator(
        task_id='upload_oglasi_clean_to_S3',
        python_callable=upload_file_to_S3,
        op_kwargs={
            'filename': '/opt/airflow/data/raw_data/scraper/processed/oglasi.csv',
            'key': f'{oglasi_file[:-4]}-clean-{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            'bucket_name': upload_bucket_name
        },
        dag=dag
    )
    delete_local_oglasi_clean = PythonOperator(
        task_id='delete_local_oglasi_clean',
        python_callable=delete_a_csv_file,
        op_kwargs={
            'path_to_file': '/opt/airflow/data/raw_data/scraper/processed/oglasi.csv'
        },
        dag=dag
    )
    upload_to_S3_oglasi_raw = PythonOperator(
        task_id='upload_oglasi_raw_to_S3',
        python_callable=upload_file_to_S3,
        op_kwargs={
            'filename': '/opt/airflow/data/raw_data/scraper/new/oglasi.csv',
            'key': f'{oglasi_file[:-4]}-clean-{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            'bucket_name': upload_bucket_name
        },
        dag=dag
    )
    delete_local_oglasi_raw = PythonOperator(
        task_id='delete_local_oglasi_raw',
        python_callable=delete_a_csv_file,
        op_kwargs={
            'path_to_file': '/opt/airflow/data/raw_data/scraper/new/oglasi.csv'
        },
        dag=dag
    )

    upload_to_S3_nekretnine_clean = PythonOperator(
        task_id='upload_nekretnine_clean_to_S3',
        python_callable=upload_file_to_S3,
        op_kwargs={
            'filename': '/opt/airflow/data/raw_data/scraper/processed/nekretnine.csv',
            'key': f'{nekretnine_file[:-4]}-clean-{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            'bucket_name': upload_bucket_name
        },
        dag=dag
    )
    delete_local_nekretnine_clean = PythonOperator(
        task_id='delete_local_nekretnine_clean',
        python_callable=delete_a_csv_file,
        op_kwargs={
            'path_to_file': '/opt/airflow/data/raw_data/scraper/processed/nekretnine.csv'
        },
        dag=dag
    )

    upload_to_S3_nekretnine_raw = PythonOperator(
        task_id='upload_nekretnine_raw_to_S3',
        python_callable=upload_file_to_S3,
        op_kwargs={
            'filename': '/opt/airflow/data/raw_data/scraper/new/nekretnine.csv',
            'key': f'{nekretnine_file[:-4]}-clean-{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            'bucket_name': upload_bucket_name
        },
        dag=dag
    )
    delete_local_nekretnine_raw = PythonOperator(
        task_id='delete_local_nekretnine_raw',
        python_callable=delete_a_csv_file,
        op_kwargs={
            'path_to_file': '/opt/airflow/data/raw_data/scraper/new/nekretnine.csv',
        },
        dag=dag
    )

    scrape_oglasi >> upload_to_S3_oglasi_raw >> pyspark_oglasi >> upload_to_S3_oglasi_clean
    scrape_nekretnine >> upload_to_S3_nekretnine_raw >> pyspark_nekretnine >> upload_to_S3_nekretnine_clean
    create_real_estate_table >> add_new_oglasi_to_db
    create_real_estate_table >> add_new_nekretnine_to_db
    pyspark_oglasi >> add_new_oglasi_to_db
    pyspark_nekretnine >> add_new_nekretnine_to_db
    upload_to_S3_oglasi_raw >> delete_local_oglasi_raw
    upload_to_S3_oglasi_clean >> delete_local_oglasi_clean
    upload_to_S3_nekretnine_raw >> delete_local_nekretnine_raw
    upload_to_S3_nekretnine_clean >> delete_local_nekretnine_clean
