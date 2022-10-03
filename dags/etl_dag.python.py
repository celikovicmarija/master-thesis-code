from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from utilities.s3_operations import upload_to_s3
from datetime import datetime, timedelta

default_args = {
    'owner': 'Dan M',
    'depends_on_past': False,
   # 'start_date': datetime(2022, 10, 11),
    'start_date': days_ago(2)
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

nekretnine_scraper=''
oglasi_scraper=''
cetirizida_scraper=''
halooglasi_scraper=''
dag = DAG('InitialETLPipeline', default_args=default_args)

scrape_nekretnine = BashOperator(
    task_id='scrape_nekretnine',
    bash_command='cd {} && scrapy crawl nekretnine'.format(nekretnine_scraper),
    dag=dag)

scrape_oglasi = BashOperator(
    task_id='scrape_oglasi',
    bash_command='cd {} && scrapy crawl oglasi'.format(oglasi_scraper),
    dag=dag)

scrape_cetirizida = BashOperator(
    task_id='scrape_cetirizida',
    bash_command='cd {} && scrapy crawl cetirizida'.format(cetirizida_scraper),
    dag=dag)

scrape_halooglasi = BashOperator(
    task_id='scrape_halooglasi',
    bash_command='cd {} && scrapy crawl halooglasi'.format(halooglasi_scraper),
    dag=dag)


pyspark_nekretnine = PythonOperator(
    task_id='prepare_nekretnine_data_pyspark',
    python_callable=clear_folder,
    dag=dag)

pyspark_oglasi = PythonOperator(
    task_id='prepare_oglasi_data_pyspark',
    python_callable=clear_folder,
    dag=dag)

pyspark_cetirizida = PythonOperator(
    task_id='prepare_cetirizida_data_pyspark',
    python_callable=clear_folder,
    dag=dag)

with DAG(
    dag_id='s3_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

    # Upload the file
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/Users/dradecic/airflow/data/posts.json',
            'key': 'posts.json',
            'bucket_name': 'bds-airflow-bucket'
        }
    )