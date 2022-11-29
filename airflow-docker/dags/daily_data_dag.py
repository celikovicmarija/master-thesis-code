from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from actions import fill_daily_stations_data, find_eur_to_rsd_and_usd_exchange_rate_for_today
from utilities.sql_scripts import create_air_quality_table, create_exchange_rates_table

default_args = {
    'owner': 'Marija Celikovic',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 11),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}
# TODO: replace local with RDS connection

with DAG(
        dag_id='daily_data_dag',
        schedule_interval='@daily',
        start_date=datetime(2022, 3, 1),
        default_args=default_args,
        catchup=False
) as dag:
    create_air_quality_table = MySqlOperator(
        task_id='create_air_quality_table',
        sql=create_air_quality_table,
        mysql_conn_id='mysql_db_local',
        database='real_estate_db'
    )
    create_exchange_rate_table = MySqlOperator(
        task_id='create_exchange_rate_table',
        sql=create_exchange_rates_table,
        mysql_conn_id='mysql_db_local',
        database='real_estate_db'
    )
    find_air_quality_daily = PythonOperator(
        task_id='find_air_quality_data',
        python_callable=fill_daily_stations_data,
        dag=dag)

    find_daily_exchange_rate = PythonOperator(
        task_id='find_daily_exchange_rate',
        python_callable=find_eur_to_rsd_and_usd_exchange_rate_for_today,
        dag=dag)

create_air_quality_table >> find_air_quality_daily
create_exchange_rate_table >> find_daily_exchange_rate
