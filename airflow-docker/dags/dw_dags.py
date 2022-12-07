from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from utilities.pyspark_utils import get_keys_and_constants
from utilities.sql_scripts import truncate_exchange_rate_cdc, truncate_air_quality_cdc, truncate_real_estate_cdc
keys = get_keys_and_constants()
mysql_jar = keys.mysql_connector_jar
default_args = {
    'owner': 'Marija Celikovic',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 11),
    'retries': 2,
    'schedule_interval': '@monthly',
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        dag_id='data_warehouse_dag',
        schedule_interval='@weekly',
        start_date=datetime(2022, 3, 1),
        default_args=default_args,
        catchup=False
) as dag:
    exchange_rate_to_dw = SparkSubmitOperator(
        task_id='exchange_rate_to_dw',
        conn_id='spark_local',
        application='dags/exchange_rate_to_dw.py',
        driver_class_path=mysql_jar,
        jars=mysql_jar,
        total_executor_cores=2,
        name='My_Spark'
    )
    air_quality_to_dw = SparkSubmitOperator(
        task_id='air_quality_to_dw',
        conn_id='spark_local',
        application='dags/air_quality_to_dw.py',
        driver_class_path=mysql_jar,
        jars=mysql_jar,
        total_executor_cores=2,
        name='My_Spark'
    )
    real_estate_to_dw = SparkSubmitOperator(
        task_id='real_estate_to_dw',
        conn_id='spark_local',
        application='dags/real_estate_to_dw.py',
        driver_class_path=mysql_jar,
        jars=mysql_jar,
        total_executor_cores=2,
        name='My_Spark'
    )
    truncate_real_estate_cdc_table = MySqlOperator(
        task_id='truncate_real_estate_cdc_table',
        sql=truncate_real_estate_cdc,
        mysql_conn_id='mysql_db_local',
        database='real_estate_db'
    )
    truncate_air_quality_cdc_table = MySqlOperator(
        task_id='truncate_air_quality_cdc_table',
        sql=truncate_air_quality_cdc,
        mysql_conn_id='mysql_db_local',
        database='real_estate_db'
    )
    truncate_exchange_rate_cdc_table = MySqlOperator(
        task_id='truncate_exchange_rate_cdc_table',
        sql=truncate_exchange_rate_cdc,
        mysql_conn_id='mysql_db_local',
        database='real_estate_db',
    )

    exchange_rate_to_dw >> truncate_exchange_rate_cdc_table
    air_quality_to_dw >> truncate_air_quality_cdc_table
    real_estate_to_dw >> truncate_real_estate_cdc_table
