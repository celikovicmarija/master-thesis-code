from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# these three dags are independent of each other

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
    interest_rate_to_dw = SparkSubmitOperator(
        task_id='interest_rate_to_dw',
        conn_id='spark_local',
        application='dags/interest_rate_to_dw.py',
        driver_class_path="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        jars="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        total_executor_cores=2,
        name='My_Spark',
        dag=dag
    )
    air_quality_to_dw = SparkSubmitOperator(
        task_id='air_quality_to_dw',
        conn_id='spark_local',
        application='dags/air_quality_to_dw.py',
        driver_class_path="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        jars="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        total_executor_cores=2,
        name='My_Spark',
        dag=dag
    )
    real_estate_to_dw = SparkSubmitOperator(
        task_id='real_estate_to_dw',
        conn_id='spark_local',
        application='dags/extract_real_estate_to_dw.py',
        driver_class_path="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        jars="/opt/airflow/dags/mysql-connector-java-8.0.30.jar",
        total_executor_cores=2,
        name='My_Spark',
        dag=dag
    )
