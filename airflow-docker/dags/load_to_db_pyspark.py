import sys

from utilities.pyspark_utils import load_data
from utilities.pyspark_utils import save_data_to_db_table, build_spark_session


def save_clean_data_to_db(file: str = None):
    if not file:
        file = sys.argv[1]
    spark = build_spark_session()
    oglasi_df = load_data(spark, f'/opt/airflow/data/raw_data/scraper/ready_for_db/{file}.csv')
    oglasi_df = oglasi_df.drop('_c0')
    save_data_to_db_table(oglasi_df, 'real_estate_post')


save_clean_data_to_db()

if __name__ == "__main__":
    save_clean_data_to_db()
