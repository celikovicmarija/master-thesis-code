from pyspark.pandas import DataFrame
from pyspark.sql import functions as f

from utilities.pyspark_utils import save_data_to_dw_table, read_from_db, read_from_dw, build_spark_session


def find_latest_date_id_in_dw(air_quality_dw: DataFrame):
    max_date = air_quality_dw.select(f.max(f.col('date_id')).alias('max_date')).collect()[0]['max_date']
    return max_date


def refresh_dw_with_newest_air_quality_data(latest_date: int, air_quality_cdc: DataFrame, dim_date_dw: DataFrame):
    # max_date_standard = dim_date_dw.select(f.col('fulldate')).filter(f.col('date_id') == latest_date)
    joined_df = dim_date_dw.join(air_quality_cdc, dim_date_dw.fulldate == air_quality_cdc.date,
                                 how='inner').filter(f.col('date_id') > latest_date)
    new_rows_to_add = joined_df.select(f.col('pm25'), f.col('pm10'), f.col('o3'), f.col('no2'), f.col('so2'),
                                       f.col('co'), f.col('monitoring_station_id'), f.col('date_id'))
    new_rows_to_add.show()
    save_data_to_dw_table(new_rows_to_add, 'fact_air_quality')


def extract_air_quality_to_dw():
    spark = build_spark_session()

    air_quality_cdc = read_from_db(spark, 'air_quality_cdc')
    air_quality_dw = read_from_dw(spark, 'fact_air_quality')
    dim_date_dw = read_from_dw(spark, 'dim_date')

    latest_date = find_latest_date_id_in_dw(air_quality_dw)
    refresh_dw_with_newest_air_quality_data(latest_date, air_quality_cdc, dim_date_dw)


# extract_air_quality_to_dw()

if __name__ == "__main__":
    extract_air_quality_to_dw()
