from pyspark.pandas import DataFrame
from pyspark.sql import functions as f

from utilities.pyspark_utils import save_data_to_dw_table, read_from_db, read_from_dw, build_spark_session


def refresh_dw_with_newest_air_quality_data(air_quality_cdc: DataFrame, dim_date_dw: DataFrame):
    joined_df = dim_date_dw.join(air_quality_cdc, dim_date_dw.fulldate == air_quality_cdc.date,
                                 how='inner')
    joined_df = find_municipality_for_monitoring_station(joined_df)
    new_rows_to_add = joined_df.select(f.col('pm25'), f.col('pm10'), f.col('o3'), f.col('no2'), f.col('so2'),
                                       f.col('co'), f.col('monitoring_station_id'), f.col('date_id'))
    save_data_to_dw_table(new_rows_to_add, 'fact_air_quality')


def extract_air_quality_to_dw():
    spark = build_spark_session()

    air_quality_cdc = read_from_db(spark, 'air_quality_cdc')
    dim_date_dw = read_from_dw(spark, 'dim_date')

    refresh_dw_with_newest_air_quality_data(air_quality_cdc, dim_date_dw)


def find_municipality_for_monitoring_station(df: DataFrame) -> DataFrame:
    return df.withColumn('municipality_id', f.when(f.col('monitoring_station_id') == 4, 3)
                         .when(f.col('monitoring_station_id') == 5, 128)
                         .when(f.col('monitoring_station_id') == 3, 15)
                         .when(f.col('monitoring_station_id') == 8, 13)
                         .when(f.col('monitoring_station_id') == 11, 107)
                         .when(f.col('monitoring_station_id') == 14, 10)
                         .when((f.col('monitoring_station_id') == 18) | (f.col('monitoring_station_id') == 42), 116)
                         .when(f.col('monitoring_station_id') == 19, 98)
                         .when(f.col('monitoring_station_id') == 20, 70)
                         .when(f.col('monitoring_station_id') == 21, 5)
                         .when((f.col('monitoring_station_id') == 10) | (f.col('monitoring_station_id') == 27), 9)
                         .when((f.col('monitoring_station_id') == 15) | (f.col('monitoring_station_id') == 16)
                               | (f.col('monitoring_station_id') == 38) | (f.col('monitoring_station_id') == 39)
                               , 38)
                         .when((f.col('monitoring_station_id') == 12) | (f.col('monitoring_station_id') == 13), 44)
                         .when((f.col('monitoring_station_id') == 6) | (f.col('monitoring_station_id') == 9), 990)
                         .when((f.col('monitoring_station_id') == 1) | (f.col('monitoring_station_id') == 2) |
                               (f.col('monitoring_station_id') == 64), 11).otherwise(f.col('monitoring_station_id')))


# extract_air_quality_to_dw()

if __name__ == "__main__":
    extract_air_quality_to_dw()
