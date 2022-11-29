import os
import sys

from pyspark.sql import functions as f
sys.path.append('../data_preparation')
from ..utilities.pyspark_utils import load_data, save_data_to_db_table, read_from_db, build_spark_session

if __name__ == "__main__":
    spark = build_spark_session()
    monitoring_station_df = read_from_db(spark, 'monitoring_station')

    for file in os.listdir('Kvalitet vazduha'):
        air_quality_df = load_data(spark, 'Kvalitet vazduha\\' + file)
        all_columns = []
        air_quality_df = air_quality_df.withColumnRenamed(' pm25', 'pm25')
        air_quality_df = air_quality_df.withColumnRenamed('monitoring_station', 'monitoring_station_id')
        air_quality_df = air_quality_df.withColumnRenamed(' pm10', 'pm10')
        air_quality_df = air_quality_df.withColumnRenamed(' o3', 'o3')
        air_quality_df = air_quality_df.withColumnRenamed(' no2', 'no2')
        air_quality_df = air_quality_df.withColumnRenamed(' so2', 'so2')
        air_quality_df = air_quality_df.withColumnRenamed(' co', 'co')

        for col in air_quality_df.columns:
            air_quality_df = air_quality_df.withColumn(col,
                                                       f.when(f.col(col) == '-', f.lit(None)).otherwise(f.col(col)))
            air_quality_df = air_quality_df.withColumn(col,
                                                       f.when(f.col(col) == ' ', f.lit(None)).otherwise(f.col(col)))

        # for col in air_quality_df.columns:
        #     all_columns.append(col)
        # air_quality_df.show()
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        air_quality_df = air_quality_df.withColumn('date', f.to_date(f.col('date'), 'dd/MM/yyyy'))

        # air_quality_df = air_quality_df.withColumn('monitoring_station', f.trim(f.lower(f.col('monitoring_station'))))
        # monitoring_station_df = monitoring_station_df.withColumn('monitoring_station_name',
        #                                                          f.trim(f.lower(f.col('monitoring_station_name'))))
        # air_quality_df = air_quality_df.withColumnRenamed('monitoring_station', 'monitoring_station_string')
        #
        # df = air_quality_df.join(monitoring_station_df,
        #                          f.col('monitoring_station_string') == f.col('monitoring_station_name'), how="left")
        # all_columns.append('monitoring_station_id')
        # df = df.drop('monitoring_station_string')
        # all_columns.remove('monitoring_station')
        # df = df.dropDuplicates(['date'])
        air_quality_df = air_quality_df.dropDuplicates(['date'])

        # air_quality_df = df.select(all_columns)
        save_data_to_db_table(air_quality_df, 'air_quality')

