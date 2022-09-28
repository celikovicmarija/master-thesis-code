import sys

from pyspark.sql import *
from pyspark.sql import functions as f

from lib.logger import Log4j
from lib.utils import load_posts_data

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Oglasi Spark") \
        .master("local[3]") \
        .getOrCreate()

    # NOT WORKING
    # conf = get_spark_app_config()

    # NOT WORKING
    # conf = SparkConf()
    # conf.set("spark.app.name", "Hello Spark")
    # conf.set("spark.master", "local[3")

    # conf=SparkConf().setMaster("local[3]").setAppName("Hello Spark")
    # spark = SparkSession.builder.config(conf).getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Spark")

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    oglasi_df = load_posts_data(spark, sys.argv[1])
    logger.info(oglasi_df.show(20))
    oglasi_df = oglasi_df.dropDuplicates()
    oglasi_df = oglasi_df.withColumn('floor_number',
                                     f.when(f.lower(f.col('floor_number')).contains('prizemlje'), '0')
                                     .when(f.col('floor_number').rlike(r'\d'),
                                           f.regexp_extract(f.col('floor_number'), r'\d', 0)).otherwise(
                                         f.col('floor_number'))
                                     )
    oglasi_df = oglasi_df.withColumn('number_of_rooms',
                                     f.when(f.lower(f.col('number_of_rooms')).contains('jednosoban'), '1')
                                     .when(f.lower(f.col('number_of_rooms')).contains('jednoiposoban'), '1.5')
                                     .when(f.lower(f.col('number_of_rooms')).contains('dvosoban'), '2')
                                     .when(f.lower(f.col('number_of_rooms')).contains('dvoiposoban'), '2.5')
                                     .when(f.lower(f.col('number_of_rooms')).contains('trosoban'), '3')
                                     .when(f.lower(f.col('number_of_rooms')).contains('troiposoban'), '3.5')
                                     .when(f.lower(f.col('number_of_rooms')).contains('četvorosoban'), '4+')
                                     .otherwise(f.col('number_of_rooms')))

    oglasi_df = oglasi_df.withColumn('size_in_squared_meters', f.split(f.col('size_in_squared_meters'), 'm2')[0])
    # oglasi_df = oglasi_df.withColumn('heating_type', f.regexp_replace(f.col('size_in_squared_meters'),  r'\n|\r', ' '))
    # oglasi_df = oglasi_df.withColumn('street', f.regexp_replace(f.col('street'),  r'\n|\r', ' '))
    for col in ['heating_type', 'street', 'object_state']:
        oglasi_df = oglasi_df.withColumn(col, f.regexp_replace(f.col(col), '[\\n|\\r]', ' ')) \
            .withColumn(col, f.trim(f.col(col)))

    # oglasi_df = oglasi_df.withColumn('location', f.when(f.size(f.split(f.col('location'), '\\(')) == 2, \
    #                                                     oglasi_df.withColumn('micro_location',
    #                                                                          f.split(f.col('location'), '\\)')[0]) \
    #                                                     .withColumn('micro_location',
    #                                                                 f.regexp_replace(f.col('micro_location'), '\\(',
    #                                                                                  '')) \
    #                                                     .withColumn('micro_location', f.trim(f.col('micro_location'))) \
    #                                                     .withColumn('city', f.split(f.col('location'), '\\(')[2]) \
    #                                                     .withColumn('micro_location', f.split(f.col('city'), ',')[2]) \
    #                                                     .withColumn('city', f.split(f.col('city'), ',')[1])))

    oglasi_df = oglasi_df.withColumn('micro_location', f.split(f.col('location'), '\\(')[0]) \
        .withColumn('micro_location', f.trim(f.col('micro_location'))) \
        .withColumn('city', f.split(f.col('location'), '\\(')[1]) \
        .withColumn('city', f.split(f.col('city'), ',')[1]) \
        .withColumn('location', f.regexp_replace(f.col('location'), '\\)', ''))

    ##.withColumn('location', f.split(f.col('city'), ',')[2]) \

    logger.info("Stopping Spark")
    logger.info(oglasi_df.show())

    # spark.stop()
