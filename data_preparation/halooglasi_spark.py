import sys

from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType

from lib.logger import Log4j
from lib.utils import load_posts_data

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Halooglasi Spark") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Spark")

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    oglasi_df = load_posts_data(spark, sys.argv[1])
    logger.info(oglasi_df.show(20))
    oglasi_df = oglasi_df.dropDuplicates()

    oglasi_df = oglasi_df.withColumn('temp1',
                                     f.when(f.lower(f.col('advertiser')).contains('gradnja'), f.col('advertiser')))

    oglasi_df = oglasi_df.withColumn('object_type',
                                     f.when(f.col('object_type').isNull(), f.col('temp1')).otherwise(
                                         f.concat(f.concat('object_type', f.lit(' ')), 'temp1').alias('object_type')))

    oglasi_df = oglasi_df.withColumn('advertiser',
                                     f.when(f.col('number_of_rooms').rlike("\D+"),
                                            f.col('number_of_rooms')).otherwise(f.col('advertiser')))

    oglasi_df = oglasi_df.withColumn('number_of_rooms',
                                     f.when(f.col('size_in_squared_meters').contains('+'),
                                            f.col('size_in_squared_meters')).otherwise(f.col('number_of_rooms')))

    oglasi_df = oglasi_df.withColumn('number_of_rooms',
                                     f.when(f.length(f.col('size_in_squared_meters')).__le__(3),
                                            f.col('size_in_squared_meters')).otherwise(f.col('number_of_rooms')))

    oglasi_df = oglasi_df.withColumn('size_in_squared_meters',
                                     f.when(f.length(f.col('real_estate_type')).__ge__(4) &
                                            f.col('real_estate_type').rlike("\D+"),
                                            f.col('real_estate_type')).otherwise(f.col('size_in_squared_meters')))

    oglasi_df = oglasi_df.withColumn('size_in_squared_meters',
                                     f.regexp_replace(f.col('size_in_squared_meters'), r' m', ''))

    oglasi_df = oglasi_df.withColumn('price', f.when(f.length(f.col('price')).__le__(3),
                                                     f.col('price').cast(FloatType()) * 1000).otherwise(
        f.col('price').cast(FloatType())))


    oglasi_df = oglasi_df.withColumn('link',
                                     f.when(f.length(f.col('real_estate_type')).__ge__(4) &
                                            f.col('real_estate_type').rlike("\D+"),
                                            f.col('real_estate_type')).otherwise(f.col('size_in_squared_meters')))

    logger.info(oglasi_df.show(20))
    oglasi_df.drop('temp1')
