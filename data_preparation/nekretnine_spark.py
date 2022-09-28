import sys

from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType

from lib.logger import Log4j
from lib.utils import load_posts_data

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Nekretnine Spark") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Spark")

    conf_out = spark.sparkContext.getConf()
    oglasi_df = load_posts_data(spark, 'nekretnine.csv')
    logger.info(oglasi_df.show(100))
    oglasi_df = oglasi_df.dropDuplicates()


    oglasi_df=oglasi_df.withColumn('title', f.regexp_replace(f.col('title'), '[^a-zA-Z0-9]', ''))
    logger.info(oglasi_df.show(100))