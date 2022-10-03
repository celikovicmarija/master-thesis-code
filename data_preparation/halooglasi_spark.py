from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType, StringType
from utilities.pyspark_utils import load_posts_data
from utilities.geoapify import send_and_receive_place_details_api
from utilities.pyspark_utils import clean_from_characters, capitalize_words_and_lowercase, trim_from_spaces

from utilities.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Ooglasi Spark") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Spark")

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    oglasi_df = load_posts_data(spark, 'real_estate_scraper\halooglasi.csv')
    oglasi_df = oglasi_df.dropDuplicates(['link'])
    # logger.info(oglasi_df.show(100))

    oglasi_df=clean_from_characters(oglasi_df)
    oglasi_df=capitalize_words_and_lowercase(oglasi_df)

    oglasi_df = oglasi_df.withColumn('price', f.when(f.col('price').contains('.') | (f.length(f.col('price')) < 2),
                                                     f.col('price').cast(FloatType()) * 1000).otherwise(
        f.col('price')))  # .cast(FloatType())

    oglasi_df = oglasi_df.withColumn('floor_number',
                                     f.regexp_replace(f.col('floor_number'), 'VRP', '0')) \
        .withColumn('floor_number',
                    f.regexp_replace(f.col('floor_number'), 'SUT', '-1')) \
        .withColumn('floor_number',
                    f.regexp_replace(f.col('floor_number'), 'PSUT', '-1')) \
        .withColumn('floor_number',
                    f.regexp_replace(f.col('floor_number'), 'PR', '0')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'CG', 'Centralno grejanje')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'EG', 'Etažno grejanje')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'TA', 'TA peć')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'Gas', 'Grejanje na gas'))

    oglasi_df = oglasi_df.withColumn('w_type', f.when(f.lower(f.col('link')).contains('prodaja'), 'Prodaja') \
                                     .when(f.lower(f.col('link')).contains('izdava'), 'Izdavanje').otherwise(None))

    oglasi_df = oglasi_df.withColumn('is_listed', f.when(f.lower(f.col('additional')).contains('uknji') | \
                                                         f.lower(f.col('description')).contains('uknji') | \
                                                         f.lower(f.col('title')).contains('uknji'), 1).otherwise(0))
    oglasi_df = oglasi_df.withColumn('additional', f.regexp_replace(f.col('additional'), 'Uknjizen', ''))

    oglasi_df = oglasi_df.withColumn('monthly_bills',
                                     f.regexp_replace(f.col('monthly_bills'), '[^0-9]', ''))  # EURO amount

    oglasi_df = oglasi_df.withColumn('real_estate_type', f.when(f.lower(f.col('link')).contains('stanova/'), 'Stan') \
                                     .when(f.lower(f.col('link')).contains('kuca/'), 'Kuća') \
                                     .when(f.lower(f.col('link')).contains('soba/'), 'Soba') \
                                     .when(f.lower(f.col('link')).contains('poslovni-prost'), 'Poslovni objekat') \
                                     .when(f.lower(f.col('link')).contains('garaza/'), 'Garaže i parking') \
                                     .when(f.lower(f.col('link')).contains('zemljista/'), 'Zemljište').otherwise(
        'Ostalo'))

    oglasi_df = oglasi_df.withColumn('size_metric',
                                     f.when(f.col('size_in_squared_meters').contains('ar'), 'ar').otherwise(
                                         'm'))
    oglasi_df = oglasi_df.withColumn('size_in_squared_meters', f.when(f.col('size_in_squared_meters').contains('m'),
                                                                      f.trim(
                                                                          f.split(f.col('size_in_squared_meters'), 'm')[
                                                                              0]))
                                     .when(f.col('size_in_squared_meters').contains('ar'),
                                           f.trim(f.split(f.col('size_in_squared_meters'), 'ar')[0])).otherwise(
        f.col('size_in_squared_meters')))

    oglasi_df=oglasi_df.withColumn('source',f.lit('halooglasi'))
    oglasi_df=trim_from_spaces(oglasi_df)

    logger.info(oglasi_df.show(500))


    df_for_geocoding = oglasi_df.select('street', 'micro_location').distinct()
    # logger.info(df_for_geocoding.show())
    df_for_geocoding = df_for_geocoding.toPandas()
    logger.info(df_for_geocoding.shape)
    logger.info(df_for_geocoding)
    # send_and_receive_geocoding(df_for_geocoding)
    # send_and_receive_place_api()
    send_and_receive_place_details_api()
