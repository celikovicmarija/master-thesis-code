import configparser

import cyrtranslit
from pyspark import SparkConf
from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField, FloatType


def load_posts_data(spark: SparkSession, data_file: str) -> DataFrame:
    return spark.read.option("header", "true") \
        .option("delimiter", ",") \
        .option("mode", "FAILFAST") \
        .csv(data_file, multiLine=True, columnNameOfCorruptRecord='broken',
             encoding='utf-8')  # .option("inferSchema", "true")


def clean_from_characters(oglasi_df):
    for col in oglasi_df.columns:
        oglasi_df = oglasi_df.withColumn(col, f.regexp_replace(f.col(col), '[\n|\r|\"|*|!|\t|,|-|~|\s]', ' '))  # \
        oglasi_df = oglasi_df.withColumn(col, f.regexp_replace(f.col(col), 'n/a ', ''))
        oglasi_df = oglasi_df.withColumn(col, f.trim(f.col(col)))
    return oglasi_df


def capitalize_words_and_lowercase(oglasi_df):
    for col in ['additional', 'description', 'title', 'street', 'location', 'micro_location']:
        oglasi_df = oglasi_df.withColumn(col, f.initcap(f.lower(f.col(col))))
    return oglasi_df


def trim_from_spaces(oglasi_df):
    for col in oglasi_df.columns:
        oglasi_df = oglasi_df.withColumn(col, f.trim(f.col(col)))
    return oglasi_df


@udf(returnType=StringType())
def transliterate_serbian_udf(col):
    return cyrtranslit.to_latin(col, 'sr')


def transliterate_serbian(oglasi_df):
    return oglasi_df.withColumn('description', transliterate_serbian_udf(f.col('description')))


def extract_columns_for_geoapify(oglasi_df):
    df_for_geocoding = oglasi_df.select('street', 'micro_location').distinct()
    return df_for_geocoding.toPandas()

def save_file_to_csv(oglasi_df, file_name):
    oglasi_df.toPandas().to_csv(file_name, index=False)




def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("data_preparation\spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


Schema = StructType([
    StructField('additional', StringType(), nullable=True),
    StructField('advertiser', StringType(), nullable=True),
    StructField('city', StringType(), nullable=True),
    StructField('city_lines', StringType(), nullable=True),
    StructField('date', StringType(), nullable=True),
    StructField('description', StringType(), nullable=True),
    StructField('floor_number', StringType(), nullable=True),
    StructField('heating_type', StringType(), nullable=True),
    StructField('link', StringType(), nullable=True),
    StructField('location', StringType(), nullable=True),
    StructField('micro_location', StringType(), nullable=True),
    StructField('monthly_bills', StringType(), nullable=True),
    StructField('number_of_rooms', StringType(), nullable=True),  # IntegerType
    StructField('object_state', StringType(), nullable=True),
    StructField('object_type', StringType(), nullable=True),
    StructField('price', FloatType(), nullable=True),
    StructField('price_per_unit', FloatType(), nullable=True),
    StructField('real_estate_type', StringType(), nullable=True),
    StructField('size_in_squared_meters', StringType(), nullable=True),  # FloatType
    StructField('street', StringType(), nullable=True),
    StructField('title', StringType(), nullable=True),
    StructField('total_number_of_floors', StringType(), nullable=True),
    StructField('w_type', StringType(), nullable=True),
])

