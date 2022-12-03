import configparser

import cyrtranslit
import pandas as pd
from pyspark import SparkConf
from pyspark.pandas import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField, FloatType, IntegerType

from .config import get_keys_and_constants, get_db_settings

keys = get_keys_and_constants()
db_settings = get_db_settings()
mysql_connector_jar = keys.mysql_connector_jar
dw_real_estate_url = keys.dw_real_estate_url
real_estate_db_url = keys.real_estate_db_url
mysql_driver = keys.mysql_driver

user = db_settings.db_user
pwd = db_settings.db_password


def build_spark_session() -> SparkSession:
    """
    Returns a local pyspark session with basic configuration.
    :return:
    """
    import os
    import sys

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    return SparkSession.builder \
        .appName("My_Spark") \
        .master("local[*]") \
        .config("spark.jars", mysql_connector_jar) \
        .config("spark.sql.broadcastTimeout", "36000") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold ", "-1") \
        .config("spark.executor.memory", "100g") \
        .config("spark.driver.memory", "200g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "16g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sparkContext.defaultParallelism", "true") \
        .getOrCreate()


def load_data_pandas(spark: SparkSession, data_file: str, sep: str = ',', schema: StructType = None) -> DataFrame:
    """
    Since pyspark's solution for importing csv files that contain comma lead to error,
    this function is created to handle the data frame creation. The assumption is
    that the file contains a header.
    :param sep:
    :param schema:
    :param spark:
    :param data_file:
    :return:
    """
    sql_sc = SQLContext(spark.sparkContext)
    sc = spark.sparkContext

    pandas_df = pd.read_csv(data_file, sep=sep, error_bad_lines=False)

    if not schema:
        s_df = sql_sc.createDataFrame(pandas_df, schema=place_details_schema).repartition(120)
    else:
        s_df = sql_sc.createDataFrame(pandas_df, schema=schema).repartition(120)
    return s_df


# TODO: remove duplicated functions


def save_data_to_dw_table(new_rows_to_add: DataFrame, table_name: str) -> None:
    new_rows_to_add.write.format('jdbc').options(
        url=dw_real_estate_url,
        driver=mysql_driver,
        dbtable=table_name,
        user=user,
        password=pwd).option('batchsize', '1000').mode('append').save()


def save_data_to_db_table(new_rows_to_add: DataFrame, table_name: str) -> None:
    new_rows_to_add.write.format('jdbc').options(
        url=real_estate_db_url,
        driver=mysql_driver,
        dbtable=table_name,
        user=user,
        password=pwd).option('batchsize', '100').mode('append').save()


def read_from_db(spark: SparkSession, table: str) -> DataFrame:
    df = spark.read.format('jdbc').options(
        url=real_estate_db_url,
        driver=mysql_driver,
        dbtable=table,
        user=user,
        password=pwd).option('fetchsize', '1000').load()
    df.repartition(1000)
    return df


def read_from_dw(spark: SparkSession, table: str) -> DataFrame:
    df = spark.read.format('jdbc').options(
        url=dw_real_estate_url,
        driver=mysql_driver,
        dbtable=table,
        user=user,
        password=pwd).option('fetchsize', '100000').load()
    df.repartition(20)
    return df


def load_data(spark: SparkSession, data_file: str) -> DataFrame:
    df = spark.read.option("header", "true") \
        .option("delimiter", ";") \
        .option("driver", mysql_driver) \
        .csv(data_file, multiLine=True, columnNameOfCorruptRecord='broken',
             encoding='utf-8')
    df.repartition(20)
    return df


def load_posts_data(spark: SparkSession, data_file: str) -> DataFrame:
    df = spark.read.option("header", "true") \
        .option("delimiter", ",") \
        .option("driver", mysql_driver) \
        .csv(data_file, multiLine=True, columnNameOfCorruptRecord='broken',
             encoding='utf-8')  # .option("inferSchema", "true")         # .option("mode", "FAILFAST") \
    df.repartition(20)
    return df


def clean_from_characters(df: DataFrame) -> DataFrame:
    for col in df.columns:
        df = df.withColumn(col, f.regexp_replace(f.col(col), '[*|!|,|-|~|#|$||&|`|+{|}]', ''))  # \
        df = df.withColumn(col, f.regexp_replace(f.col(col), '[\n|\r|\t|\s]', ' '))  # \
        df = df.withColumn(col, f.regexp_replace(f.col(col), 'n/a ', ''))
        df = df.withColumn(col, f.trim(f.col(col)))
    return df


def capitalize_words_and_lowercase(df: DataFrame) -> DataFrame:
    for col in ['additional', 'description', 'title', 'street', 'location', 'micro_location']:
        df = df.withColumn(col, f.initcap(f.lower(f.col(col))))
    return df


def trim_from_spaces(df: DataFrame) -> DataFrame:
    for col in df.columns:
        df = df.withColumn(col, f.trim(f.col(col)))
    return df


@udf(returnType=StringType())
def transliterate_serbian_udf(col):
    return cyrtranslit.to_latin(col, 'sr')


def transliterate_serbian(df: DataFrame) -> DataFrame:
    return df.withColumn('description', transliterate_serbian_udf(f.col('description')))


def transliterate_serbian_geo(df: DataFrame) -> DataFrame:
    return df.withColumn('name', transliterate_serbian_udf(f.col('name'))) \
        .withColumn('suburb', transliterate_serbian_udf(f.col('suburb'))) \
        .withColumn('district', transliterate_serbian_udf(f.col('district'))) \
        .withColumn('formatted', transliterate_serbian_udf(f.col('formatted'))) \
        .withColumn('address_line1', transliterate_serbian_udf(f.col('address_line1'))) \
        .withColumn('address_line2', transliterate_serbian_udf(f.col('address_line2')))


def transliterate_serbian_places(df: DataFrame) -> DataFrame:
    return df.withColumn('name', transliterate_serbian_udf(f.col('name'))) \
        .withColumn('formatted', transliterate_serbian_udf(f.col('formatted'))) \
        .withColumn('address_line1', transliterate_serbian_udf(f.col('address_line1'))) \
        .withColumn('address_line2', transliterate_serbian_udf(f.col('address_line2')))


def extract_columns_for_geoapify(df: DataFrame) -> DataFrame:
    df_for_geocoding = df.select('street', 'micro_location').distinct()
    return df_for_geocoding.toPandas()


def save_file_to_csv(df: DataFrame, file_name: str) -> None:
  #  df.repartition(1).write.option("header", "true").option("sep", ";").mode("overwrite").option('batchsize',
   #                                                                                              '100').csv(file_name)
    df.repartition(1).toPandas().to_csv(file_name, header=True, sep=';', index_col=False)


def get_spark_app_config() -> SparkConf:
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("..\\data_preparation\\spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


halooglasi_schema = StructType([
    StructField('additional', StringType(), nullable=True),
    StructField('city', StringType(), nullable=True),
    StructField('date', StringType(), nullable=True),
    StructField('description', StringType(), nullable=True),
    StructField('floor_number', StringType(), nullable=True),
    StructField('heating_type', StringType(), nullable=True),
    StructField('link', StringType(), nullable=True),
    StructField('location', StringType(), nullable=True),
    StructField('micro_location', StringType(), nullable=True),
    StructField('monthly_bills', FloatType(), nullable=True),
    StructField('number_of_rooms', StringType(), nullable=True),
    StructField('object_state', StringType(), nullable=True),
    StructField('object_type', StringType(), nullable=True),
    StructField('price', FloatType(), nullable=True),
    StructField('price_per_unit', FloatType(), nullable=True),
    StructField('real_estate_type', StringType(), nullable=True),
    StructField('size_in_squared_meters', FloatType(), nullable=True),
    StructField('street', StringType(), nullable=True),
    StructField('title', StringType(), nullable=True),
    StructField('total_number_of_floors', StringType(), nullable=True),
    StructField('w_type', StringType(), nullable=True),
    StructField('is_listed', StringType(), nullable=True),
    StructField('size_metric', StringType(), nullable=True),
    StructField('source', StringType(), nullable=True),

])

real_estate_db_schema = StructType([
    StructField('additional', StringType(), nullable=True),
    StructField('street', StringType(), nullable=True),
    StructField('date', StringType(), nullable=True),
    StructField('description', StringType(), nullable=True),
    StructField('floor_number', StringType(), nullable=True),
    StructField('heating_type', StringType(), nullable=True),
    StructField('link', StringType(), nullable=True),
    StructField('location', StringType(), nullable=True),
    StructField('micro_location', StringType(), nullable=True),
    StructField('monthly_bills', StringType(), nullable=True),
    StructField('number_of_rooms', StringType(), nullable=True),
    StructField('object_state', StringType(), nullable=True),
    StructField('object_type', StringType(), nullable=True),
    StructField('price_per_unit', FloatType(), nullable=True),
    StructField('real_estate_type', StringType(), nullable=True),
    StructField('size_in_squared_meters', FloatType(), nullable=True),
    StructField('title', StringType(), nullable=True),
    StructField('total_number_of_floors', StringType(), nullable=True),
    StructField('w_type', StringType(), nullable=True),
    StructField('is_listed', StringType(), nullable=True),
    StructField('size_metric', StringType(), nullable=True),
    StructField('city', StringType(), nullable=True),
    StructField('source', StringType(), nullable=True),
    StructField('price', FloatType(), nullable=True),

])

place_details_schema = StructType([
    StructField('col', IntegerType(), nullable=True),
    StructField('street_oglasi', StringType(), nullable=True),
    StructField('micro_location_oglasi', StringType(), nullable=True),
    StructField('name', StringType(), nullable=True),
    StructField('city', StringType(), nullable=True),
    StructField('county', StringType(), nullable=True),
    StructField('lon', FloatType(), nullable=True),
    StructField('lat', FloatType(), nullable=True),
    StructField('formatted', StringType(), nullable=True),
    StructField('feature_type', StringType(), nullable=True),
    StructField('address_line1', StringType(), nullable=True),
    StructField('address_line2', StringType(), nullable=True),
    StructField('categories', StringType(), nullable=True),
])

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
