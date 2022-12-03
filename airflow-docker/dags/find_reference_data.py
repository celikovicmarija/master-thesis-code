import sys

from pyspark.pandas import DataFrame
from pyspark.sql import functions as f

from utilities.pyspark_utils import load_data_pandas, clean_from_characters, save_file_to_csv, load_data
from utilities.pyspark_utils import save_data_to_db_table, read_from_db, build_spark_session, halooglasi_schema


def load_scraper_data_to_db(file: str = None):
    if not file:
        file = sys.argv[1]
    spark = build_spark_session()
    # oglasi_df = load_data(spark, f'/opt/airflow/data/raw_data/scraper/processed/{file}.csv')
    oglasi_df = load_data(spark, f'/opt/airflow/data/raw_data/scraper/processed/{file}.csv')
    oglasi_df = oglasi_df.select(
        [f.when(f.col(c) == "NaN", None).otherwise(f.col(c)).alias(c) for c in oglasi_df.columns])

    for col in oglasi_df.columns:
        oglasi_df = clean_up_latin_letters(col, oglasi_df)
    spark.sql('set spark.sql.legacy.timeParserPolicy=LEGACY')

    oglasi_df = oglasi_df.withColumnRenamed('size_in_squared_meters', 'size')
    oglasi_df = clean_up_columns(oglasi_df, 'street')
    oglasi_df = clean_up_columns(oglasi_df, 'micro_location')

    all_columns = []
    for col in oglasi_df.columns:
        all_columns.append(col)

    oglasi_df = oglasi_df.withColumn('date', f.to_date(f.col('date'), 'dd/MM/yyyy').alias('date'))

    #############FIND FOREIGN KEY FOR GEOCODE

    geocoded_df = read_from_db(spark, 'geocode')
    geocoded_df = clean_from_characters(geocoded_df)

    geocoded_df = geocoded_df.dropDuplicates(['street_oglasi', 'micro_location_oglasi'])

    for col in ['micro_location_oglasi', 'street_oglasi']:
        geocoded_df = clean_up_latin_letters(col, geocoded_df)

    geocoded_df = clean_up_columns(geocoded_df, 'street_oglasi')
    geocoded_df = clean_up_columns(geocoded_df, 'micro_location_oglasi')
    geocoded_df = geocoded_df.withColumnRenamed('city', 'city_geocoded')

    df = oglasi_df.join(geocoded_df, (f.col('street').eqNullSafe(f.col('street_oglasi')) &
                                     (f.col('micro_location').eqNullSafe(f.col('micro_location_oglasi')))) | (
                            f.col('street').eqNullSafe(f.col('street_oglasi')))
                        , how='left')
    # | (f.col('micro_location').eqNullSafe(f.col('micro_location_oglasi')))
    df = df.dropDuplicates(['link'])
    all_columns.append('geocode_id')

    ############# FIND FOREIGN KEY FOR HEATING TYPE

    heating_type_df = read_from_db(spark, 'heating_type')

    df = df.withColumn('heating_type', f.trim(f.lower(f.col('heating_type'))))
    heating_type_df = heating_type_df.withColumn('heating_type_name', f.trim(f.lower(f.col('heating_type_name'))))
    df = df.withColumnRenamed('heating_type', 'heating_type_string')
    df = df.join(heating_type_df, f.col('heating_type_string') == f.col('heating_type_name'), how='left')
    df = df.dropDuplicates(['link'])
    all_columns.append('heating_type_id')
    df = df.drop('heating_type_string')
    all_columns.remove('heating_type')

    ############# FIND FOREIGN KEY FOR TRANSACTION TYPE

    transaction_type_df = read_from_db(spark, 'transaction_type')

    df = df.withColumn('w_type', f.trim(f.lower(f.col('w_type'))))
    transaction_type_df = transaction_type_df.withColumn('transaction_type_name',
                                                         f.trim(f.lower(f.col('transaction_type_name'))))
    df = df.join(transaction_type_df, f.col('w_type') == f.col('transaction_type_name'), how='left')
    df = df.dropDuplicates(['link'])
    all_columns.append('transaction_type_id')
    df = df.drop('w_type')
    all_columns.remove('w_type')

    ############# FIND FOREIGN KEY FOR REAL ESTATE TYPE

    real_estate_type_df = read_from_db(spark, 'real_estate_type')

    df = df.withColumn('real_estate_type', f.trim(f.lower(f.col('real_estate_type'))))
    real_estate_type_df = real_estate_type_df.withColumn('real_estate_type_name',
                                                         f.trim(f.lower(f.col('real_estate_type_name'))))
    df = df.withColumnRenamed('real_estate_type', 'real_estate_type_string')
    df = df.join(real_estate_type_df, f.col('real_estate_type_string') == f.col('real_estate_type_name'), how='left')
    df = df.dropDuplicates(['link'])
    all_columns.append('real_estate_type_id')
    df = df.drop('real_estate_type_string')
    all_columns.remove('real_estate_type')

    ############# FIND FOREIGN KEY FOR SOURCE

    source_df = read_from_db(spark, 'source')

    df = df.withColumn('source', f.trim(f.lower(f.col('source'))))
    source_df = source_df.withColumn('source_name', f.trim(f.lower(f.col('source_name'))))
    df = df.withColumnRenamed('source', 'source_string')
    df = df.join(source_df, f.col('source_string') == f.col('source_name'), how='left')
    df = df.dropDuplicates(['link'])
    all_columns.append('source_id')
    df = df.drop('source_string')
    all_columns.remove('source')

    ############# FIND FOREIGN KEY SIZE METRIC

    size_metric_df = read_from_db(spark, 'size_measurement')

    df = df.withColumn('size_metric', f.trim(f.lower(f.col('size_metric'))))
    size_metric_df = size_metric_df.withColumn('measurement_name', f.trim(f.lower(f.col('measurement_name'))))
    df = df.withColumnRenamed('size_metric', 'size_metric_string')
    df = df.join(size_metric_df, f.col('size_metric_string') == f.col('measurement_name'), how='left')
    df = df.dropDuplicates(['link'])
    all_columns.append('measurement_id')
    df = df.drop('size_metric_string')
    all_columns.remove('size_metric')

    ############# SAVE TO THE DATABASE
    oglasi_df = df.select(all_columns)
    save_file_to_csv(oglasi_df, f'/opt/airflow/data/raw_data/scraper/ready_for_db/{file}.csv')

    # save_data_to_db_table(oglasi_df, 'real_estate_post')


def clean_up_latin_letters(col: str, df: DataFrame) -> DataFrame:
    """
    Ova funkcija iz zadatih polona čisti specifična slova srpske latinice.
    Zadavalo je problema u prošlosti...
    :param col:str
    :param df:DataFrame
    :return:DataFrame
    """
    df = df.withColumn(col, f.regexp_replace(f.col(col), 'č', 'c'))
    df = df.withColumn(col, f.regexp_replace(f.col(col), 'ć', 'c'))
    df = df.withColumn(col, f.regexp_replace(f.col(col), 'ž', 'z'))
    df = df.withColumn(col, f.regexp_replace(f.col(col), 'š', 's'))
    df = df.withColumn(col, f.regexp_replace(f.col(col), 'đ', 'dj'))
    return df


def clean_up_columns(df: DataFrame, col) -> DataFrame:
    """
    Očisti za slučaj da je negde nešto ostalo neočišćeno.
    :param df:DataFrame
    :param col:
    :return:DataFrame
    """
    df = df.withColumn(col, f.trim(f.lower(f.col(col))))
    df = df.withColumn(col, f.trim(f.lower(f.col(col))))
    return df


# load_scraper_data_to_db(file='halooglasi')
#
# if __name__ == "__main__":
#     load_scraper_data_to_db(file='halooglasi')
load_scraper_data_to_db()