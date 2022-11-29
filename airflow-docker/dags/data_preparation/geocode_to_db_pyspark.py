import sys

from pyspark.sql import functions as f

sys.path.append('..')
from utilities.pyspark_utils import load_data, build_spark_session, read_from_db, save_data_to_db_table, \
    transliterate_serbian_geo


def replace_characters(df, col):
    df = df.withColumn(col, f.regexp_replace(col, 'ä‡', 'ć'))
    df = df.withColumn(col, f.regexp_replace(col, 'ä†', 'ć'))
    df = df.withColumn(col, f.regexp_replace(col, 'äť', 'č'))
    df = df.withColumn(col, f.regexp_replace(col, 'äś', 'č'))
    df = df.withColumn(col, f.regexp_replace(col, 'ä‘', 'đ'))
    df = df.withColumn(col, f.regexp_replace(col, 'ä�', 'đ'))
    df = df.withColumn(col, f.regexp_replace(col, 'ĺˇ', 'š'))
    df = df.withColumn(col, f.regexp_replace(col, 'ĺ ', 'š'))
    df = df.withColumn(col, f.regexp_replace(col, 'ĺľ', 'ž'))
    df = df.withColumn(col, f.regexp_replace(col, 'i;', 'u'))
    df = df.withColumn(col, f.regexp_replace(col, 'ĺ˝', 'ž'))
    df = df.withColumn(col,
                       f.regexp_replace(col, 'ä�inä‘iä‡', 'đinđić'))
    return df


def remove_numeric_characters(df, col):
    df = df.withColumn(col, f.regexp_replace(f.col(col), '[0-9]', ''))
    df = df.withColumn(col, f.regexp_replace(f.col(col), r'[0-9]', ''))
    df = df.withColumn(col, f.regexp_replace(f.col(col), '  ', ' '))
    return df.withColumn(col, f.regexp_replace(f.col(col), '-', ''))


def transliterate_to_latin(df):
    for col in ['name', 'suburb', 'district', 'formatted', 'address_line1', 'address_line2']:
        df = df.withColumn(col,
                           f.when(f.col(col).isNull(), '').otherwise(f.col(col)))
    df.fillna(value='',
              subset=['name', 'suburb', 'district', 'formatted', 'address_line1', 'address_line2']).show()
    df = transliterate_serbian_geo(df)
    return df

def transform_geocode_data():
    spark = build_spark_session()
    conf_out = spark.sparkContext.getConf()
    conf_out.toDebugString()
    ######## Geocode quite simple
    geocoded_df = load_data(spark, 'raw_data\\geoapify\\geocoded.csv')
    geocoded_df = geocoded_df.dropDuplicates(['place_id'])
    geocoded_df = replace_characters(geocoded_df,'street_oglasi')
    geocoded_df = replace_characters(geocoded_df,'micro_location_oglasi')
    geocoded_df = remove_numeric_characters(geocoded_df, 'micro_location_oglasi')
    geocoded_df = remove_numeric_characters(geocoded_df, 'street_oglasi')
    geocoded_df = transliterate_to_latin(geocoded_df)

    save_data_to_db_table(geocoded_df, 'geocode')


if __name__ == "__main__":
    transform_geocode_data()

