import sys

from pyspark.sql import functions as f
sys.path.append('..')
from utilities.pyspark_utils import load_data, build_spark_session, read_from_db, save_data_to_db_table, \
    transliterate_serbian_places


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
    df = df.withColumn(col, f.regexp_replace(col, 'ä�inä‘iä‡', 'đinđić'))
    return df


def remove_numeric_characters(df, col):
    df = df.withColumn(col, f.regexp_replace(f.col(col), '[0-9]', ''))
    df = df.withColumn(col, f.regexp_replace(f.col(col), r'[0-9]', ''))
    df = df.withColumn(col, f.regexp_replace(f.col(col), '  ', ' '))
    return df.withColumn(col, f.regexp_replace(f.col(col), '-', ''))


def transliterate_to_latin_places(df):
    for col in ['name', 'formatted', 'address_line1', 'address_line2']:
        df = df.withColumn(col,
                           f.when(f.col(col).isNull(), '').otherwise(f.col(col)))
    df.fillna(value='',
              subset=['name', 'formatted', 'address_line1', 'address_line2']).show()
    df = transliterate_serbian_places(df)
    return df


def real_estate_data_to_db():
    global spark, col, x, df, c
    spark = build_spark_session()
    conf_out = spark.sparkContext.getConf()
    conf_out.toDebugString()
    ########   Load places data
    places_df = load_data(spark, 'raw_data\\geoapify\\places.csv')
    places_df = transliterate_to_latin_places(places_df)
    all_columns = []
    for col in places_df.columns:
        all_columns.append(col)
    ####FIND FOREIGN KEY FOR GEOCODE
    geocoded_df = read_from_db(spark, 'geocode')
    # remove any extra spaces, just in case
    places_df = places_df.withColumn('street_oglasi', f.trim(f.lower(f.col('street_oglasi'))))
    geocoded_df = geocoded_df.withColumn('street_oglasi', f.trim(f.lower(f.col('street_oglasi'))))
    places_df = places_df.withColumn('micro_location_oglasi', f.trim(f.lower(f.col('micro_location_oglasi'))))
    geocoded_df = geocoded_df.withColumn('micro_location_oglasi', f.trim(f.lower(f.col('micro_location_oglasi'))))
    # # replace crooked data
    geocoded_df = replace_characters(geocoded_df, 'street_oglasi')
    places_df = replace_characters(places_df, 'street_oglasi')
    geocoded_df = replace_characters(geocoded_df, 'micro_location_oglasi')
    places_df = replace_characters(places_df, 'micro_location_oglasi')
    geocoded_df = remove_numeric_characters(geocoded_df, 'micro_location_oglasi')
    geocoded_df = remove_numeric_characters(geocoded_df, 'street_oglasi')
    places_df = remove_numeric_characters(places_df, 'micro_location_oglasi')
    places_df = remove_numeric_characters(places_df, 'street_oglasi')
    geocoded_df = geocoded_df.withColumn('street_oglasi', f.trim(f.lower(f.col('street_oglasi'))))
    places_df = places_df.withColumn('street_oglasi', f.trim(f.lower(f.col('street_oglasi'))))
    places_df = places_df.withColumn('micro_location_oglasi', f.trim(f.lower(f.col('micro_location_oglasi'))))
    geocoded_df = geocoded_df.withColumn('micro_location_oglasi', f.trim(f.lower(f.col('micro_location_oglasi'))))
    # rename columns in geocoded data so the names don't overlap
    geocoded_df = geocoded_df.select(*(f.col(x).alias(x + '_geo') for x in geocoded_df.columns))
    # join the data frames
    df = places_df.join(geocoded_df, (f.col('street_oglasi').eqNullSafe(f.col('street_oglasi_geo'))) &
                        (f.col('micro_location_oglasi').eqNullSafe(f.col('micro_location_oglasi_geo'))), how='left')
    df = df.withColumnRenamed('geocode_id_geo', 'geocode_id')
    df = df.dropDuplicates(['_c0'])
    # pick columns to write to the db
    all_columns.remove('street_oglasi')
    all_columns.remove('micro_location_oglasi')
    all_columns.append('geocode_id')
    all_columns.remove('_c0')
    # all_columns.append('street_oglasi_geo')
    # all_columns.append('micro_location_oglasi_geo')
    #############SAVE TO THE DATABASE
    places_df = df.select(all_columns).filter(f.col('geocode_id').isNotNull())
    places_df.select([f.count(f.when(f.isnull(c), c)).alias(c) for c in ['geocode_id']]).show()
    # place_details.select(['street_oglasi_geo','micro_location_oglasi_geo','street_oglasi','micro_location_oglasi']).filter(f.isnull(f.col('geocode_id'))).distinct().show(
    #     10000000, truncate=False)
    # place_details.show()
    save_data_to_db_table(places_df, 'place')


if __name__ == "__main__":
    real_estate_data_to_db()
