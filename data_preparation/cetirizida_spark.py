from pyspark.sql import *
from pyspark.sql import functions as f

from utilities.geoapify import send_and_receive_place_details_api, send_and_receive_place_api, \
    send_and_receive_geocoding
from utilities.logger import Log4j
from utilities.pyspark_utils import clean_from_characters, capitalize_words_and_lowercase, trim_from_spaces
from utilities.pyspark_utils import load_posts_data
from utilities.pyspark_utils import transliterate_serbian, extract_columns_for_geoapify, save_file_to_csv

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Oglasi Spark") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Spark")

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    oglasi_df = load_posts_data(spark, 'real_estate_scraper/cetirizida.csv')
    oglasi_df = oglasi_df.dropDuplicates(['link'])

    oglasi_df = clean_from_characters(oglasi_df)
    oglasi_df = capitalize_words_and_lowercase(oglasi_df)

    oglasi_df = oglasi_df.withColumn('number_of_rooms', f.regexp_replace(f.col('number_of_rooms'), '[^0-9.]', ''))

    oglasi_df = oglasi_df.withColumn('w_type', f.when(f.lower(f.col('link')).contains('prodaja'), 'Prodaja') \
                                     .when(f.lower(f.col('link')).contains('izdava'), 'Izdavanje').otherwise(None))

    oglasi_df = oglasi_df.withColumn('is_listed',
                                     f.when(f.lower(f.col('description')).contains('uknj'), 1).otherwise(0))

    oglasi_df = oglasi_df.withColumn('heating_type', f.concat(f.upper(f.expr("substring(heating_type, 1, 1)")),
                                                              f.lower(f.expr("substring(heating_type, 2)"))))

    oglasi_df = oglasi_df.withColumn('real_estate_type', f.when(f.lower(f.col('link')).contains('/stanovi'), 'Stan')
                                     .when(f.lower(f.col('link')).contains('/kuce'), 'Kuća')
                                     .when(f.lower(f.col('link')).contains('/poslovni-prost'), 'Poslovni objekat')
                                     .when(f.lower(f.col('link')).contains('/garaz'), 'Garaže i parking')
                                     .when(f.lower(f.col('link')).contains('/plac'), 'Zemljište').otherwise('Ostalo'))

    oglasi_df = oglasi_df.withColumn('size_metric',
                                     f.when(f.col('size_in_squared_meters').contains('ar'), 'ar').otherwise(
                                         'm2'))
    oglasi_df = oglasi_df.withColumn('size_in_squared_meters', f.split(f.col('size_in_squared_meters'), 'm²')[0])

    oglasi_df = oglasi_df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), 'Gradske lokacije', '')))
    oglasi_df = oglasi_df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), 'Okolne lokacije', '')))

    oglasi_df = oglasi_df.withColumn('total_number_of_floors', f.when(f.col('floor_number').contains('/'),
                                                                      f.split(f.col('floor_number'), r'/')[
                                                                          1]).otherwise(f.col('total_number_of_floors')))

    oglasi_df = oglasi_df\
        .withColumn('floor_number',f.when(f.lower(f.col('floor_number')).contains('prizemlje'), '0')\
            .when(f.lower(f.col('floor_number')).contains('visoko prizemlje'), '0')\
            .when(f.lower(f.col('floor_number')).contains('suteren'), '-1').otherwise(f.col('floor_number')))



    oglasi_df = oglasi_df.withColumn('floor_number', f.when(f.col('floor_number').contains('/'),
                                                                      f.split(f.col('floor_number'), r'/')[
                                                                          0]).otherwise(f.col('floor_number')))
    oglasi_df = oglasi_df.withColumn('floor_number', f.regexp_replace(f.col('floor_number'), '[^0-9|-]', ''))

    oglasi_df = oglasi_df.withColumn('additional', f.trim(f.expr("replace(additional,total_number_of_floors,'')")))


    two_word_city_names = ['Novi Sad', 'Herceg Novi','Stari Banovci', 'Vrnjačka Banja']

    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace(f.col('location'), '\s+', ' '))
    oglasi_df = oglasi_df.withColumn('temp', f.when(f.col('location').endswith('Novi Sad'), 'Novi Sad')
                                     .when(f.col('location').endswith('Herceg Novi'), 'Herceg Novi')
                                     .when(f.col('location').endswith('Stari Banovci'), 'Stari Banovci')
                                     .when(f.col('location').endswith('Vrnjačka Banja'), 'Vrnjačka Banja')
                                     .otherwise(f.element_at(f.split(f.trim(f.col('location')), ' '), -1)))

    oglasi_df = oglasi_df.drop('street').withColumnRenamed('city', 'street')
    oglasi_df = oglasi_df.withColumnRenamed('temp', 'city').drop('temp')

    oglasi_df = oglasi_df.withColumn('description',
                                     f.when(f.col('description').isNull(), '').otherwise(f.col('description')))

    oglasi_df = transliterate_serbian(oglasi_df)

    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'Novi Beograd', 'NBGD'))
    oglasi_df = oglasi_df.withColumn('location', f.trim(f.expr("replace(location,city,'')")))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'NBGD', 'Novi Beograd'))
    oglasi_df = oglasi_df.withColumn('object_state', f.col('object_type'))
    oglasi_df = oglasi_df.withColumn('object_type', f.lit(None))
    oglasi_df = oglasi_df.withColumn('source', f.lit('cetirizida'))
    oglasi_df = trim_from_spaces(oglasi_df)

    logger.info(oglasi_df.show())
    save_file_to_csv(oglasi_df, 'cetiri_zida_clean.csv')

    df_for_geocoding = extract_columns_for_geoapify(oglasi_df)
    send_and_receive_geocoding(df_for_geocoding)
    send_and_receive_place_api()
    send_and_receive_place_details_api()
