from pyspark.sql import *
from pyspark.sql import functions as f

from utilities.geoapify import send_and_receive_place_details_api, send_and_receive_place_api, \
    send_and_receive_geocoding
from utilities.logger import Log4j
from utilities.pyspark_utils import clean_from_characters, capitalize_words_and_lowercase, trim_from_spaces
from utilities.pyspark_utils import load_posts_data
from utilities.pyspark_utils import transliterate_serbian, extract_columns_for_geoapify, save_file_to_csv


from utilities.pyspark_utils import clean_from_characters, capitalize_words_and_lowercase, trim_from_spaces



if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Oglasi Spark") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Spark")

    conf_out = spark.sparkContext.getConf()
    oglasi_df = load_posts_data(spark, 'real_estate_scraper/nekretnine.csv')
    # logger.info(oglasi_df.show(100))
    oglasi_df = oglasi_df.dropDuplicates(['link'])

    oglasi_df=clean_from_characters(oglasi_df)
    oglasi_df=capitalize_words_and_lowercase(oglasi_df)

    # logger.info(oglasi_df.select('real_estate_type').distinct().show(100, truncate=False))
    oglasi_df = oglasi_df.withColumn('price', f.regexp_replace(f.col('title'), r' EUR', ''))
    oglasi_df = oglasi_df.withColumn('price', f.regexp_replace(f.col('title'), r',00', ''))
    oglasi_df = oglasi_df.withColumn('description', f.regexp_replace(f.col('description'), 'Opis', ''))
    oglasi_df = oglasi_df.withColumn('description', f.regexp_replace(f.col('description'), '[^0-9a-zA-Z: ]', ''))

    oglasi_df = oglasi_df.withColumn('title', f.regexp_replace(f.col('title'), '[^a-zA-Z0-9 :]', ''))
    oglasi_df = oglasi_df.withColumn('size_metric',
                                     f.when(f.col('size_in_squared_meters').contains('ar'), 'ar').otherwise(
                                         'm2'))
    oglasi_df = oglasi_df.withColumn('size_in_squared_meters',
                                     f.trim(f.regexp_replace(f.col('size_in_squared_meters'), '[^0-9]', '')))
    oglasi_df = oglasi_df.withColumn('price', f.regexp_replace(f.col('price'), '[^0-9]', ''))
    oglasi_df = oglasi_df.withColumn('price_per_unit', f.regexp_replace(f.col('price_per_unit'), '[^0-9]', ''))

    oglasi_df = oglasi_df.withColumn('floor_number',
                                     f.when(f.lower(f.col('floor_number')).contains('prizemlje'), '0')
                                     .when(f.lower(f.col('floor_number')).contains('visoko prizemlje'), '0') \
                                     .when(f.lower(f.col('floor_number')).contains('suteren'), '-1') \
                                     .when(f.col('floor_number').rlike(r'\d'),
                                           f.regexp_extract(f.col('floor_number'), r'\d', 0)).otherwise(
                                         f.col('floor_number')))

    oglasi_df = oglasi_df.withColumn('number_of_rooms',
                                   f.when((f.lower(f.col('title')).contains('jednosoban') |f.lower(f.col('title')).contains('garsonjera') | f.lower(f.col('real_estate_type')).contains('jednosoban')) & f.col(
                                         'number_of_rooms').isNull(), '1')
                                     .when((f.lower(f.col('title')).contains('jednoiposoban') | f.lower(
                                       f.col('real_estate_type')).contains('jednoiposoban')) & f.col(
                                       'number_of_rooms').isNull(), '1.5')
                                     .when((f.lower(f.col('title')).contains('dvosoban') | f.lower(
                                       f.col('real_estate_type')).contains('dvosoban')) & f.col(
                                       'number_of_rooms').isNull(), '2')
                                     .when((f.lower(f.col('title')).contains('dvoiposoban') | f.lower(
                                       f.col('real_estate_type')).contains('dvoiposoban')) & f.col(
                                       'number_of_rooms').isNull(), '2.5')
                                     .when((f.lower(f.col('title')).contains('trosoban') | f.lower(
                                       f.col('real_estate_type')).contains('trosoban')) & f.col(
                                       'number_of_rooms').isNull(), '3')
                                     .when((f.lower(f.col('title')).contains('troiposoban') | f.lower(
                                       f.col('real_estate_type')).contains('troiposoban')) & f.col(
                                       'number_of_rooms').isNull(), '3.5')
                                     .when((f.lower(f.col('title')).contains('četvorosoban') | f.lower(
                                       f.col('real_estate_type')).contains('četvorosoban')) & f.col(
                                       'number_of_rooms').isNull(), '4+')
                                     .when((f.lower(f.col('title')).contains('Petosoban') | f.lower(
                                       f.col('real_estate_type')).contains('Petosoban')) & f.col(
                                       'number_of_rooms').isNull(), '4+')
                                     .otherwise(f.col('number_of_rooms')))

    oglasi_df = oglasi_df.withColumn('real_estate_type', f.when(f.lower('real_estate_type').contains('stan'), 'Stan') \
                                     .when(f.lower('real_estate_type').contains('kuća'), 'Kuća') \
                                     .when(f.lower('real_estate_type').contains('garsonjer'), 'Stan') \
                                     .when(f.lower('real_estate_type').contains('poslovn'), 'Poslovni objekat') \
                                     .when(f.lower('real_estate_type').contains('kanc'), 'Poslovni objekat') \
                                     .when(f.lower('real_estate_type').contains('garaž'), 'Garaže i parking') \
                                     .when(f.lower('real_estate_type').contains('zemlj'), 'Zemljište') \
                                     .when(f.lower('real_estate_type').contains('sob'), 'Sobe').otherwise('Ostalo'))

    okruzi = ['Grad Beograd', 'Borski', 'Braničevski', 'Zaječarski', 'Zapadno-bački', 'Zlatiborski', 'Jablanički',
              'Južno-banatski', 'Južno-bački', 'Kolubarski', 'Kosovski', 'Kosovko-mitrovački', 'Mačvanski', 'Moravički',
              'Nišavski', 'Pečki', 'Pirotski', 'Podunavski', 'Pomoravski', 'Prizrenski', 'Pčinjski', 'Rasinski',
              'Raški', 'Severno-banatski', 'Severno-bački', 'Srednjo-banatski', 'Sremski', 'Toplički', 'Šumadijski']

    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace(f.col('location'), 'Srbija', ''))
    two_word_city_names = ['Novi Sad', 'Herceg Novi', 'Stari Banovci', 'Vrnjačka Banja']
    oglasi_df = oglasi_df.withColumn('temp_loc',
                                     f.when(f.col('location').contains('Grad Beograd'), 'Beograd') \
                                     .when(f.col('location').contains('Borski'), 'Borski okrug') \
                                     .when(f.col('location').contains('Braničevski'), 'Braničevski okrug') \
                                     .when(f.col('location').contains('Zaječarski'), 'Zaječarski okrug') \
                                     .when(f.col('location').contains('Zapadno-bački'), 'Zapadno-bački okrug') \
                                     .when(f.col('location').contains('Zlatiborski'), 'Zlatiborski okrug') \
                                     .when(f.col('location').contains('Jablanički'), 'Jablanički okrug') \
                                     .when(f.col('location').contains('Južno-banatski'), 'Južno-banatski okrug') \
                                     .when(f.col('location').contains('Južno-bački'), 'Južno-bački okrug') \
                                     .when(f.col('location').contains('Kolubarski'), 'Kolubarski okrug') \
                                     .when(f.col('location').contains('Kosovski'), 'Kosovski okrug') \
                                     .when(f.col('location').contains('Kosovko-mitrovački'), 'Kosovko-mitrovački okrug') \
                                     .when(f.col('location').contains('Mačvanski'), 'Mačvanski okrug') \
                                     .when(f.col('location').contains('Moravički'), 'Moravički okrug') \
                                     .when(f.col('location').contains('Nišavski'), 'Nišavski okrug') \
                                     .when(f.col('location').contains('Pečki'), 'Pečki okrug') \
                                     .when(f.col('location').contains('Pirotski'), 'Pirotski okrug') \
                                     .when(f.col('location').contains('Podunavski'), 'Podunavski okrug') \
                                     .when(f.col('location').contains('Pomoravski'), 'Pomoravski okrug') \
                                     .when(f.col('location').contains('Prizrenski'), 'Prizrenski okrug') \
                                     .when(f.col('location').contains('Pčinjski'), 'Pčinjski okrug') \
                                     .when(f.col('location').contains('Rasinski'), 'Rasinski okrug') \
                                     .when(f.col('location').contains('Raški'), 'Raški okrug') \
                                     .when(f.col('location').contains('Severno-banatski'), 'Severno-banatski okrug') \
                                     .when(f.col('location').contains('Severno-bački'), 'Severno-bački okrug') \
                                     .when(f.col('location').contains('Srednjo-banatski'), 'Srednjo-banatski okrug') \
                                     .when(f.col('location').contains('Severno-banatski'), 'Srednjo-banatski okrug') \
                                     .when(f.col('location').contains('Sremski'), 'Sremski okrug') \
                                     .when(f.col('location').contains('Toplički'), 'Toplički okrug') \
                                     .when(f.col('location').contains('Šumadijski'), 'Šumadijski okrug').otherwise(f.lit(None)))

    for okrug in okruzi:
        oglasi_df = oglasi_df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), okrug, '')))

    for grad in two_word_city_names:
        oglasi_df = oglasi_df.withColumn('city',
                                         f.when(f.col('location').rlike(grad), grad).otherwise(
                                             f.trim(f.split(f.col('location'), r' ')[0])))

    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'Novi Beograd', 'NBGD'))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'Beograd na vodi', 'BGWF'))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'Novi Sad', 'NS'))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'Novi Sad', 'NS'))
    oglasi_df = oglasi_df.withColumn('location', f.trim(f.expr("replace(location,city,'')")))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'NBGD', 'Novi Beograd'))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'NS', ''))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'BGWF', 'Beograd na vodi'))
    oglasi_df = oglasi_df.drop('micro_location')
    oglasi_df = oglasi_df.withColumnRenamed('location', 'micro_location')
    oglasi_df = oglasi_df.withColumnRenamed('temp_loc', 'location').drop('temp_loc')
    oglasi_df = oglasi_df.withColumn('is_listed', f.when(f.col('additional').contains('Uknjiženo:Da'), 1).otherwise(f.lit(0)))
    oglasi_df = oglasi_df.withColumn('additional', f.regexp_replace('additional', 'Uknjiženo:Da', ''))
    oglasi_df = oglasi_df.withColumn('additional', f.trim('additional'))
    oglasi_df=oglasi_df.withColumn('source',f.lit('nekretnine'))
    oglasi_df = trim_from_spaces(oglasi_df)


    logger.info(oglasi_df.show())
    save_file_to_csv(oglasi_df, 'nekretnine_clean.csv')

    df_for_geocoding = extract_columns_for_geoapify(oglasi_df)
    send_and_receive_geocoding(df_for_geocoding)
    send_and_receive_place_api()
    send_and_receive_place_details_api()