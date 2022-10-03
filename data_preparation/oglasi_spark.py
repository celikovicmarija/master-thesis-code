from pyspark.sql import *
from pyspark.sql import functions as f

from utilities.geoapify import send_and_receive_place_api, send_and_receive_place_details_api
from utilities.logger import Log4j
from utilities.pyspark_utils import clean_from_characters, capitalize_words_and_lowercase, trim_from_spaces
from utilities.pyspark_utils import extract_columns_for_geoapify
from utilities.pyspark_utils import load_posts_data

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Oglasi Spark") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Spark")

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    oglasi_df = load_posts_data(spark, 'real_estate_scraper/oglasi.csv')
    oglasi_df = oglasi_df.dropDuplicates(['link'])
    logger.info(oglasi_df.show(500))

    oglasi_df = clean_from_characters(oglasi_df)
    oglasi_df = capitalize_words_and_lowercase(oglasi_df)

    oglasi_df = oglasi_df.withColumn('floor_number',
                                     f.when(f.lower(f.col('floor_number')).contains('prizemlje'), '0')
                                     .when(f.lower(f.col('floor_number')).contains('visoko prizemlje'), '0') \
                                     .when(f.lower(f.col('floor_number')).contains('suteren'), '-1') \
                                     .when(f.lower(f.col('floor_number')).contains('dvorišni'), '0') \
                                     .when(f.col('floor_number').rlike(r'\d'),
                                           f.regexp_extract(f.col('floor_number'), r'\d', 0)).otherwise(
                                         f.col('floor_number')))

    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Gradsko', 'Centralno grejanje'))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Etažno', 'Etažno grejanje'))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Gas', 'Etažno grejanje na gas'))

    oglasi_df = oglasi_df.withColumn('w_type', f.when(f.lower(f.col('link')).contains('prodaja'), 'Prodaja') \
                                     .when(f.lower(f.col('link')).contains('izdava'), 'Izdavanje').otherwise(None))

    oglasi_df = oglasi_df.withColumn('is_listed', f.when(f.lower(f.col('additional')).contains('uknji') | \
                                                         f.lower(f.col('description')).contains('uknji') | \
                                                         f.lower(f.col('title')).contains('uknji'), 1).otherwise(0))

    oglasi_df = oglasi_df.withColumn('number_of_rooms',
                                     f.when(f.lower(f.col('number_of_rooms')).contains('jednosoban'), '1')
                                     .when(f.lower(f.col('number_of_rooms')).contains('garsonjera'), '1')
                                     .when(f.lower(f.col('number_of_rooms')).contains('jednoiposoban'), '1.5')
                                     .when(f.lower(f.col('number_of_rooms')).contains('dvosoban'), '2')
                                     .when(f.lower(f.col('number_of_rooms')).contains('dvoiposoban'), '2.5')
                                     .when(f.lower(f.col('number_of_rooms')).contains('trosoban'), '3')
                                     .when(f.lower(f.col('number_of_rooms')).contains('troiposoban'), '3.5')
                                     .when(f.lower(f.col('number_of_rooms')).contains('četvorosoban'), '4+')
                                     .otherwise(f.col('number_of_rooms')))

    oglasi_df = oglasi_df.withColumn('real_estate_type', f.when(f.lower(f.col('link')).contains('-stanova'), 'Stan')
                                     .when(f.lower(f.col('link')).contains('-kuca'), 'Kuća')
                                     .when(f.lower(f.col('link')).contains('-poslovni-prost'), 'Poslovni objekat')
                                     .when(f.lower(f.col('link')).contains('-garaza'), 'Garaže i parking')
                                     .when(f.lower(f.col('link')).contains('-placeva'), 'Zemljište')
                                     .when(f.lower(f.col('link')).contains('-soba'), 'Soba').otherwise('Ostalo'))

    oglasi_df = oglasi_df.withColumn('size_metric',
                                     f.when(f.col('size_in_squared_meters').contains('ar'), 'ar').otherwise(
                                         'm2'))
    oglasi_df = oglasi_df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), 'srbija  ', '')))
    oglasi_df = oglasi_df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), 'crna gora  ', '')))
    oglasi_df = oglasi_df.withColumn('micro_location', f.when(f.size(f.split(f.col('location'), '\\(')).__ge__(3),
                                                              f.trim(f.regexp_replace(
                                                                  f.split(f.trim(f.col('location')), '\\)')[0], '\\(',
                                                                  ''))).otherwise(
        f.trim(f.split(f.trim(f.col('location')), '\\(')[0])))

    oglasi_df = oglasi_df.withColumn('location', f.expr("replace(location,micro_location,'')"))
    two_word_city_names = ['Novi Sad', 'Herceg Novi', 'Stari Banovci', 'Vrnjačka Banja', 'Sremska Kamenica',
                           'Veliko Gradište', 'Sremska Mitrovica', 'Stari Grad', 'Sremski Karlovci', 'Novi Beograd',
                           'Savski Venac', 'Stara Pazova']
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace(f.col('location'), '  ', ' '))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace(f.col('location'), '\\(|\\)', ''))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'Novi Beograd', 'NBGD')) \
        .withColumn('location', f.regexp_replace('location', 'Novi Sad', 'NS')) \
        .withColumn('location', f.regexp_replace('location', 'Herceg Novi', 'HN')) \
        .withColumn('location', f.regexp_replace('location', 'Stari Banovci', 'SB')) \
        .withColumn('location', f.regexp_replace('location', 'Sremska Kamenica', 'SremskaKamenica')) \
        .withColumn('location', f.regexp_replace('location', 'Vrnjačka Banja', 'VB')) \
        .withColumn('location', f.regexp_replace('location', 'Veliko Gradište', 'VG')) \
        .withColumn('location', f.regexp_replace('location', 'Sremska Mitrovica', 'SM')) \
        .withColumn('location', f.regexp_replace('location', 'Stari Grad', 'SG')) \
        .withColumn('location', f.regexp_replace('location', 'Sremski Karlovci', 'SK')) \
        .withColumn('location', f.regexp_replace('location', 'Savski Venac', 'SV')) \
        .withColumn('location', f.regexp_replace('location', 'Stara Pazova', 'SP'))

    oglasi_df = oglasi_df.withColumn('city', f.split(f.trim(f.col('location')), ' ')[0])
    oglasi_df = oglasi_df.withColumn('location', f.split(f.trim(f.col('location')), ' ')[1])

    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace('location', 'NBGD', 'Novi Beograd')) \
        .withColumn('location', f.regexp_replace('location', 'NS', 'Novi Sad')) \
        .withColumn('location', f.regexp_replace('location', 'HN', 'Herceg Novi')) \
        .withColumn('location', f.regexp_replace('location', 'SB', 'Stari Banovci')) \
        .withColumn('location', f.regexp_replace('location', 'SremskaKamenica', 'Sremska Kamenica')) \
        .withColumn('location', f.regexp_replace('location', 'VB', 'Vrnjačka Banja')) \
        .withColumn('location', f.regexp_replace('location', 'VG', 'Veliko Gradište')) \
        .withColumn('location', f.regexp_replace('location', 'SM', 'Sremska Mitrovica')) \
        .withColumn('location', f.regexp_replace('location', 'SG', 'Stari Grad')) \
        .withColumn('location', f.regexp_replace('location', 'SK', 'Sremski Karlovci')) \
        .withColumn('location', f.regexp_replace('location', 'SV', 'Savski Venac')) \
        .withColumn('location', f.regexp_replace('location', 'SP', 'Stara Pazova'))

    oglasi_df = oglasi_df.withColumn('city', f.regexp_replace('city', 'NBGD', 'Novi Beograd')) \
        .withColumn('city', f.regexp_replace('city', 'NS', 'Novi Sad')) \
        .withColumn('city', f.regexp_replace('city', 'HN', 'Herceg Novi')) \
        .withColumn('city', f.regexp_replace('city', 'SB', 'Stari Banovci')) \
        .withColumn('city', f.regexp_replace('city', 'SremskaKamenica', 'Sremska Kamenica')) \
        .withColumn('city', f.regexp_replace('city', 'VB', 'Vrnjačka Banja')) \
        .withColumn('city', f.regexp_replace('city', 'VG', 'Veliko Gradište')) \
        .withColumn('city', f.regexp_replace('city', 'SM', 'Sremska Mitrovica')) \
        .withColumn('city', f.regexp_replace('city', 'SG', 'Stari Grad')) \
        .withColumn('city', f.regexp_replace('city', 'SK', 'Sremski Karlovci')) \
        .withColumn('city', f.regexp_replace('city', 'SV', 'Savski Venac')) \
        .withColumn('city', f.regexp_replace('city', 'SP', 'Stara Pazova'))

    oglasi_df = oglasi_df.withColumn('city', f.when((f.trim('city')) == '', f.lit(None)).otherwise(f.col('city')))

    oglasi_df = oglasi_df.withColumn('source', f.lit('oglasi'))
    oglasi_df = trim_from_spaces(oglasi_df)
    logger.info(oglasi_df.show(500))

    # save_file_to_csv(oglasi_df, 'oglasi_clean.csv')
    #
    df_for_geocoding = extract_columns_for_geoapify(oglasi_df)
    # send_and_receive_geocoding(df_for_geocoding)
    # send_and_receive_place_api()
    send_and_receive_place_details_api()
