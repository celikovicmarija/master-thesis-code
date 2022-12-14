import os
import sys

from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType

from utilities.pyspark_utils import clean_from_characters, capitalize_words_and_lowercase, trim_from_spaces
from utilities.pyspark_utils import load_posts_data, build_spark_session, save_file_to_csv


def process_nekretnine_pyspark():
    try:
        spark = build_spark_session()
        nekretnine_df = load_posts_data(spark, '/opt/airflow/data/raw_data/scraper/new/nekretnine.csv')
        # nekretnine_df = load_posts_data(spark, 'data_preparation\\raw_data\\scraper\\new\\nekretnine.csv')

        nekretnine_df = nekretnine_df.dropDuplicates(['link'])
        nekretnine_df = nekretnine_df.where(f.col('link').isNotNull())

        nekretnine_df = clean_from_characters(nekretnine_df)
        nekretnine_df = capitalize_words_and_lowercase(nekretnine_df)
        nekretnine_df = clean_price(nekretnine_df)
        nekretnine_df = clean_description(nekretnine_df)

        nekretnine_df = transform_heating_type(nekretnine_df)

        nekretnine_df = nekretnine_df.withColumn('title', f.regexp_replace(f.col('title'), '[^a-zA-Z0-9 :]', ''))

        nekretnine_df = transform_property_size_data(nekretnine_df)

        nekretnine_df = nekretnine_df.withColumn('floor_number', f.initcap('floor_number'))

        nekretnine_df = find_number_of_rooms(nekretnine_df)
        nekretnine_df = find_real_estate_type_nekretnine(nekretnine_df)
        nekretnine_df = extract_location_data(nekretnine_df)
        nekretnine_df = is_property_listed(nekretnine_df)
        nekretnine_df = trim_from_spaces(nekretnine_df)
        nekretnine_df = clean_from_characters(nekretnine_df)

        nekretnine_df = nekretnine_df.withColumn('source', f.lit('nekretnine'))

        nekretnine_df = nekretnine_df.withColumn("price", nekretnine_df.price.cast(DoubleType()))
        nekretnine_df = nekretnine_df.withColumn("price_per_unit", nekretnine_df.price_per_unit.cast(DoubleType()))
        nekretnine_df = nekretnine_df.withColumn("monthly_bills", nekretnine_df.monthly_bills.cast(DoubleType()))

        save_file_to_csv(nekretnine_df, '/opt/airflow/data/raw_data/scraper/processed/nekretnine.csv')
        # save_file_to_csv(nekretnine_df, 'data_preparation\\raw_data\\scraper\\processed\\nekretnine.csv')

        # df_for_geocoding = extract_columns_for_geoapify(place_details)
        # send_and_receive_geocoding(df_for_geocoding)
        # send_and_receive_place_api()
        # send_and_receive_place_details_api()
    except Exception as e:
        print('*******************************************')
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print('*******************************************')
        raise Exception()


def transform_heating_type(nekretnine_df: DataFrame) -> DataFrame:
    """
    Standardizuju se nazivi polja za grejanje sa onim ??to postoji u bazi podataka.
    :param df:
    :return:
    """
    nekretnine_df = nekretnine_df.withColumn('heating_type',
                                             f.regexp_replace(f.col('heating_type'), 'Klima ure??aj', 'Klima uredjaj'))
    nekretnine_df = nekretnine_df.withColumn('heating_type',
                                             f.when(f.col('heating_type') == '', f.lit(None)).otherwise(
                                                 f.col('heating_type')))
    nekretnine_df = nekretnine_df.withColumn('heating_type',
                                             f.when(f.col('heating_type').isNull(), 'Nepoznato').otherwise(
                                                 f.col('heating_type')))
    return nekretnine_df


def transform_property_size_data(nekretnine_df: DataFrame) -> DataFrame:
    """
    Postoje dve mogu??e vrednosti: ar i m2. Ta informacija je sadr??ana u koloni size_in_squared_meters
    Ta kolona se i ??isti od prikrivenih null vrednosti.
    :param nekretnine_df:
    :return:
    """
    nekretnine_df = nekretnine_df.withColumn('size_metric',
                                             f.when(f.col('size_in_squared_meters').contains('ar'), 'ar').otherwise(
                                                 'm2'))

    nekretnine_df = nekretnine_df.withColumn('size_in_squared_meters',
                                             f.trim(f.regexp_replace(f.col('size_in_squared_meters'), '[^0-9]', '')))

    nekretnine_df = nekretnine_df.withColumn('size_in_squared_meters',
                                             f.when(f.col('size_in_squared_meters') == '', f.lit(None)).otherwise(
                                                 f.col('size_in_squared_meters')))
    return nekretnine_df


def is_property_listed(nekretnine_df: DataFrame) -> DataFrame:
    """
    Iz additional kolone tra??i informacija o uknji??enosti nekretnine.
    Podrazumevana vrednost je nula.
    :param df:
    :return:
    """

    nekretnine_df = nekretnine_df.withColumn('is_listed',
                                             f.when(f.col('additional').contains('Uknji??eno:da'), 1).otherwise(
                                                 f.lit(0)))
    nekretnine_df = nekretnine_df.withColumn('additional', f.regexp_replace('additional', 'Uknji??eno:da', ''))
    nekretnine_df = nekretnine_df.withColumn('additional', f.trim('additional'))
    return nekretnine_df


def clean_description(nekretnine_df):
    nekretnine_df = nekretnine_df.withColumn('description', f.regexp_replace(f.col('description'), 'Opis', ''))
    nekretnine_df = nekretnine_df.withColumn('description',
                                             f.regexp_replace(f.col('description'), '[^0-9a-zA-Z: ]', ''))
    return nekretnine_df


def clean_price(nekretnine_df: DataFrame) -> DataFrame:
    """
    Odstranjuju se vi??ak nule :cena ima na kraju ekstra ' ,00',
    kao i valuta. Odstranjuju se svi redovi u kojima nema nikakvog podatka o ceni.
    :param df:
    :return:
    """
    nekretnine_df = nekretnine_df.withColumn('price', f.regexp_replace(f.col('price'), r' EUR', ''))
    nekretnine_df = nekretnine_df.withColumn('price', f.regexp_replace(f.col('price'), r',00', ''))
    nekretnine_df = nekretnine_df.withColumn('price', f.regexp_replace(f.col('price'), '[^0-9]', ''))
    nekretnine_df = nekretnine_df.withColumn('price_per_unit', f.regexp_replace(f.col('price_per_unit'), '[^0-9]', ''))
    nekretnine_df = nekretnine_df.withColumn('price_per_unit',
                                             f.when(f.col('price_per_unit') == '', f.lit(None)).otherwise(
                                                 f.col('price_per_unit')))

    nekretnine_df = nekretnine_df.withColumn('price',
                                             f.when(f.col('price') == '', f.lit(None)).otherwise(
                                                 f.col('price')))
    nekretnine_df.na.drop("all", subset=['price', 'price_per_unit'])
    return nekretnine_df


def find_real_estate_type_nekretnine(nekretnine_df: DataFrame) -> DataFrame:
    """
    Iz sa??uvanom URL-a se pronalazi tip nekretnine, i postavlja se da bude u skladu sa onim ??to je
    u bazi podataka.
    :param df:
    :return:
    """
    return nekretnine_df.withColumn('real_estate_type', f.when(f.lower('real_estate_type').contains('stan'), 'Stan') \
                                    .when(f.lower('real_estate_type').contains('ku??a'), 'Kuca') \
                                    .when(f.lower('real_estate_type').contains('garsonjer'), 'Stan') \
                                    .when(f.lower('real_estate_type').contains('poslovn'), 'Poslovni objekat') \
                                    .when(f.lower('real_estate_type').contains('kanc'), 'Poslovni objekat') \
                                    .when(f.lower('real_estate_type').contains('gara??'), 'Garaze i parking') \
                                    .when(f.lower('real_estate_type').contains('zemlj'), 'Zemljiste') \
                                    .when(f.lower('real_estate_type').contains('sob'), 'Soba').otherwise('Ostalo'))


def extract_location_data(nekretnine_df: DataFrame) -> DataFrame:
    """
    I na ovom sajtu su  podaci o lokaciji navedeni na druga??iji na??in:
        - Srbija Grad Beograd Beograd Mirijevo I Kapetana Milo??a ??unji??a
        - Srbija Grad Beograd Mladenovac (varo??) Ravni??arska
        - Srbija ??umadijski Aran??elovac Veliko polje
        - Srbija Ju??no-ba??ki Novi Sad Bistrica
        Jasno je da sa leva na desno raste specifi??nost. Na osnovu toga, ova funkcija
        pronalazi podatke o gradu, lokaciji i mikro-lokaciji.
    :param nekretnine_df:
    :return:
    """
    okruzi = ['Grad Beograd', 'Borski', 'Brani??evski', 'Zaje??arski', 'Zapadno-ba??ki', 'Zlatiborski', 'Jablani??ki',
              'Ju??no-banatski', 'Ju??no-ba??ki', 'Kolubarski', 'Kosovski', 'Kosovko-mitrova??ki', 'Ma??vanski', 'Moravi??ki',
              'Ni??avski', 'Pe??ki', 'Pirotski', 'Podunavski', 'Pomoravski', 'Prizrenski', 'P??injski', 'Rasinski',
              'Ra??ki', 'Severno-banatski', 'Severno-ba??ki', 'Srednjo-banatski', 'Sremski', 'Topli??ki', '??umadijski']
    nekretnine_df = nekretnine_df.withColumn('location', f.regexp_replace(f.col('location'), 'Srbija', ''))
    two_word_city_names = ['Novi Sad', 'Herceg Novi', 'Stari Banovci', 'Vrnja??ka Banja']
    nekretnine_df = nekretnine_df.withColumn('temp_loc',
                                             f.when(f.col('location').contains('Grad Beograd'), 'Beograd') \
                                             .when(f.col('location').contains('Borski'), 'Borski okrug') \
                                             .when(f.col('location').contains('Brani??evski'), 'Brani??evski okrug') \
                                             .when(f.col('location').contains('Zaje??arski'), 'Zaje??arski okrug') \
                                             .when(f.col('location').contains('Zapadno-ba??ki'), 'Zapadno-ba??ki okrug') \
                                             .when(f.col('location').contains('Zlatiborski'), 'Zlatiborski okrug') \
                                             .when(f.col('location').contains('Jablani??ki'), 'Jablani??ki okrug') \
                                             .when(f.col('location').contains('Ju??no-banatski'), 'Ju??no-banatski okrug') \
                                             .when(f.col('location').contains('Ju??no-ba??ki'), 'Ju??no-ba??ki okrug') \
                                             .when(f.col('location').contains('Kolubarski'), 'Kolubarski okrug') \
                                             .when(f.col('location').contains('Kosovski'), 'Kosovski okrug') \
                                             .when(f.col('location').contains('Kosovko-mitrova??ki'),
                                                   'Kosovko-mitrova??ki okrug') \
                                             .when(f.col('location').contains('Ma??vanski'), 'Ma??vanski okrug') \
                                             .when(f.col('location').contains('Moravi??ki'), 'Moravi??ki okrug') \
                                             .when(f.col('location').contains('Ni??avski'), 'Ni??avski okrug') \
                                             .when(f.col('location').contains('Pe??ki'), 'Pe??ki okrug') \
                                             .when(f.col('location').contains('Pirotski'), 'Pirotski okrug') \
                                             .when(f.col('location').contains('Podunavski'), 'Podunavski okrug') \
                                             .when(f.col('location').contains('Pomoravski'), 'Pomoravski okrug') \
                                             .when(f.col('location').contains('Prizrenski'), 'Prizrenski okrug') \
                                             .when(f.col('location').contains('P??injski'), 'P??injski okrug') \
                                             .when(f.col('location').contains('Rasinski'), 'Rasinski okrug') \
                                             .when(f.col('location').contains('Ra??ki'), 'Ra??ki okrug') \
                                             .when(f.col('location').contains('Severno-banatski'),
                                                   'Severno-banatski okrug') \
                                             .when(f.col('location').contains('Severno-ba??ki'), 'Severno-ba??ki okrug') \
                                             .when(f.col('location').contains('Srednjo-banatski'),
                                                   'Srednjo-banatski okrug') \
                                             .when(f.col('location').contains('Severno-banatski'),
                                                   'Srednjo-banatski okrug') \
                                             .when(f.col('location').contains('Sremski'), 'Sremski okrug') \
                                             .when(f.col('location').contains('Topli??ki'), 'Topli??ki okrug') \
                                             .when(f.col('location').contains('??umadijski'),
                                                   '??umadijski okrug').otherwise(
                                                 f.lit(None)))
    for okrug in okruzi:
        nekretnine_df = nekretnine_df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), okrug, '')))
    for grad in two_word_city_names:
        nekretnine_df = nekretnine_df.withColumn('city',
                                                 f.when(f.col('location').rlike(grad), grad).otherwise(
                                                     f.trim(f.split(f.col('location'), r' ')[0])))
    nekretnine_df = nekretnine_df.withColumn('location', f.regexp_replace('location', 'Novi Beograd', 'NBGD'))
    nekretnine_df = nekretnine_df.withColumn('location', f.regexp_replace('location', 'Beograd na vodi', 'BGWF'))
    nekretnine_df = nekretnine_df.withColumn('location', f.regexp_replace('location', 'Novi Sad', 'NS'))
    nekretnine_df = nekretnine_df.withColumn('location', f.regexp_replace('location', 'Novi Sad', 'NS'))
    nekretnine_df = nekretnine_df.withColumn('location', f.trim(f.expr("replace(location,city,'')")))
    nekretnine_df = nekretnine_df.withColumn('location', f.regexp_replace('location', 'NBGD', 'Novi Beograd'))
    nekretnine_df = nekretnine_df.withColumn('location', f.regexp_replace('location', 'NS', ''))
    nekretnine_df = nekretnine_df.withColumn('location', f.regexp_replace('location', 'BGWF', 'Beograd na vodi'))
    nekretnine_df = nekretnine_df.drop('micro_location')
    nekretnine_df = nekretnine_df.withColumnRenamed('location', 'micro_location')
    nekretnine_df = nekretnine_df.withColumnRenamed('temp_loc', 'location').drop('temp_loc')
    return nekretnine_df


def find_number_of_rooms(nekretnine_df: DataFrame) -> DataFrame:
    '''
    Standardizuju se nazivi polja za broj soba sa onim ??to postoji u bazi podataka.
    :param df:
    :return:
    '''
    return nekretnine_df.withColumn('number_of_rooms',
                                    f.when((f.lower(f.col('title')).contains('jednosoban') | f.lower(
                                        f.col('title')).contains('garsonjera') | f.lower(
                                        f.col('real_estate_type')).contains('jednosoban')) & f.col(
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
                                    .when((f.lower(f.col('title')).contains('??etvorosoban') | f.lower(
                                        f.col('real_estate_type')).contains('??etvorosoban')) & f.col(
                                        'number_of_rooms').isNull(), '4+')
                                    .otherwise(f.col('number_of_rooms').cast('string')))



# process_nekretnine_pyspark()

if __name__ == "__main__":
    process_nekretnine_pyspark()
