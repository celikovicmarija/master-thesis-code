import os
import sys

from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType

from utilities.pyspark_utils import clean_from_characters, capitalize_words_and_lowercase, trim_from_spaces, \
    save_file_to_csv, load_posts_data, build_spark_session

sys.path.append('data_preparation')


def process_oglasi_pyspark():
    try:
        spark = build_spark_session()

        conf_out = spark.sparkContext.getConf()
        conf_out.toDebugString()

        # oglasi_df = load_data(spark, '/opt/airflow/data/raw_data/scraper/new/oglasi.csv')
        oglasi_df = load_posts_data(spark, '/opt/airflow/data/raw_data/scraper/new/oglasi.csv')
        oglasi_df = oglasi_df.dropDuplicates(['link'])
        oglasi_df = oglasi_df.where(f.col('link').isNotNull())

        oglasi_df = clean_from_characters(oglasi_df)
        oglasi_df = capitalize_words_and_lowercase(oglasi_df)
        oglasi_df = oglasi_df.withColumn('floor_number',
                                         f.regexp_replace(f.col('floor_number'), '. sprat', ''))

        oglasi_df = transform_heating_type(oglasi_df)

        oglasi_df = oglasi_df.withColumn('w_type', f.when(f.lower(f.col('link')).contains('prodaja'), 'Prodaja') \
                                         .when(f.lower(f.col('link')).contains('izdava'), 'Izdavanje').otherwise(None))

        oglasi_df = is_the_property_listed(oglasi_df)
        oglasi_df = transform_number_of_rooms(oglasi_df)
        oglasi_df = find_real_estate_type_oglasi(oglasi_df)

        oglasi_df = transform_size_info(oglasi_df)
        oglasi_df = transform_location_info(oglasi_df)
        oglasi_df = clean_price(oglasi_df)

        oglasi_df = oglasi_df.withColumn('source', f.lit('oglasi'))
        oglasi_df = trim_from_spaces(oglasi_df)
        oglasi_df = clean_from_characters(oglasi_df)
        oglasi_df = oglasi_df.withColumn("price", oglasi_df.price.cast(DoubleType()))
        oglasi_df = oglasi_df.withColumn("price_per_unit", oglasi_df.price_per_unit.cast(DoubleType()))
        oglasi_df = oglasi_df.withColumn("monthly_bills", oglasi_df.monthly_bills.cast(DoubleType()))

        save_file_to_csv(oglasi_df, '/opt/airflow/data/raw_data/scraper/processed/oglasi')
        # save_file_to_csv(oglasi_df, 'data_preparation/raw_data/scraper/processed/oglasi.csv')
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


def transform_size_info(oglasi_df: DataFrame) -> DataFrame:
    """
    Određuje se jedinica mere i čisti se vrednost za veličinu nekretnine
    :param oglasi_df:
    :return:
    """
    oglasi_df = oglasi_df.withColumn('size_metric',
                                     f.when(f.col('size_in_squared_meters').contains('a'), 'ar').otherwise(
                                         'm2'))
    oglasi_df = oglasi_df.withColumn('size_in_squared_meters',
                                     f.trim(f.regexp_replace(f.col('size_in_squared_meters'), '[^0-9]', '')))

    oglasi_df = oglasi_df.withColumn('size_in_squared_meters',
                                     f.when(f.col('size_in_squared_meters') == '', f.lit(None)).otherwise(
                                         f.col('size_in_squared_meters')))

    return oglasi_df


def is_the_property_listed(oglasi_df: DataFrame) -> DataFrame:
    """
    Iz nekoliko kolona pokušava se pronaći informacija o uknjiženosti nekretnine.
    Podrazumevana vrednost je nula.
    :param oglasi_df:
    :return:
    """
    return oglasi_df.withColumn('is_listed', f.when(f.lower(f.col('additional')).contains('uknji') | \
                                                    f.lower(f.col('description')).contains('uknji') | \
                                                    f.lower(f.col('title')).contains('uknji'), 1).otherwise(f.lit(0)))


def clean_price(oglasi_df: DataFrame) -> DataFrame:
    """
    Odstranjuju se višak nule :cena ima na kraju ekstra ' 00',
    kao i tačka za razdvajanje hiljada.
    Računa se izvedena vrednost za price_per_unit ako su podaci dostupni.
    Odstranjuju se svi redovi u kojima nema nikakvog podatka o ceni.
    :param oglasi_df:
    :return:
    """
    oglasi_df = oglasi_df.withColumn('size_in_squared_meters',
                                     f.regexp_replace(f.col('size_in_squared_meters'), 'm2', ''))
    oglasi_df = oglasi_df.withColumn("price", f.expr("substring(price, 1, length(price)-3)"))
    oglasi_df = oglasi_df.withColumn('price', f.regexp_replace(f.col('price'), '\.', ''))
    oglasi_df = oglasi_df.withColumn('price_per_unit',
                                     f.when(f.col('price_per_unit').isNull() & f.col('price').isNotNull()
                                            & f.col('size_in_squared_meters').isNotNull(),
                                            f.col('price') / f.col('size_in_squared_meters')).otherwise(
                                         f.col('price_per_unit')))

    oglasi_df.na.drop("all", subset=['price', 'price_per_unit'])
    return oglasi_df


def transform_location_info(oglasi_df: DataFrame) -> DataFrame:
    """
    Postoji više različitih slučajeva:
        - Centar (Spens) (Srbija  Novi Sad  Novi Sad)
        - Popovica (Srbija  Novi Sad  Sremska Kamenica)
        - Čortanovci (Srbija  Inđija)
        - Ada Ciganlija (Srbija  Beograd  Čukarica)
        Odatle se uočava sledeća struktura Mikro lokacija (Država, Grad, Opština)
        Kod u nastavku  izvlači vrednosti za te kolone.

    :param oglasi_df:
    :return: df
    """
    oglasi_df = oglasi_df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), 'srbija  ', '')))
    oglasi_df = oglasi_df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), 'crna gora  ', '')))
    oglasi_df = oglasi_df.withColumn('micro_location', f.when(f.size(f.split(f.col('location'), '\\(')).__ge__(3),
                                                              f.trim(f.regexp_replace(
                                                                  f.split(f.trim(f.col('location')), '\\)')[0], '\\(',
                                                                  ''))).otherwise(
        f.trim(f.split(f.trim(f.col('location')), '\\(')[0])))
    oglasi_df = oglasi_df.withColumn('location', f.expr("replace(location,micro_location,'')"))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace(f.col('location'), '  ', ' '))
    oglasi_df = oglasi_df.withColumn('location', f.regexp_replace(f.col('location'), '(/(|/))', ''))


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
    return oglasi_df


def transform_heating_type(oglasi_df: DataFrame) -> DataFrame:
    """
    Standardizuju se nazivi polja za grejanje sa onim što postoji u bazi podataka.
    :param oglasi_df:
    :return:
    """
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Gradsko', 'Centralno grejanje'))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Kaljeva', 'Kaljeva pec'))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Na struju', 'Grejanje na struju'))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Podno', 'Podno grejanje'))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Etažno', 'Etazno grejanje'))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Klima', 'Klima uredjaj'))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.regexp_replace(f.col('heating_type'), 'Gas', 'Etazno grejanje na gas'))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.when(f.col('heating_type') == '', f.lit(None)).otherwise(
                                         f.col('heating_type')))
    oglasi_df = oglasi_df.withColumn('heating_type',
                                     f.when(f.col('heating_type').isNull(), 'Nepoznato').otherwise(
                                         f.col('heating_type')))
    return oglasi_df


def transform_number_of_rooms(oglasi_df: DataFrame) -> DataFrame:
    """
    Standardizuju se nazivi polja za broj soba sa onim što postoji u bazi podataka.
    :param oglasi_df:
    :return:
    """
    return oglasi_df.withColumn('number_of_rooms',
                                f.when(f.lower(f.col('number_of_rooms')).contains('jednosoban'), '1')
                                .when(f.lower(f.col('number_of_rooms')).contains('garsonjera'), '1')
                                .when(f.lower(f.col('number_of_rooms')).contains('jednoiposoban'), '1.5')
                                .when(f.lower(f.col('number_of_rooms')).contains('dvosoban'), '2')
                                .when(f.lower(f.col('number_of_rooms')).contains('dvoiposoban'), '2.5')
                                .when(f.lower(f.col('number_of_rooms')).contains('trosoban'), '3')
                                .when(f.lower(f.col('number_of_rooms')).contains('troiposoban'), '3.5')
                                .when(f.lower(f.col('number_of_rooms')).contains('četvorosoban'), '4+')
                                .otherwise(f.col('number_of_rooms')))


def find_real_estate_type_oglasi(oglasi_df: DataFrame) -> DataFrame:
    """
    Iz sačuvanom URL-a se pronalazi tip nekretnine, i postavlja se da bude u skladu sa onim što je
    u bazi podataka.
    :param oglasi_df:
    :return:
    """
    return oglasi_df.withColumn('real_estate_type', f.when(f.lower(f.col('link')).contains('-stanova'), 'Stan')
                                .when(f.lower(f.col('link')).contains('-kuca'), 'Kuca')
                                .when(f.lower(f.col('link')).contains('-poslovni-prost'), 'Poslovni objekat')
                                .when(f.lower(f.col('link')).contains('-garaza'), 'Garaze i parking')
                                .when(f.lower(f.col('link')).contains('-placeva'), 'Zemljiste')
                                .when(f.lower(f.col('link')).contains('-soba'), 'Soba').otherwise('Ostalo'))


# process_oglasi_pyspark()


if __name__ == "__main__":
    process_oglasi_pyspark()
