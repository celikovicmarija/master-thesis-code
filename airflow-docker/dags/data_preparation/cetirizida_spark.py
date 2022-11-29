import sys

from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType

sys.path.append('..')
from utilities.pyspark_utils import clean_from_characters, capitalize_words_and_lowercase, trim_from_spaces
from utilities.pyspark_utils import load_posts_data, build_spark_session, save_file_to_csv


def process_cetirizida_spark():
    spark = build_spark_session()

    oglasi_df = load_posts_data(spark, 'raw_data\\scraper\\new\\cetirizida.csv')
    oglasi_df = oglasi_df.dropDuplicates(['link'])
    oglasi_df = oglasi_df.drop(f.col('city_lines'))
    oglasi_df = oglasi_df.where(f.col('link').isNotNull())

    oglasi_df.show()

    oglasi_df = clean_from_characters(oglasi_df)
    oglasi_df = capitalize_words_and_lowercase(oglasi_df)

    oglasi_df = oglasi_df.withColumn('number_of_rooms', f.regexp_replace(f.col('number_of_rooms'), '[^0-9.]', ''))

    oglasi_df = oglasi_df.withColumn('w_type',
                                     f.when(f.lower(f.col('link')).contains('prodaja'), 'Prodaja')
                                     .when(f.lower(f.col('link')).contains('izdava'), 'Izdavanje').otherwise(None))

    oglasi_df = is_property_listed(oglasi_df)

    oglasi_df = tranform_heating_type(oglasi_df)

    oglasi_df = extract_real_estate_type(oglasi_df)

    oglasi_df = oglasi_df.withColumn('size_metric',
                                     f.when(f.col('size_in_squared_meters').contains('a'), 'ar').otherwise(
                                         'm2'))
    oglasi_df = oglasi_df.withColumn('size_in_squared_meters', f.split(f.col('size_in_squared_meters'), 'm²')[0])

    oglasi_df = oglasi_df.withColumn('total_number_of_floors', f.when(f.col('floor_number').contains('/'),
                                                                      f.split(f.col('floor_number'), r'/')[
                                                                          1]).otherwise(
        f.col('total_number_of_floors')))

    oglasi_df = oglasi_df.withColumn('floor_number', f.when(f.col('floor_number').contains('/'),
                                                            f.split(f.col('floor_number'), r'/')[
                                                                0]).otherwise(f.col('floor_number')))

    oglasi_df = oglasi_df.withColumn('additional', f.trim(f.expr("replace(additional,total_number_of_floors,'')")))

    oglasi_df = extract_location_data(oglasi_df)
    oglasi_df = oglasi_df.withColumn('object_state', f.col('object_type'))
    oglasi_df = oglasi_df.withColumn('object_type', f.lit(None))
    oglasi_df = oglasi_df.withColumn('source', f.lit('cetirizida'))

    oglasi_df = extract_price_data(oglasi_df)

    oglasi_df = extract_property_size_information(oglasi_df)

    oglasi_df = clean_number_of_floors(oglasi_df)

    oglasi_df = trim_from_spaces(oglasi_df)

    oglasi_df.show()
    save_file_to_csv(oglasi_df, 'raw_data\\scraper\\processed\\cetirizida.csv')
    # # df_for_geocoding = extract_columns_for_geoapify(place_details)
    # # send_and_receive_geocoding(df_for_geocoding)
    # # send_and_receive_place_api()
    # # send_and_receive_place_details_api()


def extract_location_data(df: DataFrame) -> DataFrame:
    """
    U koloni location, podaci su na primer:
        - Kalenić  Vračar  Beograd
        - Mirijevo  Zvezdara opština  Beograd
        (od mikrolokacije do grada), stoga je odatle moguće popuniti i druge kolone.
    :param df:
    :return:
    """
    df = df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), 'Gradske lokacije', '')))
    df = df.withColumn('location', f.trim(f.regexp_replace(f.col('location'), 'Okolne lokacije', '')))

    df = df.withColumn('location', f.regexp_replace(f.col('location'), '\s+', ' '))
    df = df.withColumn('temp', f.when(f.col('location').endswith('Novi Sad'), 'Novi Sad')
                       .when(f.col('location').endswith('Herceg Novi'), 'Herceg Novi')
                       .when(f.col('location').endswith('Stari Banovci'), 'Stari Banovci')
                       .when(f.col('location').endswith('Vrnjačka Banja'), 'Vrnjačka Banja')
                       .otherwise(f.element_at(f.split(f.trim(f.col('location')), ' '), -1)))
    df = df.drop('street').withColumnRenamed('city', 'street')
    df = df.withColumnRenamed('temp', 'city').drop('temp')
    df = df.withColumn('location', f.regexp_replace('location', 'Novi Beograd', 'NBGD'))
    df = df.withColumn('location', f.trim(f.expr("replace(location,city,'')")))
    df = df.withColumn('location', f.regexp_replace('location', 'NBGD', 'Novi Beograd'))
    return df


def extract_property_size_information(df: DataFrame) -> DataFrame:
    df = df.withColumn('size_in_squared_meters',
                       f.trim(f.regexp_replace(f.col('size_in_squared_meters'), ':00 AM', 'a')))
    df = df.withColumn('size_in_squared_meters',
                       f.trim(f.regexp_replace(f.col('size_in_squared_meters'), 'AM', 'a')))
    df = df.withColumn('size_in_squared_meters',
                       f.trim(f.regexp_replace(f.col('size_in_squared_meters'), ':00', '')))
    df = df.withColumn('size_in_squared_meters', f.when(f.col('size_in_squared_meters').contains('m²'),
                                                        f.trim(
                                                            f.split(f.col('size_in_squared_meters'),
                                                                    'm²')[
                                                                0]))
                       .when(f.col('size_in_squared_meters').contains('a'),
                             f.trim(f.split(f.col('size_in_squared_meters'), 'a')[0])).otherwise(
        f.col('size_in_squared_meters')))
    return df


def clean_number_of_floors(df: DataFrame) -> DataFrame:
    """
    Otklanjaju se standarni sufiksi (mada je moglo i pomoću regularnih izraza)
    :param df:
    :return:
    """
    df = df.withColumn('total_number_of_floors',
                       f.trim(f.regexp_replace(f.col('total_number_of_floors'), 'sprata', '')))
    df = df.withColumn('total_number_of_floors',
                       f.trim(f.regexp_replace(f.col('total_number_of_floors'), 'spratova', '')))
    return df


def extract_price_data(df: DataFrame) -> DataFrame:
    """
    S obzirom da je sama kolona cene neklasicno popunjena, sa nedostatkom varirajućeg broja nula,
    i s obzirom da ova informacija jeste sadržana u koloni title, odatle se i izvlači
    i popunjava kolona za cenu po jedinici. Ako nije zadata ni jedna od ova dva polja,
    taj red se odbacuje.
    :param df:
    :return:
    """
    df = df.withColumn('price_temp', f.expr(f"regexp_extract(title, '[.,0-9]+€',0)"))
    df = df.withColumn('price_temp', f.regexp_replace('price_temp', '€', ''))
    df = df.withColumn('price_temp', f.regexp_replace('price_temp', ',', ''))
    df = df.withColumn('price_temp', f.regexp_replace('price_temp', '.', ''))
    df = df.drop('price')
    df = df.withColumnRenamed('price_temp', 'price')
    df = df.withColumn("size_in_squared_meters", df.size_in_squared_meters.cast(DoubleType()))
    df = df.withColumn('price_per_unit',
                       f.when(f.col('price').isNotNull() & f.col('size_in_squared_meters').isNotNull(),
                              f.round(
                                  f.col('price') / f.col('size_in_squared_meters'), 2)).otherwise(
                           f.col('price_per_unit')))
    df = df.withColumn("price", df.price.cast(DoubleType()))
    df = df.withColumn("price_per_unit", df.price_per_unit.cast(DoubleType()))
    df.na.drop("all", subset=['price', 'price_per_unit'])

    df = df.withColumn('monthly_bills',
                       f.when(f.col('monthly_bills').isNotNull() | f.col('monthly_bills').eqNullSafe(''),
                              f.lit(None)).otherwise(f.col('monthly_bills')))
    return df


def is_property_listed(oglasi_df):
    """
    Iz kolene opisa može se pronaći informacija o uknjiženosti nekretnine.
    Podrazumevana vrednost je nula.
    :param oglasi_df:
    :return:
    """
    oglasi_df = oglasi_df.withColumn('is_listed',
                                     f.when(f.lower(f.col('description')).contains('uknj'), 1).otherwise(0))
    return oglasi_df


def tranform_heating_type(df: DataFrame) -> DataFrame:
    """
    Standardizuju se nazivi polja za grejanje sa onim što postoji u bazi podataka.
    :param df:
    :return:
    """
    df = df.withColumn('heating_type', f.regexp_replace(f.col('heating_type'), 'ž', 'z'))
    df = df.withColumn('heating_type', f.regexp_replace(f.col('heating_type'), 'š', 's'))
    df = df.withColumn('heating_type', f.regexp_replace(f.col('heating_type'), 'ć', 'c'))
    df = df.withColumn('heating_type', f.concat(f.upper(f.expr("substring(heating_type, 1, 1)")),
                                                f.lower(f.expr("substring(heating_type, 2)"))))
    df = df.withColumn('heating_type',
                       f.when(f.col('heating_type') == '', f.lit(None)).otherwise(
                           f.col('heating_type')))
    df = df.withColumn('heating_type',
                       f.when(f.col('heating_type').isNull(), 'Nepoznato').otherwise(
                           f.col('heating_type')))
    return df


def extract_real_estate_type(df: DataFrame) -> DataFrame:
    """
    Iz sačuvanom URL-a se pronalazi tip nekretnine, i postavlja se da bude u skladu sa onim što je
    u bazi podataka.
    :param df:
    :return:
    """

    df = df.withColumn('real_estate_type', f.when(f.lower(f.col('link')).contains('/stanovi'), 'Stan')
                       .when(f.lower(f.col('link')).contains('/kuce'), 'Kuca')
                       .when(f.lower(f.col('link')).contains('/poslovni-prost'), 'Poslovni objekat')
                       .when(f.lower(f.col('link')).contains('/sob'), 'Soba')
                       .when(f.lower(f.col('link')).contains('/garaz'), 'Garaze i parking')
                       .when(f.lower(f.col('link')).contains('/plac'), 'Zemljiste').otherwise('Ostalo'))
    return df


if __name__ == "__main__":
    process_cetirizida_spark()
