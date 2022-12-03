import sys

from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType, DoubleType

sys.path.append('..')
from utilities.pyspark_utils import clean_from_characters, capitalize_words_and_lowercase, trim_from_spaces
from utilities.pyspark_utils import load_posts_data, build_spark_session, save_file_to_csv


def process_halooglasi_spark():
    spark = build_spark_session()

    oglasi_df = load_posts_data(spark, 'raw_data\\scraper\\new\\halooglasi.csv')
    # TODO: remove nakon prve ture
    oglasi_df = oglasi_df.dropDuplicates(['link'])
    oglasi_df = oglasi_df.drop(f.col('city_lines'))
    oglasi_df = oglasi_df.where(f.col('link').isNotNull())
    oglasi_df.show()

    oglasi_df = clean_from_characters(oglasi_df)
    oglasi_df = capitalize_words_and_lowercase(oglasi_df)
    oglasi_df = transform_heating_type(oglasi_df)
    oglasi_df = is_rent_or_sell(oglasi_df)
    oglasi_df = is_property_listed(oglasi_df)
    oglasi_df = clean_monthly_bills(oglasi_df)
    oglasi_df = find_real_estate_type(oglasi_df)
    oglasi_df = transform_size_info(oglasi_df)
    oglasi_df = clean_price(oglasi_df)

    oglasi_df = oglasi_df.withColumn('source', f.lit('halooglasi'))
    oglasi_df = trim_from_spaces(oglasi_df)

    oglasi_df.show()

    save_file_to_csv(oglasi_df, 'raw_data\\scraper\\processed\\halooglasi.csv')

    # df_for_geocoding = place_details.select('street', 'micro_location').distinct()
    # df_for_geocoding.show()
    # df_for_geocoding = df_for_geocoding.toPandas()
    # send_and_receive_geocoding(df_for_geocoding)
    # send_and_receive_place_api()
    # send_and_receive_place_details_api()


def is_rent_or_sell(oglasi_df: DataFrame) -> DataFrame:
    """
    Na osnovu URL-a može se otkriti da li je nekretnina za prodaju
    ili izdavanje.
    :param oglasi_df:DataFrame
    :return: DataFrame
    """
    oglasi_df = oglasi_df.withColumn('w_type', f.when(f.lower(f.col('link')).contains('prodaja'), 'Prodaja') \
                                     .when(f.lower(f.col('link')).contains('izdava'), 'Izdavanje').otherwise(None))
    return oglasi_df


def clean_monthly_bills(df: DataFrame) -> DataFrame:
    """
    Ova kolona ima vrednost u evrima. Čisti se od nenumeričkih karaktera.
    :param df:DataFrame
    :return:DataFrame
    """
    df = df.withColumn('monthly_bills',
                       f.regexp_replace(f.col('monthly_bills'), '[^0-9]', ''))

    df = df.withColumn('monthly_bills',
                       f.when(f.col('monthly_bills').isNotNull() | f.col('monthly_bills').eqNullSafe(''),
                              None).otherwise(f.col('monthly_bills')))
    df = df.withColumn("monthly_bills", df.monthly_bills.cast(DoubleType()))
    return df


def is_property_listed(df: DataFrame) -> DataFrame:
    """
    Iz additional kolone traži informacija o uknjiženosti nekretnine.
    Podrazumevana vrednost je nula.
    :param df:DataFrame
    :return:DataFrame
    """
    df = df.withColumn('is_listed', f.when(f.lower(f.col('additional')).contains('uknj') | \
                                           f.lower(f.col('description')).contains('uknj') | \
                                           f.lower(f.col('title')).contains('uknj'), 1).otherwise(0))
    df = df.withColumn('additional', f.regexp_replace(f.col('additional'), 'Uknjizen', ''))
    return df


def transform_size_info(df: DataFrame) -> DataFrame:
    """
    Postoje dve moguće vrednosti za jedinicu mere: ar i m2. Ta informacija je sadržana u koloni size_in_squared_meters.
    Ta kolona se i čisti od prikrivenih null vrednosti.
    :param df:DataFrame
    :return:DataFrame
    """
    df = df.withColumn('size_metric',
                       f.when(f.col('price_per_unit').contains('ar'), 'ar').otherwise(
                                         'm2'))
    df = df.withColumn('size_in_squared_meters', f.when(f.col('size_in_squared_meters').contains('m'),
                                                        f.trim(
                                                                          f.split(f.col('size_in_squared_meters'), 'm')[
                                                                              0]))
                       .when(f.col('size_in_squared_meters').contains('ar'),
                                           f.trim(f.split(f.col('size_in_squared_meters'), 'ar')[0])).otherwise(
        f.col('size_in_squared_meters')))
    df = df.withColumn('size_in_squared_meters',
                       f.trim(f.regexp_replace(f.col('size_in_squared_meters'), '[^0-9]', '')))

    df = df.withColumn('size_in_squared_meters',
                       f.when(f.col('size_in_squared_meters') == '', f.lit(None)).otherwise(
                                         f.col('size_in_squared_meters')))
    df = df.withColumn("size_in_squared_meters", df.size_in_squared_meters.cast(DoubleType()))
    return df


def clean_price(df: DataFrame) -> DataFrame:
    """
    Iz nepoznatih razloga, u većini slučajeva,
     prilikom procesa prikupljanja podataka, zadnje tri nule nisu bile deo polja.
    Zato se broj ovde množi sa hiljadu.
    Popunjavaju se vrednosti o ceni i ceni na kvadratu gde je to moguće.
    Odstranjuju se svi redovi u kojima nema nikakvog podatka o ceni.
    :param df:DataFrame
    :return:DataFrame
    """
    df = df.withColumn('price_per_unit',
                       f.regexp_replace(f.col('price_per_unit'), r'\D+', ''))

    df = df.withColumn('price',
                       f.when(f.col('price').isNull() & f.col('price_per_unit').isNotNull() & f.col(
                                         'size_in_squared_meters').isNotNull()
                                            , f.col('price_per_unit') * f.col('size_in_squared_meters')).otherwise(
                                         f.col('price')))
    df = df.withColumn('price', f.when(
        f.col('price').cast(FloatType()).__le__(10) & f.col('price_per_unit').isNull(),
        f.col('price').cast(FloatType()) * 1000).otherwise(f.col('price').cast(FloatType())))
    df = df.withColumn('price',
                       f.when(f.col('price') == '', f.lit(None)).otherwise(
                                         f.col('price')))
    df = df.withColumn('price_per_unit',
                       f.when(f.col('price_per_unit') == '', f.lit(None)).otherwise(
                                         f.col('price_per_unit')))
    df = df.withColumn('price_per_unit',
                       f.when(f.col('price_per_unit').isNull() & f.col('price').isNotNull() & f.col(
                                         'size_in_squared_meters').isNotNull(),
                                            f.col('price') / f.col('size_in_squared_meters')).otherwise(
                                         f.col('price_per_unit')))

    df.na.drop("all", subset=['price', 'price_per_unit'])
    return df


def transform_heating_type(df: DataFrame) -> DataFrame:
    df = df.withColumn('floor_number',
                       f.regexp_replace(f.col('floor_number'), 'VRP', '0')) \
        .withColumn('floor_number',
                    f.regexp_replace(f.col('floor_number'), 'SUT', 'Suteren')) \
        .withColumn('floor_number',
                    f.regexp_replace(f.col('floor_number'), 'PSUT', 'Podzemni suteren')) \
        .withColumn('floor_number',
                    f.regexp_replace(f.col('floor_number'), 'VPR', 'Visoko prizemlje')) \
        .withColumn('floor_number',
                    f.regexp_replace(f.col('floor_number'), 'PR', 'Prizemlje')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'CG', 'Centralno grejanje')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'EG', 'Etazno grejanje')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'TA', 'TA pec')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'Klima', 'Klima uredjaj')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'Podno', 'Podno grejanje')) \
        .withColumn('heating_type',
                    f.regexp_replace(f.col('heating_type'), 'Gas', 'Grejanje na gas'))

    df = df.withColumn('heating_type',
                       f.when((f.col('heating_type') == '') | (f.col('heating_type') == '3') | (
                                             f.col('heating_type') == '5') | (f.col('heating_type') == '6'),
                                            f.lit(None)).otherwise(f.col('heating_type')))
    df = df.withColumn('heating_type',
                       f.when(f.col('heating_type') == '', f.lit(None)).otherwise(f.col('heating_type')))
    df = df.withColumn('heating_type',
                       f.when(f.col('heating_type').isNull(), 'Nepoznato').otherwise(
                                         f.col('heating_type')))

    return df


def find_real_estate_type(df: DataFrame) -> DataFrame:
    """
    Iz sačuvanom URL-a se pronalazi tip nekretnine, i postavlja se da bude u skladu sa onim što je
    u bazi podataka.
    :param df:DataFrame
    :return:DataFrame
    """
    df = df.withColumn('real_estate_type', f.when(f.lower(f.col('link')).contains('stanova/'), 'Stan')
                       .when(f.lower(f.col('link')).contains('kuca/'), 'Kuca')
                       .when(f.lower(f.col('link')).contains('soba/'), 'Soba')
                       .when(f.lower(f.col('link')).contains('poslovni-prost'), 'Poslovni objekat')
                       .when(f.lower(f.col('link')).contains('garaza/'), 'Garaze i parking')
                       .when(f.lower(f.col('link')).contains('zemljista/'), 'Zemljiste').otherwise(
        'Ostalo'))
    return df


if __name__ == "__main__":
    process_halooglasi_spark()
