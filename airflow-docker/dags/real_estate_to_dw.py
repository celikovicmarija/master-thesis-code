import sys

from pyspark.pandas import DataFrame
from pyspark.sql import functions as f

from utilities.pyspark_utils import save_data_to_dw_table, read_from_db, read_from_dw, build_spark_session


def find_latest_date_id_in_dw(fact_real_estate_dw: DataFrame) -> int:
    max_date = fact_real_estate_dw.select(f.max(f.col('date_id')).alias('max_date')).collect()[0]['max_date']
    # joined_df = interest_rate_dw.join(dim_date_dw, interest_rate_dw.date_id == dim_date_dw.date_id, how='inner')
    return max_date


def refresh_dw_with_newest_real_estate_data(spark, latest_date: int, real_estate_post_cdc: DataFrame) -> None:
    dim_date_dw = read_from_dw(spark, 'dim_date')
    dim_municipality_dw = read_from_dw(spark, 'dim_municipality')
    dim_geocode_dw = read_from_dw(spark, 'dim_municipality')
    joined_df = dim_date_dw.join(real_estate_post_cdc, dim_date_dw.fulldate == real_estate_post_cdc.exchange_date,
                                 how='inner').filter(f.col('date_id') > latest_date)

    dim_property_df = add_new_property_to_dw(spark, real_estate_post_cdc)
    joined_df = joined_df.join(dim_property_df, dim_property_df.link == joined_df.link, how='left')
    joined_df = joined_df.join(dim_geocode_dw, dim_geocode_dw.geocode_id == joined_df.geocode_id, how='left')

    joined_df = joined_df.join(dim_municipality_dw, joined_df.formatted.contains(dim_municipality_dw.municipality_name),
                               how='left')
    joined_df = joined_df.join(dim_municipality_dw, joined_df.district.contains(dim_municipality_dw.municipality_name),
                               how='left')

    new_rows_to_add = joined_df.select(f.col('date_id'), f.col('price'), f.col('price_per_unit'), f.col('property_id'),
                                       f.col('source_id'), f.col('heating_type_id'),
                                       f.col('geocode_id'), f.col('transaction_type_id'),
                                       f.col('property_type_id'), f.col('municipality_id'))
    new_rows_to_add.show()
    save_data_to_dw_table(new_rows_to_add, 'fact_financial_indicators')


def add_new_property_to_dw(spark, real_estate_post_cdc: DataFrame) -> DataFrame:
    """
    Property data needs to exist in the dimension table before it is used in the fact table.
    :return:
    """
    size_measurement_df = read_from_db(spark, 'size_measurement')
    real_estate_post_cdc = real_estate_post_cdc.join(size_measurement_df,
                                                     size_measurement_df.measurement_id == real_estate_post_cdc.measurement_id,
                                                     how='left')

    new_properties = real_estate_post_cdc.select(f.col('link'),
                                                 f.col('title'), f.col('is_listed'), f.col('floor_number'),
                                                 f.col('monthly_bills'), f.col('location'), f.col('micro_location'),
                                                 f.col('total_number_of_floors'), f.col('city'),
                                                 f.col('measurement_name'),
                                                 f.col('number_of_rooms'), f.col('object_state'),
                                                 f.concat_ws('. ', f.col('additional'), f.col('description'),
                                                             f.col('object_type')).alias('property_description'))
    save_data_to_dw_table(new_properties, 'dim_property')
    return read_from_dw(spark, 'dim_property')


def extract_real_estate_to_dw():
    spark = build_spark_session()

    real_estate_post_cdc = read_from_db(spark, 'real_estate_post_cdc')
    fact_real_estate_dw = read_from_dw(spark, 'fact_real_estate')

    latest_date = find_latest_date_id_in_dw(fact_real_estate_dw)
    refresh_dw_with_newest_real_estate_data(spark, latest_date, real_estate_post_cdc)


if __name__ == "__main__":
    """
    We need to update two tables here: dim property, and fact real estate.
    Foreign keys -dim heating type and dim source are trivial joins, with foreign keys matching 
    what is in the database, so that part is omitted.
    """

    extract_real_estate_to_dw()
