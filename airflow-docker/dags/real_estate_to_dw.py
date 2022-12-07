from pyspark.pandas import DataFrame
from pyspark.sql import functions as f

from utilities.pyspark_utils import save_data_to_dw_table, read_from_db, read_from_dw, build_spark_session


def refresh_dw_with_newest_real_estate_data(spark, real_estate_post_cdc: DataFrame) -> None:
    dim_date_dw = read_from_dw(spark, 'dim_date')
    dim_municipality_dw = read_from_dw(spark, 'dim_municipality')
    dim_geocode_dw = read_from_dw(spark, 'dim_geocode')
    joined_df = dim_date_dw.join(real_estate_post_cdc, dim_date_dw.fulldate == real_estate_post_cdc.date,
                                 how='inner')

    dim_property_df = add_new_property_to_dw(spark, real_estate_post_cdc)

    dim_property_df = dim_property_df.select(
        *(f.col(x).alias(x + '_property') for x in dim_property_df.columns))
    joined_df = joined_df.join(dim_property_df, dim_property_df.link_property == joined_df.link, how='left')
    dim_geocode_dw = dim_geocode_dw.select(
        *(f.col(x).alias(x + '_geocode') for x in dim_geocode_dw.columns))

    joined_df = joined_df.join(dim_municipality_dw,
                               (joined_df.micro_location_property.contains(dim_municipality_dw.municipality_name)) |
                               (joined_df.micro_location_property.contains(
                                   dim_municipality_dw.municipality_name_srlat)) |
                               (joined_df.city_property.contains(dim_municipality_dw.municipality_name_srlat)) | (
                                   joined_df.city_property.contains(dim_municipality_dw.municipality_name)),
                               how='left')

    joined_df = joined_df.join(dim_geocode_dw, dim_geocode_dw.geocode_id_geocode == joined_df.geocode_id, how='left')
    dim_municipality_dw = dim_municipality_dw.select(
        *(f.col(x).alias(x + '_municipality') for x in dim_municipality_dw.columns))

    joined_df = joined_df.join(dim_municipality_dw,
                               (joined_df.formatted_geocode.contains(
                                   dim_municipality_dw.municipality_name_municipality)) |
                               (joined_df.formatted_geocode.contains(
                                   dim_municipality_dw.municipality_name_srlat_municipality)) |
                               (joined_df.suburb_geocode.contains(dim_municipality_dw.municipality_name_municipality)) |
                               (joined_df.suburb_geocode.contains(
                                   dim_municipality_dw.municipality_name_srlat_municipality)) |
                               (joined_df.city_geocode.contains(dim_municipality_dw.municipality_name_municipality)) |
                               (joined_df.city_geocode.contains(
                                   dim_municipality_dw.municipality_name_srlat_municipality)) |
                               (joined_df.address_line2_geocode.contains(
                                   dim_municipality_dw.municipality_name_municipality)) |
                               (joined_df.address_line2_geocode.contains(
                                   dim_municipality_dw.municipality_name_srlat_municipality)),
                               how='left')

    new_rows_to_add = joined_df.select(f.col('size'), f.col('date_id'), f.col('price'), f.col('price_per_unit'),
                                       f.col('property_id_property').alias('property_id'),
                                       f.col('source_id'), f.col('heating_type_id'), f.col('measurement_id'),
                                       f.col('geocode_id'), f.col('transaction_type_id'),
                                       f.col('real_estate_type_id').alias('property_type_id'),
                                       f.col('municipality_id_municipality').alias('municipality_id'))
    save_data_to_dw_table(new_rows_to_add, 'fact_real_estate')


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
                                                 f.col('location'), f.col('micro_location'),
                                                 f.col('total_number_of_floors'), f.col('city'),
                                                 f.col('measurement_name'),
                                                 f.col('number_of_rooms'),
                                                 f.col('object_state').alias('property_state'),
                                                 f.concat_ws('. ', f.col('additional'), f.col('description'),
                                                             f.col('object_type')).alias('property_description'))
    save_data_to_dw_table(new_properties, 'dim_property')
    return read_from_dw(spark, 'dim_property')


def extract_real_estate_to_dw():
    spark = build_spark_session()

    real_estate_post_cdc = read_from_db(spark, 'real_estate_post_cdc')
    refresh_dw_with_newest_real_estate_data(spark, real_estate_post_cdc)


if __name__ == "__main__":
    """
    We need to update two tables here: dim property, and fact real estate.
    Foreign keys -dim heating type and dim source are trivial joins, with foreign keys matching 
    what is in the database, so that part is omitted.
    """

    extract_real_estate_to_dw()
