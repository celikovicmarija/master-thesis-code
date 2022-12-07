from pyspark.pandas import DataFrame
from pyspark.sql import functions as f

from utilities.pyspark_utils import save_data_to_dw_table, read_from_db, read_from_dw, build_spark_session


def refresh_dw_with_newest_exchange_rate_data(exchange_rate_cdc: DataFrame,
                                              dim_date_dw: DataFrame) -> None:
    joined_df = dim_date_dw.join(exchange_rate_cdc, dim_date_dw.fulldate == exchange_rate_cdc.exchange_date,
                                 how='inner')
    new_rows_to_add = joined_df.select(f.col('date_id'), f.col('price_EUR_RSD'), f.col('price_EUR_USD'))
    save_data_to_dw_table(new_rows_to_add, 'fact_financial_indicators')


def extract_interest_rate_to_dw():
    spark = build_spark_session()

    exchange_rate_cdc = read_from_db(spark, 'exchange_rate_cdc')
    dim_date_dw = read_from_dw(spark, 'dim_date')

    refresh_dw_with_newest_exchange_rate_data(exchange_rate_cdc, dim_date_dw)


if __name__ == "__main__":
    extract_interest_rate_to_dw()
