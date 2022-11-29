from pyspark.pandas import DataFrame
from pyspark.sql import functions as f

from utilities.pyspark_utils import save_data_to_dw_table, read_from_db, read_from_dw, build_spark_session


def find_latest_date_id_in_dw(interest_rate_dw: DataFrame) -> int:
    max_date = interest_rate_dw.select(f.max(f.col('date_id')).alias('max_date')).collect()[0]['max_date']
    # joined_df = interest_rate_dw.join(dim_date_dw, interest_rate_dw.date_id == dim_date_dw.date_id, how='inner')
    return max_date


def refresh_dw_with_newest_exchange_rate_data(latest_date: int, exchange_rate_cdc: DataFrame,
                                              dim_date_dw: DataFrame) -> None:
    # max_date_standard = dim_date_dw.select(f.col('fulldate')).filter(f.col('date_id') == latest_date)
    joined_df = dim_date_dw.join(exchange_rate_cdc, dim_date_dw.fulldate == exchange_rate_cdc.exchange_date,
                                 how='inner').filter(f.col('date_id') > latest_date)
    new_rows_to_add = joined_df.select(f.col('date_id'), f.col('price_EUR_RSD'), f.col('price_EUR_USD'))
    new_rows_to_add.show()
    save_data_to_dw_table(new_rows_to_add, 'fact_financial_indicators')


def extract_interest_rate_to_dw():
    spark = build_spark_session()

    exchange_rate_cdc = read_from_db(spark, 'exchange_rate_cdc')
    exchange_rate_dw = read_from_dw(spark, 'fact_financial_indicators')
    dim_date_dw = read_from_dw(spark, 'dim_date')

    latest_date = find_latest_date_id_in_dw(exchange_rate_dw)
    refresh_dw_with_newest_exchange_rate_data(latest_date, exchange_rate_cdc, dim_date_dw)


# extract_interest_rate_to_dw()
if __name__ == "__main__":
    extract_interest_rate_to_dw()
