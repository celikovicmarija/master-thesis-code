import os
import sys

from pyspark.sql import functions as f
sys.path.append('../data_preparation')
from ..utilities.pyspark_utils import load_data, build_spark_session, save_data_to_db_table

if __name__ == "__main__":
    spark = build_spark_session()

    for file in os.listdir('\\master-thesis-code\\Belibor'):
        interest_rates_df = load_data(spark, 'Belibor\\' + file)
        interest_rates_df.show()
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        interest_rates_df = interest_rates_df.withColumn('date', f.to_date(f.col('date'), 'dd/MM/yyyy'))
        interest_rates_df = interest_rates_df.withColumnRenamed('type', 'period')
        interest_rates_df = interest_rates_df.withColumnRenamed('parameter', 'interest_rate_type')
        save_data_to_db_table(interest_rates_df, 'interest_rate')
