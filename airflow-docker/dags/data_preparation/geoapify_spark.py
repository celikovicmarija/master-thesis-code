import sys

import findspark
from pyspark.sql import functions as f

sys.path.append('..')
from utilities.pyspark_utils import load_posts_data, build_spark_session

findspark.add_packages('mysql:mysql-connector-java:8.0.30')

if __name__ == "__main__":
    spark = build_spark_session()

    conf_out = spark.sparkContext.getConf()
    conf_out.toDebugString()
    oglasi_df = load_posts_data(spark, 'real_estate_scraper\halooglasi.csv')
    geocoded_df = load_posts_data(spark, 'geocoded.csv')

    conn_string = 'jdbc:mysql://localhost:3306/real_estate_db?user=root&password=password'
    geocoded_df.write.jdbc(conn_string, table='geocode', mode="append")

    oglasi_df = oglasi_df.withColumn('street', f.trim(f.lower(f.col('street'))))
    geocoded_df = geocoded_df.withColumn('street_oglasi', f.trim(f.lower(f.col('street_oglasi'))))
    oglasi_df = oglasi_df.withColumn('micro_location', f.trim(f.lower(f.col('micro_location'))))
    geocoded_df = geocoded_df.withColumn('micro_location_oglasi', f.trim(f.lower(f.col('micro_location_oglasi'))))

    df = oglasi_df.join(geocoded_df, (f.col('street') == f.col('street_oglasi')) &
                        (f.col('micro_location') == f.col('micro_location_oglasi')), how="left")
    df.show()
