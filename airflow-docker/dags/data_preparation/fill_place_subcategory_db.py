import sys

from pyspark.sql import functions as f

sys.path.append('..')
from utilities.pyspark_utils import read_from_db, build_spark_session, save_data_to_db_table


def fill_place_subcategory_db():
    """
    Subcategory names can be the same for different categories, so you have to check for that also.
    :return:
    """
    spark = build_spark_session()
    place_df = read_from_db(spark, 'place')
    category_df = read_from_db(spark, 'category')
    subcategory_df = read_from_db(spark, 'subcategory')

    place_df = place_df.withColumn('temp', f.split(f.regexp_extract('categories', '\[(.*)\]', 1), ','))
    place_df = place_df.withColumn('exploded', f.explode('temp').alias('exploded'))

    subcategory_df = subcategory_df.join(category_df, category_df.category_id == subcategory_df.category_id, how='inner')

    join_df = place_df.join(subcategory_df, (place_df.categories.contains(subcategory_df.category_name + '.' +
                                                                          subcategory_df.subcategory_name) | (
                                                 place_df.categories.contains(subcategory_df.subcategory_name))),
                            how='inner')

    join_df = join_df.select(f.col('subcategory_id'), f.col('place_id')).filter(
        f.col('subcategory_id').isNotNull()).distinct()
    save_data_to_db_table(join_df, 'place_subcategory')


if __name__ == "__main__":
    fill_place_subcategory_db()
