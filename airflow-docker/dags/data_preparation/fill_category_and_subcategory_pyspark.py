import json
import sys

from sqlalchemy import select

sys.path.append('..')
from utilities.pyspark_utils import get_keys_and_constants
from utilities.models import Category, Subcategory
from utilities.db_connection import create_session

keys = get_keys_and_constants()
mysql_connector_jar = keys.mysql_connector_jar

if __name__ == "__main__":
    # spark = build_spark_session()

    # conf_out = spark.sparkContext.getConf()
    # # df = load_posts_data(spark, 'clean\geocoded.csv')
    # place_details = load_posts_data(spark, '..\..\..\clean\place_details_more_details.csv')
    #
    # categories_dict = {}
    # place_details = place_details.withColumn('col4', f.split(f.regexp_extract('categories', '\[(.*)\]', 1), ','))
    #
    # categories_df = place_details.withColumn('exploded', f.explode('col4').alias('exploded'))
    # categories_df = categories_df.select("exploded").distinct()
    # categories_df.show()
    #
    # # place_details.select(f.explode('col4').alias('exploded')).groupby('exploded').count().show()
    # categories = categories_df.filter(~f.col('exploded').contains('.'))
    # # categories = categories_df.withColumn('exploded', f.filter(f.col('exploded'), is_category))
    # list_of_categories = set(list(categories.select('exploded').toPandas()['exploded']))
    # # list_of_categories=categories.rdd.map(lambda x: x.exploded).collect()
    #
    # for category in list_of_categories:
    #     print(f"THIS IS MY CATEGORY: {category}")
    #     subcategories = categories_df.select('exploded').filter(
    #         f.col('exploded').contains('.')).filter(f.lower(f.col('exploded')).contains(category))
    #     subcategories = list(set(list(subcategories.select('exploded').toPandas()['exploded'])))
    #     # subcategories = subcategories.rdd.map(lambda x: x.exploded).collect()
    #
    #     categories_dict[category] = subcategories

    ############# THIS IS A LONG PROCESS, making it easy by saving the json, just in case
    with open('categories_subcategories.json', 'r') as file:
        categories_dict = json.load(file)

    try:
        with create_session() as session:
            for key, values in categories_dict.items():
                cat = Category()
                cat.category_name = key
                session.add(cat)
                session.flush()
                session.refresh(cat)
                if len(values) > 0:
                    for subcategory in values:
                        subcat = Subcategory()
                        subcat.subcategory_name = subcategory
                        subcat.category_id = cat.category_id
                        session.add(subcat)
                        session.flush()
                else:
                    subcat = Subcategory()
                    subcat.subcategory_name = key
                    subcat.category_id = cat.category_id
                    session.add(subcat)
                    session.flush()
            session.commit()
    except Exception as e:
        session.rollback()
        print(f'Could not save categories and subcategories to the db: {str(e)}')
