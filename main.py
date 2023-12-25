from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from functools import partial

from src.config import settings
from src.load.read import read_restaurants, read_weather
from src.transform.fillna import fill_missing_coordinates
from src.transform.geohash import add_geohash_column


if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("study_spark")
             .master("local[*]")
             .config("spark.driver.memory", "4g")
             .config("spark.executor.memory", "4g")
             .config("spark.sql.shuffle.partitions", settings.NUM_PARTITIONS)
             .getOrCreate() )

    fill_missing_coordinates_with_default = partial(fill_missing_coordinates, ss=spark)

    # reading transformations
    restaurants_df = read_restaurants(spark)
    weather_df = read_weather(spark)

    # enrichment transformations
    enriched_df = (restaurants_df
                   .transform(fill_missing_coordinates_with_default)
                   .transform(add_geohash_column)
                   )

    enriched_weather_df = (weather_df
                           # .filter(F.col("wthr_date") == "2017-08-09")  #for testing purposes
                           .transform(add_geohash_column)
                            .groupby("geohash", "wthr_date", "year", "month", "day")
                            .agg(F.avg("avg_tmpr_c"), F.avg("avg_tmpr_f"))
                           )

    # final join transformation
    restaurant_weather_df = (enriched_df
                             .join(enriched_weather_df, on=["geohash"], how="left")
                             .withColumnRenamed("avg(avg_tmpr_c)", "avg_tmpr_c")
                             .withColumnRenamed("avg(avg_tmpr_f)", "avg_tmpr_f")
                             .drop('geohash')
                             )

    # writing action
    (restaurant_weather_df
     .repartition(settings.NUM_PARTITIONS)
     .write
     .partitionBy("year", "month", "day")
     .mode("overwrite")
    .parquet(settings.OUTPUT_DIR)
     )