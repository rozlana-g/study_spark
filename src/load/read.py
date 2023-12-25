import logging
import time

import pyspark.sql as ps
import pyspark.sql.functions as F
from src.load.schemas import restaurants_schema, weather_schema
from src.config import settings

logger = logging.getLogger()


def read_restaurants(ss, path=settings.RESTAURANTS_DIR) -> ps.DataFrame:
    df = (ss.read
          .format("csv")  # Returns a DataFrameReader that can be used to read data in as a DataFrame.
          .option("header", True)
          .schema(restaurants_schema)  # the underlying data source can skip the schema inference step,
          # and thus speed up data loading. but it does not check it!
          .load(path)  # creates a dataframe from datasources,
          # on top which Spark transformations can be applied.
          # see https://stackoverflow.com/questions/56818629/what-does-load-do-in-spark
          .persist()  # I want to persist the dataframe in memory because I will reference it several times:
          # when validating data and for further transformations,
          # source https://stackoverflow.com/questions/59178418/is-it-efficient-to-cache-a-dataframe-for-a
          # -single-action-spark-application-in-wh/59182062#59182062
          # QUESTION: do i understand it right?
          )

    # fail in the runtime if the schema is not as expected
    # QUESTION: is there an analogue of pandera for pyspark to validate data?
    a = df.count()
    b = (df
         .select(F.col("id"))
         .dropDuplicates()
         .count())  # QUESTION: will it read the data again?

    assert a == b, "There are duplicates in the data"
    assert df.filter(F.col("id").isNull()).count() == 0, "There are nulls in the id column"
    assert df.schema.simpleString() == restaurants_schema.simpleString(), "The schema is not as expected"

    logger.info(f"Restaurants data is persisted, rows: {a}")

    return df


def read_weather(ss) -> ps.DataFrame:
    weather_df = (ss.read
                  .parquet(settings.WEATHER_DIR)
                  .filter(F.col("lat").isNotNull() & F.col("lng").isNotNull() & F.col("wthr_date").isNotNull())
                  )

    # not check but aggressively drop nulls

    return weather_df
