import logging
import pyspark.sql as ps
from src.load.schemas import restaurants_schema
from src.config import settings
logger = logging.getLogger()


def read_restaurants(ss) -> ps.DataFrame:
    df = (ss.read
          .format("csv")
          .option("header", True)
          .schema(restaurants_schema)
          .load(settings.RESTAURANTS_DIR)

          )
    logger.info("Read restaurants data")
    # TODO:
    #  - Add logging df.count()
    #  - Add schema validation, nulls check
    #   - Add test: how to test reading?
    return df