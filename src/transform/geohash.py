from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pygeohash as pgh
import pyspark.sql as ps


@udf(returnType=StringType(), useArrow=True)  # type: ignore[no-redef]
def get_geohash(lat: float, lng: float) -> str:
    return pgh.encode(latitude=lat, longitude=lng, precision=4)


# from pyspark.sql.functions import pandas_udf
# import pandas as pd
#
# def pd_geohash(lat: pd.Series, lng: pd.Series) -> pd.Series:
#     return pgh.encode(latitude=lat, longitude=lng, precision=4)
# pd_geohash = pandas_udf(pd_geohash, returnType=StringType()) # type: ignore[no-redef]


def add_geohash_column(df: ps.DataFrame) -> ps.DataFrame:
    return df.withColumn("geohash", get_geohash("lat", "lng"))
