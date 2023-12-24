from opencage.geocoder import OpenCageGeocode
import pandas as pd
import pyspark.sql as ps
from pyspark.sql.functions import col, isnan, coalesce
from src.config import settings
import logging

logger = logging.getLogger()


def get_missing_coordinates(restaurants_df: ps.DataFrame) -> pd.DataFrame:
    restaurants_to_fill = (restaurants_df
                           .filter((restaurants_df["lng"].isNull()) | isnan(restaurants_df["lng"]))
                           .select("country", "city")
                           .collect()
                           )

    geocoder = OpenCageGeocode(settings.opencage_api_key)

    missing_coordinates_df = pd.DataFrame()
    for row in restaurants_to_fill:
        location = row["country"] + ", " + row["city"]
        results = geocoder.geocode(location)
        if results is None:
            logger.warning('No results found: %s', location)
            continue

        df = pd.DataFrame({"country": row["country"],
                           "city": row["city"],
                           "missing_lat": results[0]['geometry']['lat'],
                           "missing_lng": results[0]['geometry']['lng']}, index=[0])
        missing_coordinates_df = pd.concat([missing_coordinates_df, df], axis=0)

    #TODO: add logging
    # add test for no duplicates

    return missing_coordinates_df


def fill_missing_coordinates(restaurants_df: ps.DataFrame, ss: ps.session.SparkSession) -> ps.DataFrame:
    missing_coordinates_df = get_missing_coordinates(restaurants_df)

    restaurants_df = (restaurants_df
                      .join(ss.createDataFrame(missing_coordinates_df),
                            on=["country", "city"],
                            how="left")
                      .withColumn('lat', coalesce(col('lat'), col('missing_lat')))
                      .withColumn('lng', coalesce(col('lng'), col('missing_lng')))
                      .drop(col('missing_lat'), col('missing_lng'))
                      .collect()
                      )

    # TODO: rewrite with drop, append

    return ss.createDataFrame(restaurants_df)

