from pyspark.sql.types import (StructType,StructField, StringType, IntegerType , LongType,
                               ArrayType, DoubleType, BooleanType)

restaurants_schema = StructType([
                      StructField("id", LongType(), nullable=False),
                      StructField("franchise_id", IntegerType(), nullable=True),
                      StructField("franchise_name", StringType(), nullable=True),
                      StructField("restaurant_franchise_id", IntegerType(), nullable=True),
                      StructField("country", StringType(), nullable=True),
                      StructField("city", StringType(), nullable=True),
                      StructField("lat", DoubleType(), nullable=True),
                      StructField("lng", DoubleType(), nullable=True)
    ])