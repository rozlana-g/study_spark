from pyspark.sql.types import (StructType,StructField, StringType, IntegerType , LongType,
                               ArrayType, DoubleType, BooleanType)

restaurants_schema = StructType([
                      StructField("id", LongType(), nullable=False),
    # nullable argument is not a constraint
    # but a reflection of the source and type semantics which enables certain types of optimization
                      StructField("franchise_id", IntegerType(), nullable=True),
                      StructField("franchise_name", StringType(), nullable=True),
                      StructField("restaurant_franchise_id", IntegerType(), nullable=True),
                      StructField("country", StringType(), nullable=True),
                      StructField("city", StringType(), nullable=True),
                      StructField("lat", DoubleType(), nullable=True),
                      StructField("lng", DoubleType(), nullable=True)
    ])

weather_schema = StructType([
                        StructField("lng", DoubleType(), nullable=False),
                        StructField("lat", DoubleType(), nullable=False),
                        StructField("avg_tmpr_f", DoubleType(), nullable=True),
                        StructField("avg_tmpr_c", DoubleType(), nullable=True),
                        StructField("wthr_date", StringType(), nullable=False),
                        StructField("year", IntegerType(), nullable=True),
                        StructField("month", IntegerType(), nullable=True),
                        StructField("day", IntegerType(), nullable=True)
        ])