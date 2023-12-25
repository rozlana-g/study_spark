import pytest
from src.load.read import read_restaurants
from src.transform.fillna import fill_missing_coordinates
from chispa.dataframe_comparer import *


def test_no_missing_coordinates(spark):
    "Tests that fill_missing_coordinates work even if there is no missing coordinates"

    dff = read_restaurants(spark, './data/tests/right')


    trr = fill_missing_coordinates(dff, spark)


    assert_df_equality(dff, trr)

