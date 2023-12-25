import pytest
from unittest.mock import patch
import geocoder


def test_get_missing_coordinates():


    def test_get_location(mock_geocode):
        # Set up the mock
        mock_geocode.return_value.latlng = [51.5074, -0.1278]

        # Call the function with the mock
        result = get_location("London")

        # Assert that the mock was called with the correct argument
        mock_geocode.assert_called_once_with("London")

        # Assert that the function returned the correct result
        assert result == [51.5074, -0.1278]