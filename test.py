import pytest
import pandas as pd
from funtions import data_extraction, data_validation
@pytest.fixture
def sample_data():
    data = {
        'column1': [1, 3, None, 4, 5, 4],
        'column2': [1.1, 2.2, 3.3, 3.2, None, 3.2],
        'column3': ['a', 'b', 'c', 'd', 'e', 'd']
    }
    return pd.DataFrame(data)


def test_data_import_weather_response():
    latitude = 48.7758
    longitude = 9.1829
    expected_result = 200

    result = data_extraction.data_import_weather_response(latitude, longitude, "2024-01-01", "2024-02-21")

    assert result.status_code == expected_result

    latitude = "123rgf"
    longitude = "123rgf"
    expected_result = None

    result = data_extraction.data_import_weather_response(latitude, longitude, "2024-01-01", "2024-02-21")

    assert result == expected_result


def test_check_missing_values(sample_data):
    dv = data_validation.DataValidation(sample_data)
    result = dv.check_missing_values()
    assert len(result) == 2  # Assuming one missing value in the sample data


def test_ensure_one_column_max(sample_data):
    dv = data_validation.DataValidation(sample_data)
    result = dv.ensure_one_column_max('column1', 'column2')
    assert len(result) == 1  # Assuming two rows where column1 < column2 in the sample data


def test_check_duplicates(sample_data):
    dv = data_validation.DataValidation(sample_data)
    result = dv.check_duplicates()
    assert len(result) == 1  # Assuming one duplicate in the sample data
