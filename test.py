import pytest
from funtions import data_extraction, file_process

def test_price_computation():
        latitude = 48.7758
        longitude = 9.1829
        expected_result = 200

        result = data_extraction.data_import_weather(latitude, longitude,"2024-01-01","2024-02-21")

        print("result =", {result})
        assert result.status_code == expected_result

        latitude = "123rgf"
        longitude = "123rgf"
        expected_result = None

        result = data_extraction.data_import_weather(latitude, longitude,"2024-01-01","2024-02-21")

        print("result =", {result})
        assert result == expected_result


# Run the test case
test_price_computation()