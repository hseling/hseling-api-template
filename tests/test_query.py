import pytest

from hseling_api_template.query import query_data


test_data = [
    ({}, "lines", 0),
    ({"asd": "a"}, "lines", 1),
    ({"asd": "a\nb", "zxc": "ad"}, "lines", 3)
]


@pytest.mark.parametrize("input_data, query_type, expected_result", test_data)
def test_process_data(input_data, query_type, expected_result):
    result = query_data(input_data, query_type)
    assert result == expected_result
