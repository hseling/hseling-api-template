import pytest

from hseling_api_template.validate import validate_data


test_data = [
    ((), True),
    ((None,), False),
    ((1, 2, "a"), False),
    ((1, 2, 3), True),
    (("a", "b"), True),
]


@pytest.mark.parametrize("input_data, expected_result", test_data)
def test_validate_data(input_data, expected_result):
    assert validate_data(*input_data) == expected_result
