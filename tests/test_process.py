import pytest

from hseling_api_template.process import process_data


test_data = [
    ((), None),
    ((None,), None),
    ((1, 2, 3), 6),
    (("a", "b"), "ab"),
]


@pytest.mark.parametrize("input_data, expected_result", test_data)
def test_process_data(input_data, expected_result):
    assert process_data(*input_data) == expected_result


def test_process_data_bad_values():
    with pytest.raises(TypeError):
        process_data(1, 2, "a")
