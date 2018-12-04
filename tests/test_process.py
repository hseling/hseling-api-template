import pytest

from hseling_api_template.process import process_data


test_data = [
    ({}, []),
    ({"asd": "a b c"}, ["a!!!\nb!!!\nc!!!"]),
    ({"asd": "a b c", "zxc": "a c d"}, ["a!!!\nb!!!\nc!!!\nd!!!"]),
    ({"asd": b"a b c"}, ["a!!!\nb!!!\nc!!!"])
]


@pytest.mark.parametrize("input_data, expected_result", test_data)
def test_process_data(input_data, expected_result):
    processed_data = [contents for _, contents in process_data(input_data)] 
    assert processed_data == expected_result


def test_process_data_bad_values():
    with pytest.raises(AttributeError):
        assert [contents for _, contents in process_data({"test": 1})]
