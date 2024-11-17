# test_transform_load.py
from mylib.transform_load import transform_load

def test_transform_load():
    result = transform_load("sample_input_path.csv")
    assert result == "finished transform and load"
