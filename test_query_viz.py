import os
from mylib.query_viz import viz  

def test_viz():
    # Assuming `viz` saves a file, check if the file is created
    save_path = ("./drinks.csv")
    viz(save_path=save_path)
    assert os.path.exists(save_path), "Visualization should create the output file"
