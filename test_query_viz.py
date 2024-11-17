import os
from mylib.query_viz import viz  # Import the visualization function

def test_viz():
    # Assuming `viz` saves a file, check if the file is created
    save_path = "/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/Sizhe_Chen_mini_Project_11/plot.png"
    viz(save_path=save_path)
    assert os.path.exists(save_path), "Visualization should create the output file"
