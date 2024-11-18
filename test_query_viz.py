import os
from mylib.query_viz import viz

def test_viz():
    # Absolute path to the CSV file
    file_path = ("/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/"
    "Sizhe_Chen_mini_Project_11/drinks.csv")
    save_path = "./average_beer_servings_by_country.png"
    
    # Ensure the CSV file exists
    assert os.path.exists(file_path), "CSV file does not exist."
    
    # Run the visualization function
    viz(file_path=file_path, save_path=save_path)
    
    # Check if the output file was created
    assert os.path.exists(save_path), "Visualization should create the output file."
    
    # Clean up the generated file
    if os.path.exists(save_path):
        os.remove(save_path)
