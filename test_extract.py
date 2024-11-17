# test_extract.py
import os
from mylib.extract import extract  

def test_extract():
    # Here, you'll want to mock any requests or file operations
    # Example:
    file_path = "/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/Sizhe_Chen_mini_Project_11/drinks.csv"
    result = extract(file_path)
    assert os.path.exists(file_path), "File should exist after extraction"
