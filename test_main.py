"""
Test Databricks functionality
"""

import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/mini_project11"
url = f"https://{server_h}/api/2.0"

# Check that environment variables are loaded correctly
if not server_h or not access_token:
    raise ValueError("SERVER_HOSTNAME or ACCESS_TOKEN environment variable is not set.")

# Function to check if a file path exists and if authentication settings work
def check_filestore_path(path, headers):
    try:
        response = requests.get(f"{url}/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        return response.json().get('path') is not None
    except requests.exceptions.RequestException as e:
        print(f"Error checking file path: {e}")
        return False

# Test if the specified FILESTORE_PATH exists on Databricks
def test_databricks():
    headers = {'Authorization': f'Bearer {access_token}'}
    assert check_filestore_path(FILESTORE_PATH, headers), "Databricks filestore path check failed."

if __name__ == "__main__":
    test_databricks()
