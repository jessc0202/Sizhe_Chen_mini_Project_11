import requests
import os
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")  # Databricks server hostname
access_token = os.getenv("ACCESS_TOKEN")  # Databricks access token
url = f"https://{server_h}/api/2.0"  # Databricks API base URL
headers = {'Authorization': f'Bearer {access_token}'}

def perform_query(path, data=None):
    """Function to make a POST request to the Databricks API.
    
    Args:
        path (str): API endpoint path.
        data (dict): Data to be sent in the request.

    Returns:
        dict: JSON response from the API.
    """
    session = requests.Session()
    response = session.post(url + path, json=data, headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

def mkdirs(dbfs_path):
    """Create a directory in DBFS.
    
    Args:
        dbfs_path (str): Path of the directory in DBFS.
    """
    response = perform_query('/dbfs/mkdirs', data={'path': dbfs_path})
    print(f"Directory created: {dbfs_path}")
    return response

def create_file(dbfs_path, overwrite=True):
    """Open a handle to create a new file in DBFS.
    
    Args:
        dbfs_path (str): Path where the file will be created.
        overwrite (bool): Whether to overwrite the file if it exists.

    Returns:
        int: File handle used to write data.
    """
    response = perform_query('/dbfs/create', 
                             data={'path': dbfs_path, 'overwrite': overwrite})
    print(f"File created at path: {dbfs_path}")
    return response['handle']

def add_block(handle, data_chunk):
    """Add a block of data to an open file handle.
    
    Args:
        handle (int): File handle returned by create_file.
        data_chunk (bytes): Data chunk to be written, in bytes format.
    """
    encoded_data = base64.standard_b64encode(data_chunk).decode('utf-8')
    perform_query('/dbfs/add-block', data={'handle': handle, 'data': encoded_data})
    print("Added a block of data.")

def close_file(handle):
    """Close the file handle in DBFS, finalizing the upload.
    
    Args:
        handle (int): File handle to close.
    """
    perform_query('/dbfs/close', data={'handle': handle})
    print("File upload complete, and handle closed.")

def upload_file(local_path, dbfs_path, overwrite=True):
    """Upload a local file to Databricks DBFS in chunks.
    
    Args:
        local_path (str): Path to the local file to upload.
        dbfs_path (str): Destination path in DBFS.
        overwrite (bool): Whether to overwrite the file if it exists.
    """
    # Ensure the directory exists in DBFS
    dbfs_dir = os.path.dirname(dbfs_path)
    mkdirs(dbfs_dir)

    # Open a handle to write to DBFS
    handle = create_file(dbfs_path, overwrite)

    # Read the file and upload it in chunks of 1 MB
    with open(local_path, 'rb') as f:
        while chunk := f.read(1 * 1024 * 1024):  # Read 1 MB chunks
            add_block(handle, chunk)

    # Close the file handle to complete the upload
    close_file(handle)
    print(f"File '{local_path}' successfully uploaded to '{dbfs_path}'.")

if __name__ == "__main__":
    local_file_path = (
        "/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/"
        "Sizhe_Chen_mini_Project_11/drinks.csv"
    )
    dbfs_file_path = "dbfs:/FileStore/mini_project11/drink.csv"
    upload_file(local_file_path, dbfs_file_path)
