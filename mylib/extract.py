import requests
from dotenv import load_dotenv
import os
import json
import base64

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/mini_project11"
headers = {'Authorization': f'Bearer {access_token}'}
url = f"https://{server_h}/api/2.0"


def perform_query(path, headers, data=None):
    """Performs a POST request to the Databricks API."""
    if data is None:
        data = {}
    session = requests.Session()
    response = session.post(url + path, 
                            data=json.dumps(data), 
                            headers=headers, 
                            verify=True)
    response.raise_for_status()  # Raises an exception for HTTP errors
    return response.json()


def mkdirs(path, headers):
    """Create a directory in DBFS if it does not exist."""
    data = {'path': path}
    return perform_query('/dbfs/mkdirs', headers=headers, data=data)


def create(path, overwrite, headers):
    """Create a handle for uploading a file to DBFS."""
    data = {'path': path, 'overwrite': overwrite}
    return perform_query('/dbfs/create', headers=headers, data=data)


def add_block(handle, data, headers):
    """Add a block of data to a DBFS file handle."""
    data_payload = {'handle': handle, 'data': data}
    return perform_query('/dbfs/add-block', headers=headers, data=data_payload)


def close(handle, headers):
    """Close a DBFS file handle after uploading."""
    data = {'handle': handle}
    return perform_query('/dbfs/close', headers=headers, data=data)


def put_local_file(src_path, dbfs_path, overwrite, headers):
    """
    Upload a local file to Databricks DBFS by reading in chunks.
    :param src_path: Local path of the file to upload
    :param dbfs_path: Destination path in DBFS
    :param overwrite: Boolean indicating if the file should be overwritten
    :param headers: Authorization headers for the request
    """
    with open(src_path, 'rb') as local_file:
        content = local_file.read()
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print(f"Uploading file to {dbfs_path}...")

        # Upload file in chunks of 1 MB
        for i in range(0, len(content), 2**20):
            chunk = base64.standard_b64encode(content[i:i+2**20]).decode()
            add_block(handle, chunk, headers=headers)
        
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")


def extract(local_path=("/Users/chensi/Desktop/MIDS/Fall 2024/" 
                        "IDS 706/Sizhe_Chen_mini_Project_11/drinks.csv"),
            dbfs_path=FILESTORE_PATH + "/drinks.csv",
            directory=FILESTORE_PATH,
            overwrite=True):
    """
    Extracts a local file and uploads it to Databricks DBFS.
    :param local_path: Local path of the file to upload
    :param dbfs_path: Destination path in DBFS
    :param directory: DBFS directory path
    :param overwrite: Boolean indicating if the file should be overwritten
    """
    # Ensure the directory exists in DBFS
    mkdirs(path=directory, headers=headers)

    # Upload the local file to DBFS
    put_local_file(local_path, dbfs_path, overwrite, headers=headers)

    return dbfs_path


if __name__ == "__main__":
    extract()
