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
headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://" + server_h + "/api/2.0"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request('POST', url + path, data=json.dumps(data), verify=True, headers=headers)
    return resp.json()


def mkdirs(path, headers):
    _data = {'path': path}
    return perform_query('/dbfs/mkdirs', headers=headers, data=_data)


def create(path, overwrite, headers):
    _data = {'path': path, 'overwrite': overwrite}
    return perform_query('/dbfs/create', headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {'handle': handle, 'data': data}
    return perform_query('/dbfs/add-block', headers=headers, data=_data)


def close(handle, headers):
    _data = {'handle': handle}
    return perform_query('/dbfs/close', headers=headers, data=_data)


def put_local_file(src_path, dbfs_path, overwrite, headers):
    """Upload a local file to Databricks DBFS."""
    with open(src_path, 'rb') as local_file:
        content = local_file.read()
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(handle, base64.standard_b64encode(content[i:i+2**20]).decode(), headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")


def extract(
        local_path="/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/Sizhe_Chen_mini_Project_11/drinks.csv",
        dbfs_path=FILESTORE_PATH + "/drinks.csv",
        directory=FILESTORE_PATH,
        overwrite=True
):
    """Upload a local file to Databricks DBFS."""
    # Make the directory, no need to check if it exists or not
    mkdirs(path=directory, headers=headers)
    # Upload the local file to DBFS
    put_local_file(local_path, dbfs_path, overwrite, headers=headers)

    return dbfs_path


if __name__ == "__main__":
    extract()
