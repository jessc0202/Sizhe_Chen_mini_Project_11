# simple_test_extract_upload.py
from mylib.extract import upload_file

def main():
    # Path to the local file to upload (make sure this file exists)
    local_file_path = (
        "/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/"
        "Sizhe_Chen_mini_Project_11/drinks.csv"
    )

    # Destination path in DBFS
    dbfs_file_path = "dbfs:/FileStore/mini_project11/drinks.csv"

    # Call the upload_file function
    try:
        upload_file(local_file_path, dbfs_file_path)
        print("File uploaded successfully.")
    except Exception as e:
        print(f"An error occurred during file upload: {e}")

if __name__ == "__main__":
    main()
