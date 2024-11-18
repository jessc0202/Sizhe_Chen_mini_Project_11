import requests
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import json
import base64

spark = SparkSession.builder \
    .appName("AlcoholConsumptionAnalysis") \
    .getOrCreate()

# Display the current DBFS contents
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")

headers = {'Authorization': f'Bearer {access_token}'}
url = f"https://{server_h}/api/2.0"

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

def put_file(src_path, dbfs_path, overwrite, headers):
    handle = create(dbfs_path, overwrite, headers=headers)['handle']
    with open(src_path, 'rb') as local_file:
        while True:
            contents = local_file.read(2**20)
            if len(contents) == 0:
                break
            add_block(handle, base64.standard_b64encode(contents).decode(), headers=headers)
        close(handle, headers=headers)

# Specify the path and upload file
mkdirs("dbfs:/FileStore/drinks_data", headers=headers)
put_file("/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/Sizhe_Chen_mini_Project_11/drinks.csv", "dbfs:/FileStore/drinks_data/drinks.csv", True, headers=headers)

def extract():
    FILESTORE_PATH = "dbfs:/FileStore/drinks_data"
    file_path = f"{FILESTORE_PATH}/drinks.csv"
    mkdirs(FILESTORE_PATH, headers=headers)
    put_file("/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/Sizhe_Chen_mini_Project_11/drinks.csv", file_path, True, headers=headers)
    return file_path

drinks_file_path = extract()

def load(file_path):
    spark = SparkSession.builder.appName("Drinks Data Processing").getOrCreate()
    drinks_df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Example transformation: Add a unique ID column
    from pyspark.sql.functions import monotonically_increasing_id
    drinks_df = drinks_df.withColumn("id", monotonically_increasing_id())
    
    # Save as Delta table
    spark.sql("DROP TABLE IF EXISTS drinks_delta")
    drinks_df.write.format("delta").mode("overwrite").saveAsTable("drinks_delta")

    return drinks_df

drinks_df = load(drinks_file_path)

query_result = spark.sql("""
    SELECT country, continent, avg(total_litres_of_pure_alcohol) as avg_alcohol
    FROM drinks_delta
    GROUP BY country, continent
    ORDER BY avg_alcohol DESC
""")
query_result.show()

import matplotlib.pyplot as plt
import pandas as pd

# Convert query result to Pandas for plotting
query_df = query_result.toPandas()

# Plotting average alcohol consumption by continent
plt.figure(figsize=(10, 6))
plt.bar(query_df['country'], query_df['avg_alcohol'], color='skyblue')
plt.xlabel('Country')
plt.ylabel('Average Alcohol Consumption (L)')
plt.title('Average Alcohol Consumption by Country')
plt.xticks(rotation=90)
plt.show()

def trigger_job():
    url = f'https://{server_h}/api/2.0/jobs/run-now'
    data = {'job_id': os.getenv("JOB_ID")}
    response = requests.post(url, headers={'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}, json=data)
    if response.status_code == 200:
        print('Job run successfully triggered')
    else:
        print(f'Error: {response.status_code}, {response.text}')

trigger_job()
