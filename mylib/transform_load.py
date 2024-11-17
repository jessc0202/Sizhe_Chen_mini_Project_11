"""
transform and load function for drinks dataset
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/mini_project11/drinks.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    
    # Load the drinks dataset and infer the schema
    drinks_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # Add a unique ID column to the DataFrame
    drinks_df = drinks_df.withColumn("id", monotonically_increasing_id())

    # Transform into a delta lake table and store it 
    drinks_df.write.format("delta").mode("overwrite").saveAsTable("drinks_delta")
    
    # Count and print the number of rows
    num_rows = drinks_df.count()
    print(f"Number of rows in drinks dataset: {num_rows}")
    
    return "Finished transform and load for drinks dataset"

if __name__ == "__main__":
    load()
