import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Query Visualization").getOrCreate()

# Load data from the Delta table
drinks_df = spark.sql("SELECT * FROM drinks_delta")
drinks_df.createOrReplaceTempView("drinks")

# Sample query
def query_transform():
    """
    Run a predefined SQL query on the 'drinks' table.
    Returns:
        DataFrame: Result of the SQL query.
    """
    query = """
        SELECT country, AVG(beer_servings) AS avg_beer_servings
        FROM drinks
        GROUP BY country
        ORDER BY avg_beer_servings DESC
    """
    query_result = spark.sql(query)
    return query_result

def viz(file_path=("/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/"
                   "Sizhe_Chen_mini_Project_11/drinks.csv"),
         save_path="./average_beer_servings_by_country.png"):
    # Initialize Spark session
    spark = SparkSession.builder.appName("AlcoholConsumptionAnalysis").getOrCreate()
    
    # Read the CSV file
    drinks_df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Debugging: Print schema and show data
    drinks_df.printSchema()
    drinks_df.show(5)

    # Register the DataFrame as a SQL temporary view
    drinks_df.createOrReplaceTempView("drinks")
    
    # Run the query to calculate average beer servings by country
    query_result = spark.sql("""
        SELECT country, AVG(beer_servings) AS avg_beer_servings
        FROM drinks
        GROUP BY country
        ORDER BY avg_beer_servings DESC
    """)
    
    # Convert query result to Pandas for plotting
    query_df = query_result.toPandas()
    
    # Plotting average beer servings by country
    plt.figure(figsize=(10, 6))
    plt.bar(query_df['country'], 
            query_df['avg_beer_servings'], color='skyblue')
    plt.xlabel('Country')
    plt.ylabel('Average Beer Servings')
    plt.title('Average Beer Servings by Country')
    plt.xticks(rotation=90)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(save_path)
    print(f"Visualization saved as '{save_path}'")