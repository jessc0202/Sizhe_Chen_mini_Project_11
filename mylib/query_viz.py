import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Query Visualization").getOrCreate()

# Load your dataset into a Spark DataFrame and register it as a table
file_path = "/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/Sizhe_Chen_mini_Project_11/drinks.csv"  # Replace with the actual path to your CSV file
drinks_df = spark.read.csv(file_path, header=True, inferSchema=True)
drinks_df.createOrReplaceTempView("drinks")  # Register as a temporary table

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

# Sample visualization function
def viz(save_path="/Users/chensi/Desktop/MIDS/Fall 2024/IDS 706/Sizhe_Chen_mini_Project_11/average_beer_servings_by_country.png"):
    query_result = query_transform()
    df_surface_avg = query_result.toPandas()  # Convert Spark DataFrame to Pandas for plotting

    # Plot the bar chart with improved x-axis readability
    plt.figure(figsize=(18, 8))  # Increase figure size for better spacing
    plt.bar(
        df_surface_avg["country"],
        df_surface_avg["avg_beer_servings"],
        color="skyblue"
    )
    plt.xlabel("Country")
    plt.ylabel("Average Beer Servings")
    plt.title("Average Beer Servings by Country")

    # Rotate x-tick labels for readability and reduce font size
    plt.xticks(rotation=45, ha="right", fontsize=9)

    # Display every nth label for readability if too many labels (show every 5th label)
    plt.xticks(ticks=range(0, len(df_surface_avg), 5), labels=df_surface_avg["country"][::5])

    plt.tight_layout()  # Adjust layout to prevent label cutoff
    
    # Save the figure
    plt.savefig(save_path, format='png')
    print(f"Visualization saved as '{save_path}'")

# Run the visualization and save it to the specified path
viz()
