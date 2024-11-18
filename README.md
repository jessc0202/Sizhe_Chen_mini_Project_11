[![CI](https://github.com/jessc0202/Sizhe_Chen_mini_Project_11/actions/workflows/cicd.yml/badge.svg)](https://github.com/jessc0202/Sizhe_Chen_mini_Project_11/actions/workflows/cicd.yml)

## Data Pipeline with Databricks

**Overview:**
The Data Extraction and Transformation Pipeline project retrieves and processes data using Databricks, in conjunction with the Databricks API and various Python libraries.

**Key Components:**
1. **Data Extraction:**
   - Uses the `requests` library to retrieve data from a specified source.
   - Downloads and stores the data in the Databricks FileStore.

2. **Databricks Environment Setup:**
   - Establishes a connection to the Databricks environment using environment variables for authentication (`SERVER_HOSTNAME` and `ACCESS_TOKEN`).
   - Sets up the Databricks environment with necessary libraries, including `python-dotenv` for managing environment variables.

3. **Data Transformation and Load:**
    - Transforms the CSV file into a Spark DataFrame, which is then converted into a Delta Lake Table and stored in the Databricks environment.

4. **Query Transformation and Visualization:**
   - Defines a Spark SQL query to apply transformations to the retrieved data.
   - Utilizes the transformed Spark DataFrame to create visualizations of the data.

5. **File Path Checking for `make test`:**
   - Implements a function to check if a specified file path exists in the Databricks FileStore.
   - As the majority of functions work exclusively within the Databricks environment, GitHub Actions cannot directly access the data in Databricks. Instead, the pipeline checks connectivity to the Databricks API.

6. **Automated Trigger via GitHub Push:**
    - Uses the Databricks API to run a job in the Databricks workspace. When a user pushes to this repository, it initiates a job run on Databricks.

**Preparation:**
1. Set up a Databricks workspace on Azure.
2. Connect GitHub to the Databricks workspace.
3. Create a global init script on the Databricks cluster to store environment variables.
4. Configure a Databricks cluster that supports PySpark.
5. Clone this repository into the Databricks workspace.
6. Create a job on Databricks to orchestrate the pipeline.
7. Set up tasks within the job:
   - **Extract Task (Data Source)**: `mylib/extract.py`
   - **Transform and Load Task (Data Sink)**: `mylib/transform_load.py`
   - **Query and Visualization Task**: `mylib/query_viz.py`

## Job Run from Automated Trigger
The pipeline is automatically triggered when a push to GitHub is detected. This triggers the pipeline to execute the extraction, transformation, and visualization tasks on Databricks.

![Sample Databricks Pipeline Job](https://github.com/SizheChen/Sizhe_Chen_mini_Project_11/assets/your_image_url_here)

## Check format and test errors
1. Open Codespaces or run the repository locally with the terminal open.
2. Format code with `make format`.
3. Lint code with `make lint`.

## Sample Visualizations from Query:

Below are example visualizations generated from the transformed query data.

![Visualization 1](server.png)
![Visualization 2](surface.png)

## References
1. https://github.com/nogibjj/python-ruff-template
2. https://hypercodelab.com/docs/spark/databricks-platform/global-env-variables
3. https://docs.databricks.com/en/dbfs/filestore.html
4. https://learn.microsoft.com/en-us/azure/databricks/delta/
5. https://learn.microsoft.com/en-us/training/paths/data-engineer-azure-databricks/
6. https://docs.databricks.com/en/getting-started/data-pipeline-get-started.html