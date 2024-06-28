from numpy import save
from pyspark.sql import SparkSession
from pyspark import SQLContext
import json
import boto3

spark = SparkSession.builder \
    .appName("OpenLineageExample") \
    .config("spark.jars.packages", "io.openlineage:openlineage-spark_2.12-1.13.1") \
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
    .config('spark.openlineage.transport.type', 'http') \
    .config('spark.openlineage.transport.url', 'http://localhost:8080') \
    .config('spark.openlineage.transport.endpoint', '/api/v1/lineage') \
    .config('spark.openlineage.namespace', 'spark_integration') \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")
sc = spark.sparkContext
sqlContext = SQLContext(sc)
# Your Spark job code
input_path = "C://Users//satishkr//PycharmProjects//pythonProject3//pythonProject//lineage//test.csv"
output_path = "C://Users/satishkr//PycharmProjects//pythonProject3//pythonProject//lineage//dlineage.json"
df = spark.read.csv(input_path, header=True)
third_column = df.columns[2]
df_transformed = df.filter(df[third_column] >= 40)
df_transformed.write.csv(output_path, mode="overwrite", header=True)
df.show()
third_column = df.columns[2]
age = 40
filtered_df = df.filter(df[third_column] >= age)
filtered_df.show()
lineage_df = {
      "job_name": "YourSparkJob",
      "inputs": [input_path],
      "outputs": [output_path],
}
lineage_json = json.dumps(lineage_df)
# Convert dictionary to JSON string
lineage_json = json.dumps(lineage_df, indent=4)  # indent for pretty printing

# Write JSON string to a file
file_path = 'lineage.json'
with open(file_path, 'w') as file:
    file.write(lineage_json)
print(f'Lineage JSON data written to {"C://Users/satishkr//PycharmProjects//pythonProject3//lineage//dlineage.json"}')

# Initialize Glue client
#glue = boto3.client('glue')

# Example S3 location to store lineage information
#s3_bucket = 's3://ddsl-extension-bucket/'
#s3_key = 'lineage.json'

# # Upload lineage JSON to S3
# glue.put_data_catalog_lineage_settings(CatalogId='your-catalog-id', LineageSettings={"LineageData": lineage_json})
spark.stop()