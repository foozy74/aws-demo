from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read and Transform CSV") \
    .getOrCreate()

# Define S3 bucket and file path
s3_bucket = "futurexskills"
input_file_path = "s3a://{}/bank_prospects.csv".format(s3_bucket)

# Read CSV file into DataFrame
df = spark.read.csv(input_file_path, header=True, inferSchema=True)

# Drop rows where Age is blank
df = df.filter(col("Age").isNotNull())

# Define output file path
output_file_path = "s3a://{}/futurex-transformed/bank_prospects_filtered.csv".format(s3_bucket)

# Write DataFrame to CSV
df.write.csv(output_file_path, header=True, mode="overwrite")

# Stop SparkSession
spark.stop()
