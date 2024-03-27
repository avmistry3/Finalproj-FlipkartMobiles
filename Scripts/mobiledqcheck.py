from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Duplicate Check") \
    .getOrCreate()

project_id = "eng-origin-408719"
dataset_id = "mobiledata"

# Load data from stg_table1
stg_table1_df = spark.read.format("bigquery").option("table", "project_id.dataset_id.stg_table1").load()

# Check for duplicates
duplicate_count = stg_table1_df.count() - stg_table1_df.dropDuplicates().count()

# Print the number of duplicates found

print("---------------------------")
print(f"Number of duplicate records in stg_table1: {duplicate_count}")

print("---------------------------")

# Stop SparkSession
spark.stop()
