from pyspark.sql import SparkSession

# Step 1: Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV to BigQuery") \
    .getOrCreate()

# Step 2: Read CSV into DataFrame
csv_path = "gs://mobiledata12/inputdata/Flipkart_Mobiles.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Step 3: Configure BigQuery settings
project_id = "eng-origin-408719"
dataset_id = "mobiledata"
table_name = "moblielanding"

# Step 4: Write DataFrame to BigQuery
df.write \
  .format("bigquery") \
  .option("table", f"{project_id}.{dataset_id}.{table_name}") \
  .option("temporaryGcsBucket", "tempbucketavm") \
  .mode("overwrite") \
  .save()

# Stop Spark session
spark.stop()
