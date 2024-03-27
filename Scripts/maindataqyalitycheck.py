import argparse
import pyspark.sql as sql
from dataqualitycheck import perform_data_quality_check

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark DataFrame Assignment")
    parser.add_argument("bq_table", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()

    # Initialize Spark session
    spark = sql.SparkSession.builder.appName("Data Quality Check").getOrCreate()

    # Read cleaned data from staging table
    df = spark.read.format("bigquery").option("table", args.bq_table).load()

    # Perform data quality check
    total_null_count, null_counts = perform_data_quality_check(df)

    # Print data quality check results
    print("Total NULL values found:", total_null_count)
    print("NULL counts by column:", null_counts)

    # Stop Spark session
    spark.stop()
