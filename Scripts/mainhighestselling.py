from pyspark.sql import SparkSession
import argparse
from calculate_higestselling import dohighestselling

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pyspark dataframe Project")
    parser.add_argument("enriched_table", help="name of the Bigquery table to read data from")
    parser.add_argument("consume_table", help="name of staging table")

    # parse the arguments

    args = parser.parse_args()
    spark = SparkSession.builder.appName(" highest selling mobile").getOrCreate()
    dohighestselling(spark, args.enriched_table, args.consume_table)