from pyspark.sql import SparkSession
import argparse
from datacleaning import doclean

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pyspark dataframe Project")
    parser.add_argument("landingdata", help="name of the Bigquery table to read data from")
    parser.add_argument("stg_table1", help="name of staging table")

    # parse the arguments

    args = parser.parse_args()
    spark = SparkSession.builder.appName(" Clean data").getOrCreate()
    doclean(spark, args.landingdata,
            args.stg_table1)