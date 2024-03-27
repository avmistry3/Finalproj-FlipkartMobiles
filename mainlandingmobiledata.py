from pyspark.sql import SparkSession
import argparse
from landingdata import doWork as doWork2bq


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description ="Pyspark Dataframe project")
    parser.add_argument("inputfile", help="Name of CSV file to read data from")
    parser.add_argument("landingdata", help="path to the ouput table in bq ")

    #parse the arguments
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Read CSV file").getOrCreate()
    doWork2bq(spark, args.inputfile, args.landingdata)