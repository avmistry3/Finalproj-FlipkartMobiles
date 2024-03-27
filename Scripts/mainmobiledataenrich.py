import pyspark.sql as sql
import argparse
from mobiledataenrichment import (doenrich)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark DataFrame Assignment")
    parser.add_argument("stg_table1", help="Name final output table")
    parser.add_argument("enriched_table", help="Path to the enriched output table in bq")

    # Parse the arguments
    args = parser.parse_args()
    spark = sql.SparkSession.builder.appName("Enrich Data").getOrCreate()
    doenrich(spark, args.stg_table1, args.enriched_table)
