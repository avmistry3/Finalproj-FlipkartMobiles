from pyspark.sql.functions import col, trim
def doclean(spark, landingdata, stg_table1):
    project_id = "eng-origin-408719"
    dataset_id = "mobiledata"

    df = spark.read.format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{landingdata}") \
        .load()
  # remove duplicate , trim whitespace, clean null value row
    df_clean = df.dropDuplicates()
    df_clean = df_clean.select(trim(col("Brand")).alias("Brand"),
                               trim(col("Model")).alias("Model"),
                               trim(col("Color")).alias("Color"),
                               "Memory", "Storage", "Selling_Price", "Original_Price") \
        .filter(df.Memory.isNotNull() & df.Storage.isNotNull())

    #change data type for original_price and selling_price columns
    df_clean = df_clean.withColumn("Selling_Price", col("Selling_Price").cast("integer")) \
                .withColumn("Original_Price", col("Original_Price").cast("integer"))

    df_clean.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{stg_table1}") \
        .option("orderBy", "Selling_Price DESC") \
        .option("temporaryGcsBucket", "tempbucketavm") \
        .mode("overwrite") \
        .save()