from pyspark.sql import functions as F
from pyspark.sql.window import Window

def doenrich(spark, stg_table1, enriched_table):
    project_id = "eng-origin-408719"
    dataset_id = "mobiledata"
    reference_dataset = "mobiledata"
    reference_table = "specifications_table"


    df_consume_data_original = spark.read.format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{stg_table1}") \
        .load()

    # Rename the Model column to Brand
    #df_consume_table = df_consume_data_original.withColumnRenamed("Brand", "Brand1")

    # Rank rows based on Selling_Price
    window_spec = Window.orderBy(F.desc("Selling_Price"))
    df_consume_table = df_consume_data_original.withColumn("rank", F.row_number().over(window_spec))

    # Filter out the top 10 rows
   # df_consume_top10 = df_consume_table.filter("rank <= 20")


    # Read the reference data
    df_ref = spark.read.format("bigquery").option("table", f"{project_id}.{reference_dataset}.{reference_table}").load()

    # Join the  rows with the reference table
    df_consume_enriched = df_consume_table.join(df_ref, df_consume_table.Brand == df_ref.Brand1, "inner")

    dffinal_1 = df_consume_enriched.select(df_consume_enriched['Brand'],
    df_consume_enriched['Model'],
    df_consume_enriched['Color'],
    df_consume_enriched['Storage'],
    df_consume_enriched['Selling_Price'],
    df_consume_enriched['OS_Type'],
    df_consume_enriched['Best_Place_to_Buy'])

    dffinal_1.show()
    dffinal_1.write.format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{enriched_table}") \
        .option("orderBy", "Selling_Price DESC") \
        .option("temporaryGcsBucket", "tempbucketavm") \
        .mode("overwrite") \
        .save()


