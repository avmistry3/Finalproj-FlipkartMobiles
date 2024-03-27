from pyspark.sql.functions import sum, avg, desc, col

def dohighestselling(spark, enriched_table , consume_table):
    project_id = "eng-origin-408719"
    dataset_id = "mobiledata"

    df = spark.read.format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{enriched_table}") \
        .load()


    # Calculate the average selling price of the entire dataset
    average_selling_price = df.select(avg("Selling_Price")).collect()[0][0]

    # Filter records where the selling price is greater than the average selling price
    filtered_data = df.filter(df["Selling_Price"] > average_selling_price) \
                       .orderBy(desc("Selling_Price"))

    filtered_data.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{consume_table}") \
        .option("orderBy", "Selling_Price DESC") \
        .option("temporaryGcsBucket", "tempbucketavm") \
        .mode("overwrite") \
        .save()

    filtered_data.show()

    # Determine the highest-selling mobile
    highest_selling_mobile = filtered_data.orderBy(desc("Selling_Price")).first()

    # Print the highest-selling
    print("========================================")
    print("Highest Selling Mobile:")
    print("Brand:", highest_selling_mobile["Brand"])
    print("Model:", highest_selling_mobile["Model"])
    print("Selling Price:", highest_selling_mobile["Selling_Price"])
    print("===========================================")





    # Print success message
    print("Data successfully written to BigQuery table.")
