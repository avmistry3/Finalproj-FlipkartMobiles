def doWork(spark, inputfile, landingdata):
    project_id = "eng-origin-408719"
    dataset_id = "mobiledata"

 # read data from cloud storage csv file

    df = spark.read.csv(inputfile, header="true")

    #  Write DataFrame to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{landingdata}") \
        .option("temporaryGcsBucket", "tempbucketavm") \
        .mode("overwrite") \
        .save()