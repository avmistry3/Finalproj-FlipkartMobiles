




![Project Architect drawio](https://github.com/avmistry3/Finalproj-FlipkartMobiles/assets/51489015/210951e9-e31e-4b6c-a554-5e57f06db4bd)







This is our batch ELT pipeline diagram.
Our project revolves around building an ELT: Extract, Load, Transform, pipeline to process the data using various GCP services and tools.Throughout our project, we used several GCP services and tools including: Cloud storage, cloud function, Big query, and Data proc. 
The Whole pipeline is orchestrated by data composer airflow and the Data is visualized by using the data studio.

Now lets look at the workflow of our pipeline:

1. Extract: The first task will ingest raw data into Google cloud storage.

2. Load: The second task will upload the CSV file in google cloud. This action will trigger the cloud function and initiate the airflow Dag. Then data will be loaded on Google Big query.

3. Transform: The third task will be using DataProc and pyspark to clean, enrich and aggregate the data making it ready for analysis.

            Data Quality Check: Before we enrich the data, there will be a data quality check done. 
            quality check, Data enrichment will be done. This will enhance the dataset with additional information. 
            Following that, data aggregation will be done. Here the data is
   
5. Consumption: The data is loaded back into BigQuery, completing the pipeline.
   
6. The data is visualized using Data Studio. 

