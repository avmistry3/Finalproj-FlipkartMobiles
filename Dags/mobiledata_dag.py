from datetime import timedelta, datetime
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

CLUSTER_NAME = 'dataproc-airflow-cluster'
REGION = 'us-central1'
PROJECT_ID = 'eng-origin-408719'
jar_path = 'gs://mobiledata12/spark-3.1-bigquery-0.36.1.jar'


# SPARK_JOB1 = {
#      "reference": {"project_id": PROJECT_ID},
#      "placement": {"cluster_name": CLUSTER_NAME},
#      "pyspark_job": {
#          "main_python_file_uri": "gs://mobiledata12/Scripts/mainlandingmobiledata.py",
#          "jar_file_uris": [jar_path],
#          "args": ["gs://mobiledata12/inputdata/Flipkart_Mobiles.csv", "landingdata"],
#          "python_file_uris": ["gs://mobiledata12/Scripts/landingdata.py"],
#      }
# }
SPARK_JOB2 = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": "gs://mobiledata12/Scripts/maincleaningbqdata.py",
         "jar_file_uris": [jar_path],
         "args": ["landingdata", "stg_table1"],
         "python_file_uris": ["gs://mobiledata12/Scripts/datacleaning.py"],
     }
}
SPARK_JOB3 = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": "gs://mobiledata12/Scripts/dataqualitycheck.py",
         "jar_file_uris": [jar_path],
         "args": ["stg_table1"],
        # "python_file_uris": ["gs://mobiledata12/Scripts/calculate_higestselling.py"],
     }
}
SPARK_JOB5 = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": "gs://mobiledata12/Scripts/mainhighestselling.py",
         "jar_file_uris": [jar_path],
         "args": ["enriched_table", "consume_table"],
         "python_file_uris": ["gs://mobiledata12/Scripts/calculate_higestselling.py"],
     }
}

SPARK_JOB4 = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": "gs://mobiledata12/Scripts/mainmobiledataenrich.py",
         "jar_file_uris": [jar_path],
         "args": ["stg_table1", "enriched_table"],
         "python_file_uris": ["gs://mobiledata12/Scripts/mobiledataenrichment.py"],
     }
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    }
}

# Define default_args dictionary
default_dag_args = {
    'owner': 'Archana',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'tags': ["dataproc_airflow"],
}

with models.DAG(
    "dataproc_airflow_gcp",
    schedule_interval=None,
    default_args=default_dag_args,
    tags=["dataproc_airflow"],
) as dag:



# tasks and operators

    # landing_task = DataprocSubmitJobOperator(
    #      task_id='Landing',
    #      project_id=PROJECT_ID,
    #      region=REGION,
    #      job=SPARK_JOB1,
    #      dag=dag,
    #
    # )
    # Task to load CSV file into BigQuery

    load_csv_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='mobiledata12',
        source_objects=['inputdata/Flipkart_Mobiles.csv'],
        destination_project_dataset_table='eng-origin-408719.mobiledata.landingdata',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True  # Automatically detect schema from CSV file
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        dag=dag,
    )

    datacleaning_task = DataprocSubmitJobOperator(
        task_id='DataCleaning',
        project_id=PROJECT_ID,
        region=REGION,
        job=SPARK_JOB2,
        dag=dag,

    )
    dataqualitycheck_task = DataprocSubmitJobOperator(
        task_id='DataQualityCheck',
        project_id=PROJECT_ID,
        region=REGION,
        job=SPARK_JOB3,
        dag=dag,

    )
    consume_data = DataprocSubmitJobOperator(
        task_id='Consumedata',
        project_id=PROJECT_ID,
        region=REGION,
        job=SPARK_JOB5,
        dag=dag,

    )
    Enrich_data = DataprocSubmitJobOperator(
        task_id='Enrich_data',
        project_id=PROJECT_ID,
        region=REGION,
        job=SPARK_JOB4,
        dag=dag,

    )
    # pyspark_task5 = DataprocSubmitJobOperator(
    #     task_id='Consumedata',
    #     project_id=PROJECT_ID,
    #     region=REGION,
    #     job=SPARK_JOB5,
    #     dag=dag,
    #
    # )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        dag=dag,

    )


load_csv_to_bq >> create_cluster >> datacleaning_task >> dataqualitycheck_task >> Enrich_data >> consume_data >> delete_cluster

