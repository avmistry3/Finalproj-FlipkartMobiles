import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from datacleaning import doclean


@pytest.fixture
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test") \
        .config("spark.driver.extraClassPath", "spark-3.1-bigquery-0.36.1.jar") \
        .config("spark.executor.extraClassPath", "spark-3.1-bigquery-0.36.1.jar") \
        .getOrCreate()


def test_doclean(spark):
    # Mock data
    landingdata = "sample_landing_data"
    stg_table1 = "sample_staging_table"
    project_id = "eng-origin-408719"
    dataset_id = "mobiledata"

    # Mock SparkSession and DataFrame
    mock_df = Mock()
    mock_spark_session = Mock()
    mock_spark_session.read.format.return_value.option.return_value.load.return_value = mock_df
    mock_spark_session.createDataFrame.return_value = mock_df
    mock_spark_session.read.table.return_value = mock_df

    # Patching to mock SparkSession
    with patch("pyspark.sql.SparkSession", return_value=mock_spark_session):
        # Patching to mock functions from pyspark.sql.functions
        with patch("pyspark.sql.functions.col", col), \
                patch("pyspark.sql.functions.trim", trim):
            # Call function to be tested
            doclean(spark, landingdata, stg_table1)

            # Asserts
            mock_spark_session.read.format.assert_called_once_with("bigquery")
            mock_spark_session.read.format().option.assert_called_once_with("table",
                                                                            f"{project_id}.{dataset_id}.{landingdata}")
            mock_spark_session.read.format().option().load.assert_called_once()
            mock_df.dropDuplicates.assert_called_once()
            mock_df.select.assert_called_once_with(trim(col("Brand")).alias("Brand"),
                                                   trim(col("Model")).alias("Model"),
                                                   trim(col("Color")).alias("Color"),
                                                   "Memory", "Storage", "Selling_Price", "Original_Price")
            mock_df.filter.assert_called_once()
            mock_df.withColumn.assert_called()
            mock_df.write.format.assert_called_once_with("bigquery")
            mock_df.write.option.assert_called_once_with("table", f"{project_id}.{dataset_id}.{stg_table1}")
            mock_df.write.option.assert_called_with("temporaryGcsBucket", "tempbucketavm")
            mock_df.write.mode.assert_called_once_with("overwrite")
            mock_df.write.save.assert_called_once()


if __name__ == "__main__":
    pytest.main()
