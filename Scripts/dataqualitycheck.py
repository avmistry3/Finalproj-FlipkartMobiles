from pyspark.sql.functions import col

def perform_data_quality_check(df):
    # Data quality check logic
    null_counts = {col_name: df.filter(col(col_name).isNull()).count() for col_name in df.columns}
    total_null_count = sum(null_counts.values())

    return total_null_count, null_counts
