import os
import sys
from datetime import date, timedelta

try:
    from data_pipeline._02_utils.utils import *
except ImportError:
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, '..', '..', '..')
    config_path = os.path.abspath(config_path)
    if config_path not in sys.path:
        sys.path.insert(0, config_path)
    # Retry import after path setup
    from data_pipeline._02_utils.utils import *

def _030101_customer_search(etl_date=None):
    spark = None
    try:
        # Default etl_date = yesterday for local execution
        if etl_date is None:
            if os.getenv("AIRFLOW_HOME"):
                raise ValueError("etl_date must be provided when running via Airflow")
            else:
                etl_date = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
                print(f"Local run default etl_date={etl_date}")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_bronze_spark_session("_030101_customer_search")

        # Build S3 path according to etl_date
        s3_path = (
            f"{S3_DATALAKE_PATH}"
            f"/customer_search_log_data/{etl_date}"
        )

        # Read raw data 
        print(f"===== Reading data from: {s3_path} =====")
        df = spark.read.parquet(s3_path)

        # Show source data
        print("===== Showing source data...=====")
        df.show(5, truncate=False)

        # ETL CODE
        s3_bronze_path = f"{S3_DATALAKE_PATH}/bronze/customer_search"

        # Ensure s3 bronze path is exist, if not create it
        ensure_s3_prefix(spark, s3_bronze_path)

        # Load data to Bronze Layer
        print("===== Loading data to Bronze Layer... =====")
        df.write.format("parquet")\
            .mode("overwrite")\
            .save(s3_bronze_path)

        print("===== ✅ Load data to Bronze Layer successfully... =====")

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_030101_customer_search')
    parser.add_argument(
        '--etl_date', 
        type=str, 
        required=True,
        help='etl_date (YYYYMMDD). If not provided, will use yesterday for local execution'
    )
    args = parser.parse_args()
    
    success = _030101_customer_search(etl_date=args.etl_date)