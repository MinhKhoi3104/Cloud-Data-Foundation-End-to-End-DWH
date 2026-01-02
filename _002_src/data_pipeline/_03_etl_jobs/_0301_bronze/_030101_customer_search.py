import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _01_config.jar_paths import *
from _02_utils.utils import *
from datetime import date

def _030101_customer_search(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_spark_s3_session(_030101_customer_search)

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

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_030101_customer_search')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = _030101_customer_search(etl_date=args.etl_date)
    exit(0 if success else 1)