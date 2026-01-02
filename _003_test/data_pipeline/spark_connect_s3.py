import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _002_src.data_pipeline._01_config.jar_paths import *
from _002_src.data_pipeline._02_utils.utils import *
from _002_src.data_pipeline._01_config.data_storage_config import *
from datetime import date

def _spark_connect_s3_test(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_spark_s3_session(_spark_connect_s3_test)

        print("Testing S3 connection ...")

        # Build S3 path theo etl_date
        s3_path = (
            f"{S3_DATALAKE_PATH}/customer_search_log_data/{etl_date}"
        )

        print(f"Reading data from: {s3_path}")
        try:
            df = spark.read.parquet(s3_path)
            df.show(5)

            print("✅ S3 connection SUCCESS")
        except:
            print("❌ S3 connection SUCCESS but s3 path not found")

        return True

    except Exception as e:
        print("❌ S3 connection FAILED")
        print(f"ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_spark_connect_s3_test')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = _spark_connect_s3_test(etl_date=args.etl_date)
    exit(0 if success else 1)