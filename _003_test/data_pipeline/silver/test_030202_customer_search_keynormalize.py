import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _002_src.data_pipeline._01_config.jar_paths import *
from _002_src.data_pipeline._02_utils.utils import *
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import *

def test_030202_customer_search_keynormalize(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_silver_spark_session("test_030202_customer_search_keynormalize")

        # Source data
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/bronze/customer_search"
        )

        src_data = spark.read.parquet(src_path)

        source_df = src_data\
            .filter(col("datetime").cast(DateType()) == to_date(lit(etl_date),"yyyyMMdd"))
        
        # Target data
        target_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/customer_search_keynormalize"
        )

        target_df = spark.read.parquet(target_path)

        # Unit test
        tests_passed = 0
        # Count test (count source data and target data)
        if target_df.count() == source_df.count():
            print("===== ✅ Passed the count data test...=====")
            tests_passed += 1
        else: 
            print("===== ❌ Failed the count data test...=====")
            tests_passed += 0
        
        # Test Summary
        print(f"""===== SUMMARY: USER_PLANS_MAP (SILVER LAYER) PASSED: {tests_passed}/1 =====""")
         
        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030202_customer_search_keynormalize')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030202_customer_search_keynormalize(etl_date=args.etl_date)
    exit(0 if success else 1)