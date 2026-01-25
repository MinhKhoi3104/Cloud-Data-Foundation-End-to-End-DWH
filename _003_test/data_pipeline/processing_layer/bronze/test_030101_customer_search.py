import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from config.jar_paths import *
from utils.utils import *
from datetime import date

def test_030101_customer_search(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_bronze_spark_session("test_030101_customer_search")

        # Source data
        source_path = (
            f"{S3_DATALAKE_PATH}"
            f"/customer_search_log_data/{etl_date}"
        )

        source_data = spark.read.parquet(source_path)

        # Target data
        target_path = (
            f"{S3_DATALAKE_PATH}"
            "/bronze/customer_search"
        )

        target_data = spark.read.parquet(target_path)

        # Unit test
        tests_passed = 0
        # Count test (count source data and target data)
        if target_data.count() == source_data.count():
            print("===== ✅ Passed the count data test...=====")
            tests_passed += 1
        else: 
            print("===== ❌ Failed the count data test...=====")
            tests_passed += 0
        
        # Test minus
        """
        The test means target data is right or wrong and data is loaded enough or not
        """
        minus_df = target_data.subtract(source_data)
        if  minus_df.isEmpty():
            print("===== ✅ Passed the minus data test...=====")
            tests_passed += 1
        else:
            print("===== ❌ Failed the minus data test...=====")
            tests_passed += 0
        
        # Test Summary
        print(f"""===== SUMMARY: CUSTOMER SEARCH (BRONZE LAYER) PASSED: {tests_passed}/2 =====""")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030101_customer_search')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030101_customer_search(etl_date=args.etl_date)
    exit(0 if success else 1)