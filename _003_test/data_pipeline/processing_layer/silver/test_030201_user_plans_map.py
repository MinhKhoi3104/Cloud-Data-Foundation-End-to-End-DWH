import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from config.jar_paths import *
from utils.utils import *
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import *

def test_030201_user_plans_map(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_silver_spark_session("test_030201_user_plans_map")

        # Source data
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/bronze/customer_search"
        )

        src_data = spark.read.parquet(src_path)

        src_exploded = src_data\
            .filter(col("datetime").cast(DateType()) == to_date(lit(etl_date),"yyyyMMdd"))\
            .withColumn(
            "plan_raw",
            explode(
                when(
                    col("userPlansMap").isNotNull(),
                    col("userPlansMap")
                ).otherwise(array())
            )
        )
        
        source_df = (
            src_exploded
            .withColumn("plan_name", trim(split(col("plan_raw"), ":").getItem(0)))
            .withColumn("plan_type", trim(split(col("plan_raw"), ":").getItem(1)))
            .select(
                col("user_id").cast(IntegerType()), 
                col("plan_name").cast(StringType()), 
                col("plan_type").cast(StringType()), 
                col("datetime").cast(TimestampType()).alias("search_datetime"),
                col("datetime").cast(DateType()).alias("date_log"))
        )
        source_df = source_df.filter(
            col("plan_name").isNotNull() &
            col("plan_type").isNotNull()
        )

        # Target data
        target_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/user_plans_map"
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
        
        # Test minus
        """
        The test means target data is right or wrong and data is loaded enough or not
        """
        minus_df = source_df.subtract(target_df)
        if  minus_df.isEmpty():
            print("===== ✅ Passed the minus data test...=====")
            tests_passed += 1
        else:
            print("===== ❌ Failed the minus data test...=====")
            tests_passed += 0
        
        # Test Summary
        print(f"""===== SUMMARY: USER_PLANS_MAP (SILVER LAYER) PASSED: {tests_passed}/2 =====""")
         
        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030201_user_plans_map')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030201_user_plans_map(etl_date=args.etl_date)
    exit(0 if success else 1)