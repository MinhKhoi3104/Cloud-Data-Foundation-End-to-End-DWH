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

def test_030307_bridge_user_plan(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("test_030307_bridge_user_plan")

        # Logic code
        ### Read source data (from silver layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/user_plans_map"
        )

        src_df = spark.read.parquet(src_path)

        # Read data from dim_subscription
        dim_subscription = read_from_redshift(spark,"gold.dim_subscription")

        source_df = src_df.alias("s")\
            .join(dim_subscription.alias("d"), 
                (lower(trim(col("s.plan_name"))) == col("d.plan_name")) & (lower(trim(col("s.plan_type")))== col("d.plan_type")),
                "left"
            )\
            .withColumn("is_effective", lit(1))\
            .withColumn("first_effective_date", col("s.date_log"))\
            .withColumn("recent_effective_date", col("s.date_log"))\
            .withColumn("last_effective_date", lit(None).cast(DateType()))\
            .select(
                col("s.user_id"),
                col("d.subscription_key"),
                col("is_effective"),
                col("first_effective_date"),
                col("recent_effective_date"),
                col("last_effective_date")
            )\
            .dropDuplicates()

        # Target đate
        target_df = read_from_redshift(spark,"gold.bridge_user_plan")
        cols = ("user_id","subscription_key","is_effective","recent_effective_date","last_effective_date")

        # Unit test
        tests_passed = 0
        # Test minus
        """
        The test means target data is right or wrong and data is loaded enough or not
        """
        minus_df = source_df.select(*cols).subtract(target_df.select(*cols))
        if  minus_df.isEmpty():
            print("===== ✅ Passed the minus data test...=====")
            tests_passed += 1
        else:
            print("===== ❌ Failed the minus data test...=====")
            tests_passed += 0
        
        # Test Summary
        print(f"""===== SUMMARY: BRIDGE_USER_PLAN (GOLD LAYER) PASSED: {tests_passed}/1 =====""")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030307_bridge_user_plan')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030307_bridge_user_plan(etl_date=args.etl_date)
    exit(0 if success else 1)
