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

def test_030306_dim_subscription_append(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("test_030306_dim_subscription_append")

        # Logic code
        ### Read source data (from silver layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/user_plans_map"
        )

        src_df = spark.read.parquet(src_path)

        ### Transform
        # plan_name: lower(trim(plan_name))
        # plan_type: lower(trim(plan_type))
        # is_paid: case when lower(trim(plan_type)) = 'giftcode' then 0 else 1 end
        
        source_df = src_df\
            .select("plan_name", "plan_type")\
            .withColumn("plan_name", lower(trim(col("plan_name"))))\
            .withColumn("plan_type", lower(trim(col("plan_type"))))\
            .withColumn(
                "is_paid",
                when(col("plan_type") == "giftcode", lit(0)).otherwise(lit(1))
            )\
            .distinct()

        # Target date
        target_df = read_from_redshift(spark,"gold.dim_subscription")
        target_df = target_df.select("plan_name", "plan_type", "is_paid")

        # Unit test
        tests_passed = 0
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
        print(f"""===== SUMMARY: DIM_SUBSCRIPTION (GOLD LAYER) PASSED: {tests_passed}/1 =====""")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030306_dim_subscription_append')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030306_dim_subscription_append(etl_date=args.etl_date)
    exit(0 if success else 1)
