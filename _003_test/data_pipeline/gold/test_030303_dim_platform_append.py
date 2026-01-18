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

def test_030303_dim_platform_append(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("test_030303_dim_platform_append")

        # Logic code
        ### Read source data (from silver layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/customer_search_keynormalize"
        )
        src_df = spark.read.parquet(src_path)

        # Process platform logic
        tg_df = src_df.select("platform")

        # Normalize platform and derive device_type
        # Same transformation logic as in the ETL job
        source_df = tg_df.withColumn(
            "platform_normalized",
            when(col("platform").isNotNull(), lower(trim(col("platform"))))
            .otherwise("unknown")
        ).withColumn(
            "device_type",
            when(col("platform_normalized").isin("android", "ios"), "mobile")
            .when(col("platform_normalized").like("%smart%"), "smarttv")
            .when(col("platform_normalized").like("%ottbox%"), "ottbox")
            .when(col("platform_normalized").like("%web%"), "web")
            .otherwise("others")
        ).select(
            col("platform_normalized").alias("platform"),
            col("device_type")
        ).distinct()
        
        # Target data
        target_df = read_from_redshift(spark, "gold.dim_platform")
        target_df = target_df.select("platform", "device_type")

        # Unit test
        tests_passed = 0
        # Test minus
        """
        The test means target data is right or wrong and data is loaded enough or not
        """
        minus_df = source_df.subtract(target_df)
        if minus_df.isEmpty():
            print("===== ✅ Passed the minus data test...=====")
            tests_passed += 1
        else:
            print("===== ❌ Failed the minus data test...=====")
            print(f"Row count in minus_df: {minus_df.count()}")
            print("Showing first 20 rows of minus_df:")
            minus_df.show()
            tests_passed += 0
        
        # Test Summary
        print(f"""===== SUMMARY: DIM_PLATFORM (GOLD LAYER) PASSED: {tests_passed}/1 =====""")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030303_dim_platform_append')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030303_dim_platform_append(etl_date=args.etl_date)
    exit(0 if success else 1)
