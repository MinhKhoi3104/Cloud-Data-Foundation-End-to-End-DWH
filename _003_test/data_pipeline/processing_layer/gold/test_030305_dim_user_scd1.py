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
from pyspark.sql.window import Window
import unicodedata

def test_030305_dim_user_scd1(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("test_030305_dim_user_scd1")

        # Logic code
        ### Read source data (from silver layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/customer_search_keynormalize"
        )

        src_df = spark.read.parquet(src_path)

        # User grouping
        source_df = src_df\
            .select("user_id", "date_log")\
            .withColumn(
                "user_type", 
                when(col("user_id") == lit("00000000"), lit("guest"))
                     .otherwise(lit("registered"))
            )
        
        # Caculate fist and last time user access
        window_des = Window.partitionBy("user_id","user_type")

        source_df = source_df\
            .withColumn("first_search_dt", min("date_log").over(window_des))\
            .withColumn("last_search_dt", max("date_log").over(window_des))
        
        source_df = source_df\
            .select(
                col("user_id").cast(StringType()),
                col("user_type").cast(StringType()),
                col("first_search_dt").cast(TimestampType()),
                col("last_search_dt").cast(TimestampType())
            )\
            .dropDuplicates()
        
        # Target đate
        target_df = read_from_redshift(spark,"gold.dim_user")

        # Unit test
        tests_passed = 0
        cols = ("user_id","user_type","last_search_dt")
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
        print(f"""===== SUMMARY: DIM_USER (GOLD LAYER) PASSED: {tests_passed}/1 =====""")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030305_dim_user_scd1')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030305_dim_user_scd1(etl_date=args.etl_date)
    exit(0 if success else 1)