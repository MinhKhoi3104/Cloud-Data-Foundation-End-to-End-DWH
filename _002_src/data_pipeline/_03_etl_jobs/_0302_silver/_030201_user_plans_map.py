import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _01_config.jar_paths import *
from _02_utils.utils import *
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import *

def _030201_user_plans_map(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_silver_spark_session("_030201_user_plans_map")

        # Read source data (bronze layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/bronze/customer_search"
        )

        src_data = spark.read.parquet(src_path)

        # Transform
        tg_exploded = src_data\
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
        
        tg_df = (
            tg_exploded
            .withColumn("plan_name", trim(split(col("plan_raw"), ":").getItem(0)))
            .withColumn("plan_type", trim(split(col("plan_raw"), ":").getItem(1)))
            .select(
                col("user_id").cast(IntegerType()), 
                col("plan_name").cast(StringType()), 
                col("plan_type").cast(StringType()), 
                col("datetime").cast(TimestampType()).alias("search_datetime"),
                col("datetime").cast(DateType()).alias("date_log"))
        )
        tg_df = tg_df.filter(
            col("plan_name").isNotNull() &
            col("plan_type").isNotNull()
        )

        print("===== silver.user_plans_map... =====")
        tg_df.show(5, truncate=False)

        # Load
        s3_silver_path = f"{S3_DATALAKE_PATH}/silver/user_plans_map"

        # Ensure s3 silver path is exist, if not create it
        ensure_s3_prefix(spark, s3_silver_path)

        # Load data to Silver Layer
        print("===== Loading data to Silver Layer... =====")
        tg_df.write.format("parquet")\
            .mode("overwrite")\
            .save(s3_silver_path)

        print("===== ✅ Load data to silver.user_plans_map successfully !... =====")
         
        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_030201_user_plans_map')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = _030201_user_plans_map(etl_date=args.etl_date)
    exit(0 if success else 1)