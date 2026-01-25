import os
import sys
from datetime import date, timedelta

try:
    from data_pipeline._02_utils.utils import *
    from data_pipeline._02_utils.surrogate_key_registry import *
except ImportError:
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, '..', '..', '..')
    config_path = os.path.abspath(config_path)
    if config_path not in sys.path:
        sys.path.insert(0, config_path)
    # Retry import after path setup
    from data_pipeline._02_utils.utils import *
    from data_pipeline._02_utils.surrogate_key_registry import *

from pyspark.sql.functions import *
from pyspark.sql.types import *

def _030307_bridge_user_plan(etl_date=None):
    spark = None
    try:
        # Default etl_date = yesterday for local execution
        if etl_date is None:
            if os.getenv("AIRFLOW_HOME"):
                raise ValueError("etl_date must be provided when running via Airflow")
            else:
                etl_date = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
                print(f"Local run default etl_date={etl_date}")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("_030307_bridge_user_plan")

        # Read source data (from silver layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/user_plans_map"
        )

        src_df = spark.read.parquet(src_path)

        # Read data from dim_subscription
        dim_subscription = read_from_redshift(spark,"gold.dim_subscription")

        # Transform
        """
        Create iceberg table
        """
        spark.sql("""CREATE NAMESPACE IF NOT EXISTS iceberg.gold""")
        spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold.bridge_user_plan(
            user_plan_key   int,
            user_id       string,
            subscription_key    int,
            is_effective       int,
            first_effective_date       date,
            recent_effective_date       date,
            last_effective_date       date
        )
        USING iceberg;
        """)

        tg_df = src_df.alias("s")\
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
        
        tg_df.createOrReplaceTempView("tg_df")
        
        """
        (1)
        Execute MERGE funtion
            -> if matched -> update recent_effective_date column (it means this user plans that is recorded does not expired)
            -> if not matched -> set last_effective_date =  old recent_effective_date and is_effective = 0 (it means today the user plan is not recorded = expired)

        (2)
        filter new records -> add surrogate key --> apppend to table
        """
        
        # Thực hiện đánh hết hạn cho các gói
        spark.sql("""
        MERGE INTO iceberg.gold.bridge_user_plan b
        USING tg_df o
        ON b.user_id = o.user_id
        AND b.subscription_key = o.subscription_key

        WHEN MATCHED THEN
        UPDATE SET
            recent_effective_date = o.recent_effective_date

        WHEN NOT MATCHED BY SOURCE
        AND b.is_effective = 1 THEN
        UPDATE SET
            last_effective_date = b.recent_effective_date,
            is_effective = 0
        """)

        # Extract old data of dim_category tbl
        bridge_after_merge_df = spark.sql("SELECT * FROM iceberg.gold.bridge_user_plan")

        # Just choose new data
        insert_df = tg_df.alias("t") \
            .join(
                bridge_after_merge_df.alias("o"),
                (col("t.user_id") == col("o.user_id")) &
                (col("t.subscription_key") == col("o.subscription_key")) &
                (col("o.is_effective") == 1),
                "left_anti"
            )\
            .select("t.*")

        # Create surrogate key
        insert_df = allocate_surrogate_keys(
            spark,
            insert_df,
            "bridge_user_plan",
            "user_id",
            "user_plan_key"
        )

        insert_df = insert_df\
            .select(
                col("user_plan_key").cast(IntegerType()),
                col("user_id").cast(StringType()),
                col("subscription_key").cast(IntegerType()),
                col("is_effective").cast(IntegerType()),
                col("first_effective_date").cast(DateType()),
                col("recent_effective_date").cast(DateType()),
                col("last_effective_date").cast(DateType())
            )

        insert_records_count = insert_df.limit(1).count()

        """
        Load data to Iceberg
        """
        if insert_records_count > 0:

            # LOAD
            insert_df.writeTo("iceberg.gold.bridge_user_plan").append()
            print("===== ✅ Completely insert new records into iceberg.gold.bridge_user_plan! =====")

        else:
            print('===== No records need to insert! =====')
        

        # Create Redshift schema
        sql_query = "CREATE SCHEMA IF NOT EXISTS gold;"
        execute_sql_ddl(spark,sql_query)

        # Create Redshift table
        sql_query = """CREATE TABLE IF NOT EXISTS gold.bridge_user_plan (
            user_plan_key   integer,
            user_id       varchar(255),
            subscription_key    integer,
            is_effective       integer,
            first_effective_date       date,
            recent_effective_date       date,
            last_effective_date       date
        );"""
        execute_sql_ddl(spark,sql_query)

        """
        Read data from iceberg and insert to Redshift
        """
        # Read data from iceberg
        insert_df = spark.sql("SELECT * FROM iceberg.gold.bridge_user_plan")

        # Load to Redshift
        write_to_redshift(insert_df, "gold.bridge_user_plan","overwrite")
        print("===== ✅ Completely insert into Readshift: gold.bridge_user_plan ! =====")

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_030307_bridge_user_plan')
    parser.add_argument(
        '--etl_date', 
        type=str, 
        required=True,
        help='etl_date (YYYYMMDD). If not provided, will use yesterday for local execution'
    )
    args = parser.parse_args()
    
    success = _030307_bridge_user_plan(etl_date=args.etl_date)