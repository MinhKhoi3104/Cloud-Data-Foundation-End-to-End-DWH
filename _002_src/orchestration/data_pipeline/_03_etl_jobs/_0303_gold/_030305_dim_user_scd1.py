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
from pyspark.sql.window import Window

def _030305_dim_user_scd1(etl_date=None):
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
        spark = create_gold_spark_session("_030305_dim_user_scd1")

        # Read source data (from silver layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/customer_search_keynormalize"
        )

        src_df = spark.read.parquet(src_path)

        # Transform
        """
        Create iceberg table
        """
        spark.sql("""CREATE NAMESPACE IF NOT EXISTS iceberg.gold""")
        spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold.dim_user(
            user_id   string,
            user_type   string,
            first_search_dt   timestamp,
            last_search_dt   timestamp
        )
        USING iceberg;
        """)

        # User grouping
        tg_df = src_df\
            .select("user_id", "date_log")\
            .withColumn(
                "user_type", 
                when(col("user_id") == lit("00000000"), lit("guest"))
                     .otherwise(lit("registered"))
            )
        
        # Calculate first and last time user access
        window_des = Window.partitionBy("user_id","user_type")

        tg_df = tg_df\
            .withColumn("first_search_dt", min("date_log").over(window_des))\
            .withColumn("last_search_dt", max("date_log").over(window_des))
        
        tg_df = tg_df\
            .select(
                col("user_id").cast(StringType()),
                col("user_type").cast(StringType()),
                col("first_search_dt").cast(TimestampType()),
                col("last_search_dt").cast(TimestampType())
            )\
            .dropDuplicates()

        tg_df.createOrReplaceTempView("tg_df")

        # SCD 1
        spark.sql("""
        MERGE INTO iceberg.gold.dim_user d
        USING tg_df t
        ON d.user_id = t.user_id
        WHEN MATCHED THEN
            UPDATE SET 
                first_search_dt = coalesce(d.first_search_dt,t.first_search_dt),
                last_search_dt = t.last_search_dt
        WHEN NOT MATCHED THEN
            INSERT (user_id,user_type,first_search_dt,last_search_dt)
            VALUES (t.user_id,t.user_type,t.first_search_dt,t.last_search_dt)
        """)
        print("===== ✅ Completely update and insert new records into iceberg.gold.dim_user ! =====")

        # Create Redshift schema
        sql_query = "CREATE SCHEMA IF NOT EXISTS gold;"
        execute_sql_ddl(spark,sql_query)

        # Create Redshift table
        sql_query = """CREATE TABLE IF NOT EXISTS gold.dim_user (
            user_id   VARCHAR(255),
            user_type  VARCHAR(255),
            first_search_dt  TIMESTAMP,
            last_search_dt  TIMESTAMP
        );"""
        execute_sql_ddl(spark,sql_query)

        """
        Read data from iceberg and insert to Redshift
        """
        # Read data from iceberg
        insert_df = spark.sql("SELECT * FROM iceberg.gold.dim_user")

        # Load to Redshift
        write_to_redshift(insert_df, "gold.dim_user","overwrite")
        print("===== ✅ Completely insert into Readshift: gold.dim_user ! =====")

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_030305_dim_user_scd1')
    parser.add_argument(
        '--etl_date', 
        type=str, 
        required=True,
        help='etl_date (YYYYMMDD). If not provided, will use yesterday for local execution'
    )
    args = parser.parse_args()
    
    success = _030305_dim_user_scd1(etl_date=args.etl_date)