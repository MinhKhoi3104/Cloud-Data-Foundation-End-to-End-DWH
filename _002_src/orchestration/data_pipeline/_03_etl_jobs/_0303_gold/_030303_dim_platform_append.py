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

def _030303_dim_platform_append(etl_date=None):
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
        spark = create_gold_spark_session("_030303_dim_platform_append")

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
        spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold.dim_platform(
            platform_key   int,
            platform       string,
            device_type    string
        )
        USING iceberg;
        """)

        # Process platform logic
        tg_df = src_df.select("platform")

        # Normalize platform and derive device_type
        tg_df = tg_df.withColumn(
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

        # Extract old data of dim_platform tbl
        tg_df_old = spark.sql("SELECT * FROM iceberg.gold.dim_platform")
        # Choose specific columns to compare
        tg_df_old = tg_df_old.select("platform", "device_type")
        # Just choose new data
        insert_df = tg_df.subtract(tg_df_old)
        # Create surrogate key
        insert_df = allocate_surrogate_keys(
            spark,
            insert_df,
            "dim_platform",
            "platform",
            "platform_key"
        )

        insert_records_count = insert_df.limit(1).count()

        # Create Redshift schema
        sql_query = "CREATE SCHEMA IF NOT EXISTS gold;"
        execute_sql_ddl(spark,sql_query)

        # Create Redshift table
        sql_query = """CREATE TABLE IF NOT EXISTS gold.dim_platform (
            platform_key   INTEGER,
            platform       VARCHAR(255),
            device_type    VARCHAR(255)
        );"""
        execute_sql_ddl(spark,sql_query)

        # Load to Redshift
        """
        Load data to Redshift first
        """
        if insert_records_count > 0:

            # LOAD
            write_to_redshift(insert_df, "gold.dim_platform","append")
            print("===== ✅ Completely insert new records into Readshift: gold.dim_platform! =====")
        else:
            print('===== No records need to insert! =====') 
        # Load to Iceberg
        """
        Load data to Iceberg second
        """
        if insert_records_count > 0:

            # LOAD
            insert_df.writeTo("iceberg.gold.dim_platform").append()
            print("===== ✅ Completely insert new records into iceberg.gold.dim_platform! =====")
        else:
            print('===== No records need to insert! =====')

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_030303_dim_platform_append')
    parser.add_argument(
        '--etl_date', 
        type=str, 
        required=True,
        help='etl_date (YYYYMMDD). If not provided, will use yesterday for local execution'
    )
    args = parser.parse_args()
    
    success = _030303_dim_platform_append(etl_date=args.etl_date)
