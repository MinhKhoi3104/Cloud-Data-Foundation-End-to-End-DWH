import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..','..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _002_src.data_pipeline._01_config.jar_paths import * 
from _002_src.data_pipeline._02_utils.utils import *
from _002_src.data_pipeline._02_utils.surrogate_key_registry import *
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import *

def _030306_dim_subscription_append(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            config_etl_date = date.today().strftime("%Y%m%d")
        else:
            config_etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("_030306_dim_subscription_append")

        # Read source data (from silver layer)
        # Assuming the path follows the standard convention
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/user_plans_map"
        )
        # If the file format might be parquet or delta, standard here seems to be parquet based on _030302
        src_df = spark.read.parquet(src_path)

        # Transform
        """
        Create iceberg table
        """
        spark.sql("""CREATE NAMESPACE IF NOT EXISTS iceberg.gold""")
        spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold.dim_subscription(
            subscription_key int,
            plan_name        string,
            plan_type        string,
            is_paid          int
        )
        USING iceberg;
        """)

        # Normalize
        # plan_name: lower(trim(plan_name))
        # plan_type: lower(trim(plan_type))
        # is_paid: case when lower(trim(plan_type)) = 'giftcode' then 0 else 1 end
        
        tg_df = src_df\
            .select("plan_name", "plan_type")\
            .withColumn("plan_name", lower(trim(col("plan_name"))))\
            .withColumn("plan_type", lower(trim(col("plan_type"))))\
            .withColumn(
                "is_paid",
                when(col("plan_type") == "giftcode", lit(0)).otherwise(lit(1))
            )\
            .distinct()
        
        # Extract old data of dim_subscription tbl
        tg_df_old = spark.sql("SELECT * FROM iceberg.gold.dim_subscription")

        # Choose specific columns to compare (exclude key)
        tg_df_old = tg_df_old.select("plan_name", "plan_type", "is_paid")

        # Just choose new data
        insert_df = tg_df.subtract(tg_df_old)

        # Create surrogate key
        # Using plan_name as the business key for ordering
        insert_df = allocate_surrogate_keys(
            spark,
            insert_df,
            "dim_subscription",
            "plan_name",
            "subscription_key"
        )
        insert_df = insert_df.cache()
        insert_records_count = insert_df.count()

        if insert_records_count > 0:

            print(f'===== The number of insert records: {insert_records_count} =====')

            # LOAD to Iceberg
            # Reorder columns to match table definition: subscription_key, plan_name, plan_type, is_paid
            insert_df = insert_df.select("subscription_key", "plan_name", "plan_type", "is_paid")
            
            insert_df.writeTo("iceberg.gold.dim_subscription").append()
            print("===== ✅ Completely insert new records into iceberg.gold.dim_subscription! =====")

        else:
            print('===== No records need to insert! =====')
        
        
        # Create Redshift schema
        sql_query = "CREATE SCHEMA IF NOT EXISTS gold;"
        execute_sql_ddl(spark,sql_query)

        # Create Redshift table
        sql_query = """CREATE TABLE IF NOT EXISTS gold.dim_subscription (
            subscription_key INTEGER,
            plan_name        VARCHAR(255),
            plan_type        VARCHAR(255),
            is_paid          INTEGER
        );"""
        execute_sql_ddl(spark,sql_query)

        """
        Read data from iceberg and insert to Redshift
        """

        # Load to Redshift
        if insert_records_count > 0:

            print(f'===== The number of insert records: {insert_records_count} =====')

            # LOAD
            write_to_redshift(insert_df, "gold.dim_subscription","append")
            print("===== ✅ Completely insert new records into Redshift: gold.dim_subscription! =====")

        else:
            print('===== No records need to insert! =====')
        

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_030306_dim_subscription_append')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = _030306_dim_subscription_append(etl_date=args.etl_date)
    exit(0 if success else 1)
