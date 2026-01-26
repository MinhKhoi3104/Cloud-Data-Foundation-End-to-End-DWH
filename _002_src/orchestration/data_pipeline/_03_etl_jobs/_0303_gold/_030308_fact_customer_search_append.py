import os
import sys
from datetime import date, timedelta

try:
    from data_pipeline._02_utils.utils import *
except ImportError:
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, '..', '..', '..')
    config_path = os.path.abspath(config_path)
    if config_path not in sys.path:
        sys.path.insert(0, config_path)
    # Retry import after path setup
    from data_pipeline._02_utils.utils import *

from pyspark.sql.functions import *
from pyspark.sql.types import *

def _030308_fact_customer_search_append(etl_date=None):
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
        spark = create_gold_spark_session("_030308_fact_customer_search_append")

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
        spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold.fact_customer_search(
            event_id   string,
            datetime_log       timestamp,
            date_key    string,
            user_id       string,
            keyword       string,
            keyword_slug       string,
            category       string,
            action       string,
            network_key       int,
            platform_key       int,
            main_keyword_category       int,
            sub1_keyword_category       int,
            sub2_keyword_category       int,
            sub3_keyword_category       int
        )
        USING iceberg
        PARTITIONED BY (date_key)
        ;
        """)

        # Read data from dim_network
        dim_network = broadcast(read_from_redshift(spark,"gold.dim_network"))

        # Read data from dim_platform
        dim_platform = broadcast(read_from_redshift(spark,"gold.dim_platform"))

        # Read data from dim_category
        dim_category = broadcast(read_from_redshift(spark,"gold.dim_category"))

        """
        Transform to create fact table
        """

        # split keyword_category to 4 levels columns and add network_type column
        tg_df = src_df\
            .withColumn("main_keyword_category", trim(split(col("keyword_category"), ";").getItem(0)))\
            .withColumn("sub1_keyword_category", trim(split(col("keyword_category"), ";").getItem(1)))\
            .withColumn("sub2_keyword_category", trim(split(col("keyword_category"), ";").getItem(2)))\
            .withColumn("sub3_keyword_category", trim(split(col("keyword_category"), ";").getItem(3)))\
            .withColumn(
                "network_type",
                when(lower(trim(col("networktype"))) == "wifi", "wifi")
                    .when(lower(trim(col("networktype"))).isin("ethernet","cab"), "ethernet")\
                        .when(lower(trim(col("networktype"))).isin("3g", "4g","5g","wwan"), "cellular")\
                            .when(lower(trim(col("networktype"))).like("%tethering%"), "tethering")\
                                .when(lower(trim(col("networktype"))) == "no_internet", "no_internet")\
                                    .otherwise("others")
            )
        
        tg_df = tg_df.alias("t")\
            .join(dim_network.alias("dn"), (lower(trim(col("t.proxy_isp"))) == col("dn.proxy_isp")) & (col("t.network_type") == col("dn.network_type")),"left")\
            .join(dim_platform.alias("dp"),lower(trim(col("t.platform"))) == col("dp.platform"), "left")\
            .join(dim_category.alias("dc"),trim(col("t.main_keyword_category")) == col("dc.category_name"), "left")\
            .join(dim_category.alias("dc1"),trim(col("t.sub1_keyword_category")) == col("dc1.category_name"), "left")\
            .join(dim_category.alias("dc2"),trim(col("t.sub2_keyword_category")) == col("dc2.category_name"), "left")\
            .join(dim_category.alias("dc3"),trim(col("t.sub3_keyword_category")) == col("dc3.category_name"), "left")\
            .select(
                col("t.event_id").cast(StringType()),
                col("t.date_log").alias("datetime_log").cast(TimestampType()),
                col("t.date_key").cast(StringType()),
                col("t.user_id").cast(StringType()),
                when(col("keyword_normalized") != lit("not_matched"), col("keyword_normalized"))
                    .otherwise(col("original_keyword"))
                        .alias("keyword").cast(StringType()),
                when(col("keyword_normalized_slug") != lit("not_matched"), col("keyword_normalized_slug"))
                    .otherwise(col("original_keyword_slug"))
                        .alias("keyword_slug").cast(StringType()),
                col("t.category").cast(StringType()),
                col("t.action").cast(StringType()),
                col("dn.network_key").cast(IntegerType()),
                col("dp.platform_key").cast(IntegerType()),
                col("dc.category_key").alias("main_keyword_category").cast(IntegerType()),
                col("dc1.category_key").alias("sub1_keyword_category").cast(IntegerType()),
                col("dc2.category_key").alias("sub2_keyword_category").cast(IntegerType()),
                col("dc3.category_key").alias("sub3_keyword_category").cast(IntegerType())
            )\
            .dropDuplicates()
        
        # Extract old data of fact_customer_search tbl
        tg_df_old = spark.sql(f"""
            SELECT event_id
            FROM iceberg.gold.fact_customer_search
            WHERE date_key = '{etl_date}'
        """)

        # Just choose new data
        insert_df = tg_df.alias("t") \
            .join(
                tg_df_old.select("event_id").alias("o"),
                col("t.event_id") == col("o.event_id"),
                "left_anti"
            )
        

        insert_records_count = insert_df.limit(1).count()

        # Create Redshift schema
        sql_query = "CREATE SCHEMA IF NOT EXISTS gold;"
        execute_sql_ddl(spark,sql_query)

        # Create Redshift table
        sql_query = """CREATE TABLE IF NOT EXISTS gold.fact_customer_search (
            event_id   VARCHAR(255),
            datetime_log       TIMESTAMP,
            date_key    VARCHAR(255),
            user_id       VARCHAR(255),
            keyword       VARCHAR(255),
            keyword_slug       VARCHAR(255),
            category       VARCHAR(255),
            action       VARCHAR(255),
            network_key       INTEGER,
            platform_key       INTEGER,
            main_keyword_category       INTEGER,
            sub1_keyword_category       INTEGER,
            sub2_keyword_category       INTEGER,
            sub3_keyword_category       INTEGER
        );"""
        execute_sql_ddl(spark,sql_query)

       # Load to Redshift
        """
        Load data to Redshift first
        """
        if insert_records_count > 0:

            # LOAD
            write_to_redshift(insert_df, "gold.fact_customer_search","append")
            print("===== ✅ Completely insert new records into Readshift: gold.fact_customer_search! =====")
        else:
            print('===== No records need to insert! =====')

        # Load to Iceberg
        """
        Load data to Iceberg second
        """
        if insert_records_count > 0:

            # LOAD
            insert_df.writeTo("iceberg.gold.fact_customer_search").append()
            print("===== ✅ Completely insert new records into iceberg.gold.fact_customer_search! =====")

        else:
            print('===== No records need to insert! =====')

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='_030308_fact_customer_search_append')
    parser.add_argument(
        '--etl_date', 
        type=str, 
        required=True,
        help='etl_date (YYYYMMDD). If not provided, will use yesterday for local execution'
    )
    args = parser.parse_args()
    
    success = _030308_fact_customer_search_append(etl_date=args.etl_date)