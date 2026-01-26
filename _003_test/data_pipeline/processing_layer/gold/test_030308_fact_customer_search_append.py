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

def test_030308_fact_customer_search_append(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("test_030308_fact_customer_search_append")

        # Read source data (from silver layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/customer_search_keynormalize"
        )

        src_df = spark.read.parquet(src_path)

        # Read data from dim_network
        dim_network = read_from_redshift(spark,"gold.dim_network")

        # Read data from dim_platform
        dim_platform = read_from_redshift(spark,"gold.dim_platform")

        # Read data from dim_category
        dim_category = read_from_redshift(spark,"gold.dim_category")

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
        
        source_df = tg_df.alias("t")\
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
        
        # Target đate
        target_df = read_from_redshift(spark,"gold.fact_customer_search")

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
        print(f"""===== SUMMARY: FACT_CUSTOMER_SEARCH (GOLD LAYER) PASSED: {tests_passed}/1 =====""")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030308_fact_customer_search_append')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030308_fact_customer_search_append(etl_date=args.etl_date)
    exit(0 if success else 1)