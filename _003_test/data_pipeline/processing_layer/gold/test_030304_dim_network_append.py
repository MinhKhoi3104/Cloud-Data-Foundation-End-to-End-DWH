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

def test_030304_dim_network_append(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("test_030304_dim_network_append")

        # Logic code
        ### Read source data (from silver layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/customer_search_keynormalize"
        )

        src_df = spark.read.parquet(src_path)

        # Transform
        source_df = src_df\
            .withColumn(
                "proxy_isp",
                when(col("proxy_isp").isNotNull(), lower(trim(col("proxy_isp"))))
                    .otherwise(lit("unknown"))
            )\
            .withColumn(
                "network_type",
                when(lower(trim(col("networktype"))) == "wifi", "wifi")
                    .when(lower(trim(col("networktype"))).isin("ethernet","cab"), "ethernet")\
                        .when(lower(trim(col("networktype"))).isin("3g", "4g","5g","wwan"), "cellular")\
                            .when(lower(trim(col("networktype"))).like("%tethering%"), "tethering")\
                                .when(lower(trim(col("networktype"))) == "no_internet", "no_internet")\
                                    .otherwise("others")
            )\
            .withColumn(
                "isp_group",
                when(lower(trim(col("proxy_isp"))).isin("vnpt","viettel","fpt","spt","cmc","sctv","htvc","mobipone","vinaphone"
                    ,"netnam","vtc","hanot telecom"),'local')\
                    .when(lower(trim(col("proxy_isp"))).isin("akamai","cloudflare","aws","amazon","google","gcp","azure"
                        ,"microsoft","fastly","alibaba","ovh","digitalocean","linode","oracle","ibm"), 'international')\
                        .otherwise("unknown")
            )\
            .select(
                col("proxy_isp").cast(StringType()),
                col("network_type").cast(StringType()),
                col("isp_group").cast(StringType()))\
            .dropDuplicates()
        
        # Target đate
        target_df = read_from_redshift(spark,"gold.dim_network")
        target_df = target_df.select("proxy_isp","network_type","isp_group")

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
        print(f"""===== SUMMARY: DIM_NETWORK (GOLD LAYER) PASSED: {tests_passed}/1 =====""")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030304_dim_network_append')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030304_dim_network_append(etl_date=args.etl_date)
    exit(0 if success else 1)