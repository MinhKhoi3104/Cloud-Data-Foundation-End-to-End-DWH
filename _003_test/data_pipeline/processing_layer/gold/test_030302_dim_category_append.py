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
import unicodedata

def test_030302_dim_category_append(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("test_030302_dim_category_append")

        # Logic code
        ### Read source data (from silver layer)
        src_path = (
            f"{S3_DATALAKE_PATH}"
            "/silver/customer_search_keynormalize"
        )

        src_df = spark.read.parquet(src_path)

        ### Each category is a row
        source_df = src_df\
            .select("keyword_category")\
            .withColumn(
                "category_name",
                explode(
                    split(col("keyword_category"), r"\s*;\s*")
                )
            )\
            .select(trim(col("category_name")).alias("category_name"))\
            .distinct()
        
        # Create category_slug
        def remove_accents(s: str) -> str:
            if s is None:
                return None

            # Handle Vietnamese special characters first
            s = s.replace("Đ", "D").replace("đ", "d")

            return (
                unicodedata
                .normalize("NFKD", s)
                .encode("ascii", "ignore")
                .decode("utf-8")
            )


        remove_accents_udf = udf(remove_accents, StringType())


        def with_category_slug(df, category_col="category_name", slug_col="category_slug"):
            """
            Add category_slug column to DataFrame.
            """
            return (
                df
                .withColumn(
                    slug_col,
                    lower(
                        trim(
                            regexp_replace(
                                regexp_replace(
                                    remove_accents_udf(col(category_col)),
                                    r"[^a-zA-Z0-9]+",
                                    "-"
                                ),
                                r"(^-|-$)",
                                ""
                            )
                        )
                    )
                )
            )
        
        # Use with_category_slug fuction to create category_slug column
        source_df = with_category_slug(source_df)

        # Target đate
        target_df = read_from_redshift(spark,"gold.dim_category")
        cols = ("category_name","category_slug")

        # Unit test
        tests_passed = 0
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
        print(f"""===== SUMMARY: DIM_CATEGORY (GOLD LAYER) PASSED: {tests_passed}/1 =====""")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='test_030302_dim_category_append')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = test_030302_dim_category_append(etl_date=args.etl_date)
    exit(0 if success else 1)