import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _02_utils.utils import *
from _02_utils.surrogate_key_registry import *
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.types import *
import unicodedata

def _030302_dim_category_append(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("_030302_dim_category_append")

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
        spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold.dim_category(
            category_key   int,
            category_name   string,
            category_slug    string
        )
        USING iceberg;
        """)

        # Each category is a row
        tg_df = src_df\
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
        tg_df = with_category_slug(tg_df)

        # Extract old data of dim_category tbl
        tg_df_old = spark.sql("SELECT * FROM iceberg.gold.dim_category")

        # Choose specific columns to compare
        tg_df_old = tg_df_old.select("category_name","category_slug")

        # Just choose new data
        insert_df = tg_df.subtract(tg_df_old)

        # Create surrogate key
        insert_df = allocate_surrogate_keys(
            spark,
            insert_df,
            "dim_category",
            "category_name",
            "category_key"
        )

        insert_records_count = insert_df.count()

        # Create Redshift schema
        sql_query = "CREATE SCHEMA IF NOT EXISTS gold;"
        execute_sql_ddl(spark,sql_query)

        # Create Redshift table
        sql_query = """CREATE TABLE IF NOT EXISTS gold.dim_category (
            category_key   INTEGER,
            category_name  VARCHAR(255),
            category_slug  VARCHAR(255)
        );"""
        execute_sql_ddl(spark,sql_query)

       # Load to Redshift
        """
        Load data to Redshift first
        """
        if insert_records_count > 0:

            print(f'===== The number of insert records: {insert_records_count} =====')
            # LOAD
            write_to_redshift(insert_df, "gold.dim_category","append")
            print("===== ✅ Completely insert new records into Readshift: gold.dim_category! =====")
        else:
            print('===== No records need to insert! =====')

        # Load to Iceberg
        """
        Load data to Iceberg second
        """
        if insert_records_count > 0:

            print(f'===== The number of insert records: {insert_records_count} =====')

            # LOAD
            insert_df.writeTo("iceberg.gold.dim_category").append()
            print("===== ✅ Completely insert new records into iceberg.gold.dim_category! =====")

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

    parser = argparse.ArgumentParser(description='_030302_dim_category_append')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = _030302_dim_category_append(etl_date=args.etl_date)
    exit(0 if success else 1)