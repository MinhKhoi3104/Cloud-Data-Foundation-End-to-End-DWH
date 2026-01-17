import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..','..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from _02_utils.utils import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def _030301_dim_date_overwrite():
    spark = None
    try:
        # Create spark session
        spark = create_gold_spark_session("_030301_dim_date_overwrite")

        # create date_dim
        start_date = "2020-01-01"
        end_date   = "2030-12-31"

        df_dim_date = (
            spark.sql(
                f"""
                SELECT explode(
                    sequence(
                        to_date('{start_date}'),
                        to_date('{end_date}'),
                        interval 1 day
                    )
                ) AS date
                """
            )
            .withColumn("date_key", date_format("date", "yyyyMMdd"))
            .withColumn("year", year("date"))
            .withColumn("quarter",quarter("date"))
            .withColumn("month", month("date"))
            .withColumn("day", dayofmonth("date"))
            .withColumn("day_of_week", dayofweek("date")) # 1 = Sunday, 7 = Saturday
            .withColumn(
                "is_weekend",
                col("day_of_week").isin(1, 7).cast("int")
            )
            .select(
                col("date_key").cast(StringType()),
                col("date").cast(DateType()),
                col("year").cast(IntegerType()),
                col("quarter").cast(IntegerType()),
                col("month").cast(IntegerType()),
                col("day").cast(IntegerType()),
                col("day_of_week").cast(IntegerType()),
                col("is_weekend").cast(IntegerType())
            )
        )

         # Create Redshift schema
        sql_query = "CREATE SCHEMA IF NOT EXISTS gold;"
        execute_sql_ddl(spark,sql_query)

        # Create Redshift table
        sql_query = """CREATE TABLE IF NOT EXISTS gold.dim_date (
            date_key VARCHAR(255),
            date DATE,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_week INTEGER,
            is_weekend INTEGER
        );"""
        execute_sql_ddl(spark,sql_query)

        """
        Read data from iceberg and insert to Redshift
        """

        # LOAD
        write_to_redshift(df_dim_date, "gold.dim_date","overwrite")
        print("===== ✅ Completely insert new records into Readshift: gold.dim_date! =====")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = _030301_dim_date_overwrite()
    exit(0 if success else 1)