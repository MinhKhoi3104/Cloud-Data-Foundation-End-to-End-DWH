import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir)
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from config.jar_paths import *
from utils.utils import *
from datetime import date

def _spark_connect_redshift_test(etl_date=None):
    spark = None
    try:
        # Default etl_date = today
        if etl_date is None:
            etl_date = date.today().strftime("%Y%m%d")
        else:
            etl_date = str(etl_date)

        # Create spark session
        spark = create_gold_spark_session("_spark_connect_redshift_test")

        print("Testing Redshift connection ...")

        # ✅ Test Query
        test_sql = "(SELECT 1 AS ok) t"

        df = (
            spark.read
            .format("jdbc")
            .option("url", REDSHIFT_JDBC["url"])
            .option("dbtable", test_sql)
            .option("user", REDSHIFT_JDBC["properties"]["user"])
            .option("password", REDSHIFT_JDBC["properties"]["password"])
            .option("driver", "com.amazon.redshift.jdbc42.Driver")
            .load()
        )

        df.show()

        print("✅ Redshift connection SUCCESS")
        return True

    except Exception as e:
        print("❌ Redshift connection FAILED")
        print(f"ERROR: {e}")
        return False

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='_spark_connect_redshift_test')
    parser.add_argument('--etl_date', type=int, help='etl_date (YYYYMMDD)')
    args = parser.parse_args()

    success = _spark_connect_redshift_test(etl_date=args.etl_date)
    exit(0 if success else 1)