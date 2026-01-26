from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys


# Add project source to Python path
SRC_PATH = "/opt/airflow/_002_src"
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from data_pipeline._02_utils.utils import (
    create_gold_spark_session,
    read_from_redshift,
    write_to_postgres
)

from data_pipeline._01_config.data_storage_config import (
    POSTGRES_CONN
)
def extract_from_redshift(**context):
    """
    Extract data from Redshift tables in gold schema.
    Returns a list of table names that were successfully extracted.
    """
    spark = None
    try:
        print("📤 Extracting data from Redshift...")
        
        # Create Spark session (PostgreSQL JDBC driver already included)
        spark = create_gold_spark_session("redshift_to_postgres_extract")
        
        # List of tables to replicate from Redshift gold schema
        tables_to_replicate = [
            "dim_date",
            "dim_category",
            "dim_platform", 
            "dim_network",
            "dim_user",
            "dim_subscription",
            "bridge_user_plan",
            "fact_customer_search"
        ]
        
        extracted_tables = []
        
        for table_name in tables_to_replicate:
            try:
                full_table_name = f"gold.{table_name}"
                print(f"📖 Reading table: {full_table_name}")
                
                # Read from Redshift
                read_from_redshift(spark, full_table_name)
                
                print(f"✅ Extracted records from {full_table_name}")
                extracted_tables.append(table_name)
                
            except Exception as e:
                print(f"⚠️ Warning: Could not extract {table_name}: {e}")
                continue
        
        print(f"✅ Successfully extracted {len(extracted_tables)} tables from Redshift")
        return extracted_tables
        
    except Exception as e:
        print(f"❌ ERROR extracting from Redshift: {e}")
        raise
    
    finally:
        if spark:
            spark.stop()

def load_to_postgres(**context):
    """
    Load data from Redshift into PostgreSQL.
    Reads each table from Redshift and writes to PostgreSQL.
    """
    spark = None
    try:
        print("📥 Loading data into PostgreSQL...")
        
        # Create Spark session (PostgreSQL JDBC driver already included)
        spark = create_gold_spark_session("redshift_to_postgres_load")
        
        # List of tables to replicate
        tables_to_replicate = [
            "dim_date",
            "dim_category",
            "dim_platform", 
            "dim_network",
            "dim_user",
            "dim_subscription",
            "bridge_user_plan",
            "fact_customer_search"
        ]
        
        loaded_tables = []
        
        for table_name in tables_to_replicate:
            try:
                full_table_name = f"gold.{table_name}"
                print(f"📤 Loading table: {table_name} to PostgreSQL...")
                
                # Read from Redshift
                df = read_from_redshift(spark, full_table_name)
                record_count = df.limit(1).count()
                
                if record_count > 0:
                    # Write to PostgreSQL (overwrite mode to ensure fresh data)
                    write_to_postgres(df, table_name, mode="overwrite", schema=POSTGRES_CONN["schema"])
                    print(f"""✅ Successfully loaded {record_count} records to PostgreSQL: {POSTGRES_CONN["schema"]}.{table_name}""")
                    loaded_tables.append(table_name)
                else:
                    print(f"⚠️ Table {table_name} is empty, skipping...")
                    
            except Exception as e:
                print(f"❌ ERROR loading {table_name} to PostgreSQL: {e}")
                # Continue with other tables even if one fails
                continue
        
        print(f"✅ Successfully loaded {len(loaded_tables)} tables to PostgreSQL")
        return loaded_tables
        
    except Exception as e:
        print(f"❌ ERROR loading to PostgreSQL: {e}")
        raise
    
    finally:
        if spark:
            spark.stop()


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="redshift_to_postgre",
    description="Replicate data from Redshift Gold to PostgreSQL",
    start_date=datetime(2022, 1, 1),
    schedule_interval = "0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["replication", "redshift", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_gold_finish",
        external_dag_id="data_pipeline_daily",
        external_task_id="gold_finish",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
    )

    extract = PythonOperator(
        task_id="extract_from_redshift",
        python_callable=extract_from_redshift
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_gold >> extract >> load >> end
