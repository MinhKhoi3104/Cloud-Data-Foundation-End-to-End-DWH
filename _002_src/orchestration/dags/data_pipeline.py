from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param
from datetime import datetime, timedelta

# ==============================
# DEFAULT ARGS
# ==============================
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ==============================
# DAG DEFINITION
# ==============================
with DAG(
    dag_id="data_pipeline_daily",
    description="End-to-end pipeline (Airflow submit Spark jobs only)",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["spark", "bronze", "silver", "gold"],
    params={
        "etl_date": Param(
            default=(datetime.now() - timedelta(days=1)).strftime('%Y%m%d'),
            type="string",
            title="ETL Date",
            description="ETL date in YYYYMMDD format (e.g., 20250123)",
            minLength=8,
            maxLength=8,
            pattern="^[0-9]{8}$",  # Validate YYYYMMDD format
        )
    },
    render_template_as_native_obj=False, 
) as dag:

    # ======================================================
    # Common Spark config
    # ======================================================
    SPARK_CONN_ID = "spark_default"
    APP_BASE_PATH = "/opt/airflow/data_pipeline/_03_etl_jobs"
    ETL_DATE = "{{ params.etl_date | string }}"

    start = EmptyOperator(task_id="start")

    # ==============================
    # BRONZE
    # ==============================
    with TaskGroup("bronze") as bronze:
        SparkSubmitOperator(
            task_id="030101_customer_search",
            application=f"{APP_BASE_PATH}/_0301_bronze/_030101_customer_search.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            verbose=True,
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            }
        )

    # ==============================
    # SILVER
    # ==============================
    with TaskGroup("silver") as silver:

        user_plans_map = SparkSubmitOperator(
            task_id="030201_user_plans_map",
            application=f"{APP_BASE_PATH}/_0302_silver/_030201_user_plans_map.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            }
        )

        customer_search_keynormalize = SparkSubmitOperator(
            task_id="030202_customer_search_keynormalize",
            application=f"{APP_BASE_PATH}/_0302_silver/_030202_customer_search_keynormalize.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            }
        )

        user_plans_map >> customer_search_keynormalize

    # ==============================
    # GOLD
    # ==============================
    with TaskGroup("gold") as gold:

        dim_category = SparkSubmitOperator(
            task_id="030302_dim_category_append",
            application=f"{APP_BASE_PATH}/_0303_gold/_030302_dim_category_append.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            }
        )

        dim_platform = SparkSubmitOperator(
            task_id="030303_dim_platform_append",
            application=f"{APP_BASE_PATH}/_0303_gold/_030303_dim_platform_append.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            }
        )

        dim_network = SparkSubmitOperator(
            task_id="030304_dim_network_append",
            application=f"{APP_BASE_PATH}/_0303_gold/_030304_dim_network_append.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            }
        )

        dim_user = SparkSubmitOperator(
            task_id="030305_dim_user_scd1",
            application=f"{APP_BASE_PATH}/_0303_gold/_030305_dim_user_scd1.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            }
        )

        dim_subscription = SparkSubmitOperator(
            task_id="030306_dim_subscription_append",
            application=f"{APP_BASE_PATH}/_0303_gold/_030306_dim_subscription_append.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            }
        )

        bridge_user_plan = SparkSubmitOperator(
            task_id="030307_bridge_user_plan",
            application=f"{APP_BASE_PATH}/_0303_gold/_030307_bridge_user_plan.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            }
        )

        fact_customer_search = SparkSubmitOperator(
            task_id="030308_fact_customer_search_append",
            application=f"{APP_BASE_PATH}/_0303_gold/_030308_fact_customer_search_append.py",
            conn_id=SPARK_CONN_ID,
            application_args=["--etl_date", ETL_DATE],
            conf = {
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "3g",
            "spark.executor.memoryOverhead": "1g",
            "spark.driver.memory": "1g",
            "spark.driver.memoryOverhead": "512m",
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
            }
        )

        dim_category >> dim_platform >> dim_network >> dim_subscription >> dim_user >> bridge_user_plan >> fact_customer_search 


    gold_finish = EmptyOperator(task_id="gold_finish")

    start >> bronze >> silver >> gold >> gold_finish
    