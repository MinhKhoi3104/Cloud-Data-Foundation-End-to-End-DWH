from pyspark.sql.functions import *
from pyspark.sql.window import Window

SK_REGISTRY_TABLE = "iceberg.gold.sk_registry"


def allocate_surrogate_keys(
    spark,
    df_new,
    entity_name: str,
    business_key_col: str,
    sk_col: str
):
    """
    Allocate surrogate keys using Iceberg registry table.
    - entity_name: 'dim_user', 'dim_plan', ...
    """

    # 0. Create surrogate key registry tbl
    spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.gold.sk_registry(
        entity_name   STRING,
        current_max   BIGINT,
        updated_at    TIMESTAMP
    )
    USING iceberg;
    """)

    # 1. Read registry
    registry = (
        spark.read
             .format("iceberg")
             .load(SK_REGISTRY_TABLE)
             .filter(col("entity_name") == entity_name)
    )

    current_max = (
        registry
        .agg(max("current_max").alias("current_max"))
        .collect()[0]["current_max"]
    )

    current_max = current_max or 0

    # 2. Assign new surrogate keys
    w = Window.orderBy(business_key_col)

    df_with_sk = (
        df_new
        .withColumn(
            sk_col,
            row_number().over(w) + lit(current_max)
        )
    )

    # 3. Update registry
    new_max = current_max + df_new.count()

    df_registry_upsert = spark.createDataFrame(
        [(entity_name, new_max)],
        ["entity_name", "current_max"]
    ).withColumn("updated_at", current_timestamp())

    df_registry_upsert.createOrReplaceTempView("stg_registry")

    spark.sql(f"""
        MERGE INTO {SK_REGISTRY_TABLE} t
        USING stg_registry s
        ON t.entity_name = s.entity_name
        WHEN MATCHED THEN
          UPDATE SET
            current_max = s.current_max,
            updated_at  = s.updated_at
        WHEN NOT MATCHED THEN
          INSERT (entity_name, current_max, updated_at)
          VALUES (s.entity_name, s.current_max, s.updated_at)
    """)

    return df_with_sk
