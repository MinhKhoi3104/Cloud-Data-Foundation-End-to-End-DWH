import os
import sys
from uuid import uuid4
from pyspark.sql import SparkSession, DataFrame

# ==========================================================
# CONFIG IMPORT (AIRFLOW SAFE)
# ==========================================================
try:
    from _01_config.data_storage_config import *
    from _01_config.jar_paths import *
except ImportError:
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from _01_config.data_storage_config import *
    from _01_config.jar_paths import *


# ==========================================================
# ENV DETECTION (LOG ONLY)
# ==========================================================
def is_airflow() -> bool:
    return os.getenv("AIRFLOW_HOME") is not None


# ==========================================================
# S3 / AWS CONFIG (AIRFLOW / PROD)
# ==========================================================
def apply_s3_config(builder):
    return (
        builder
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "s3.ap-southeast-1.amazonaws.com"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
    )


# ==========================================================
# SPARK BUILDER (BASE)
# ==========================================================
def build_spark(app_name: str, jars: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jars)
    )

    builder = apply_s3_config(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if is_airflow():
        print("🚀 Spark running inside Airflow")

    return spark


# ==========================================================
# BRONZE
# ==========================================================
def create_bronze_spark_session(app_name: str) -> SparkSession:
    jars = f"{HADOOP_AWS_JAR_PATH},{AWS_JAVA_SDK_BUNDLE_JAR_PATH}"
    return build_spark(app_name, jars)


# ==========================================================
# SILVER
# ==========================================================
def create_silver_spark_session(app_name: str) -> SparkSession:
    jars = f"{HADOOP_AWS_JAR_PATH},{AWS_JAVA_SDK_BUNDLE_JAR_PATH}"
    return build_spark(app_name, jars)


# ==========================================================
# GOLD (ICEBERG + REDSHIFT)
# ==========================================================
def create_gold_spark_session(app_name: str) -> SparkSession:
    jars = ",".join([
        # Hadoop / AWS
        HADOOP_AWS_JAR_PATH,
        AWS_JAVA_SDK_BUNDLE_JAR_PATH,

        # Spark
        SPARK_AVRO_JAR_PATH,

        # Iceberg
        ICEBERG_SPARK_RUNTIME_JAR_PATH,
        ICEBERG_AWS_BUNDLE_JAR_PATH,

        # Redshift
        REDSHIFT_JDBC_JAR_PATH,
        SPARK_REDSHIFT_JAR_PATH,

        # Postgres
        POSTGRES_JDBC_JAR_PATH,
    ])

    builder = (
        SparkSession.builder
        .appName(app_name)

        # Iceberg
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.iceberg",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.iceberg.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        .config(
            "spark.sql.catalog.iceberg.warehouse",
            S3_ICEBERG_PATH
        )
        .config(
            "spark.sql.catalog.iceberg.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.jars", jars)
    )

    builder = apply_s3_config(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("✨ Spark Gold session with Iceberg initialized")

    return spark


# ==========================================================
# S3 UTILS
# ==========================================================
def ensure_s3_prefix(spark: SparkSession, s3_path: str) -> None:
    if not s3_path.startswith("s3a://"):
        raise ValueError("S3 path must start with s3a://")

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(s3_path),
        spark._jsc.hadoopConfiguration()
    )

    path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)

    if not fs.exists(path):
        fs.mkdirs(path)
        print(f"✅ Created S3 prefix: {s3_path}")
    else:
        print(f"ℹ️ S3 prefix exists: {s3_path}")


# ==========================================================
# REDSHIFT
# ==========================================================
def execute_sql_ddl(spark: SparkSession, sql_query: str) -> None:
    jvm = spark._jvm

    try:
        jvm.java.lang.Class.forName("com.amazon.redshift.jdbc.Driver")
        conn = jvm.java.sql.DriverManager.getConnection(
            REDSHIFT_JDBC["url"],
            REDSHIFT_JDBC["properties"]["user"],
            REDSHIFT_JDBC["properties"]["password"],
        )
        stmt = conn.createStatement()
        stmt.execute(sql_query)
        print(f"✅ Executed SQL: {sql_query}")
    except Exception as e:
        raise RuntimeError(f"❌ Failed SQL: {sql_query}") from e
    finally:
        try:
            stmt.close()
            conn.close()
        except Exception:
            pass


def write_to_redshift(df: DataFrame, table_name: str, mode: str) -> None:
    """
    Write DataFrame to Redshift using JDBC with batch insert
    """
    print(f"Writing to Redshift table: {table_name}")
    print(f"   Mode: {mode}")
    print(f"   Rows: {df.count()}")
    df.write \
        .format("jdbc") \
        .option("url", REDSHIFT_JDBC["url"]) \
        .option("dbtable", table_name) \
        .option("user", REDSHIFT_JDBC["properties"]["user"]) \
        .option("password", REDSHIFT_JDBC["properties"]["password"]) \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .option("batchsize", "10000") \
        .option("isolationLevel", "NONE") \
        .option("truncate", "true" if mode == "overwrite" else "false") \
        .mode(mode) \
        .save()

    print(f"✅ Written to Redshift: {table_name}")


def read_from_redshift(spark: SparkSession, table_name: str) -> DataFrame:
    return (
        spark.read
        .format("jdbc")
        .option("url", REDSHIFT_JDBC["url"])
        .option("dbtable", table_name)
        .option("user", REDSHIFT_JDBC["properties"]["user"])
        .option("password", REDSHIFT_JDBC["properties"]["password"])
        .option("driver", "com.amazon.redshift.jdbc.Driver")
        .load()
    )


# ==========================================================
# POSTGRES
# ==========================================================
def write_to_postgres(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    schema: str = None
):
    from _01_config.data_storage_config import POSTGRES_CONN

    schema = schema or POSTGRES_CONN["schema"]
    jdbc_url = (
        f"jdbc:postgresql://{POSTGRES_CONN['host']}:"
        f"{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}"
    )

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"{schema}.{table_name}") \
        .option("user", POSTGRES_CONN["user"]) \
        .option("password", POSTGRES_CONN["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()

    print(f"✅ Written to Postgres: {schema}.{table_name}")


def read_from_postgres(
    spark: SparkSession,
    table_name: str,
    schema: str = None
) -> DataFrame:
    from _01_config.data_storage_config import POSTGRES_CONN

    schema = schema or POSTGRES_CONN["schema"]
    jdbc_url = (
        f"jdbc:postgresql://{POSTGRES_CONN['host']}:"
        f"{POSTGRES_CONN['port']}/{POSTGRES_CONN['database']}"
    )

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{schema}.{table_name}")
        .option("user", POSTGRES_CONN["user"])
        .option("password", POSTGRES_CONN["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )