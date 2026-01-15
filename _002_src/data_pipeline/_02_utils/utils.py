import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from pyspark.sql import SparkSession, DataFrame
from _01_config.data_storage_config import *
from _01_config.jar_paths import *

# Using to create spark session at Bronze Layer
def create_bronze_spark_session(appName):
    """Auto-select credentials based on environment"""
    
    # Detect environment
    is_airflow = os.getenv("AIRFLOW_HOME") is not None
    
    builder = (
        SparkSession.builder
        .appName(appName)
        .config("spark.jars", 
                f"{HADOOP_AWS_JAR_PATH},{AWS_JAVA_SDK_BUNDLE_JAR_PATH}")
        .config("spark.hadoop.fs.s3a.impl", 
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint",
                "s3.ap-southeast-1.amazonaws.com")
    )
    
    if is_airflow:
        print("🚀 Running in AIRFLOW - using IAM Role")
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        )
    else:
        print("💻 Running LOCALLY - using AWS Profile")
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
        ).config("spark.hadoop.fs.s3a.profile", "default")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Execute SQL DDL/DML directly in Redshift
def execute_sql_ddl(spark, sql_query: str) -> None:
    """
    Execute DDL SQL on Redshift via JDBC using Spark JVM.
    If any error occurs → raise exception and stop the job.
    """

    jvm = spark._jvm
    connection = None
    statement = None

    try:
        jvm.java.lang.Class.forName(
            "com.amazon.redshift.jdbc.Driver"
        )

        DriverManager = jvm.java.sql.DriverManager

        connection = DriverManager.getConnection(
            REDSHIFT_JDBC["url"],
            REDSHIFT_JDBC["properties"]["user"],
            REDSHIFT_JDBC["properties"]["password"]
        )

        statement = connection.createStatement()
        statement.execute(sql_query)

        print(f"✅ Executed SQL on Redshift: {sql_query}")

    except Exception as e:
        print("❌ Error executing SQL on Redshift")
        print(f"❌ SQL: {sql_query}")

        raise RuntimeError(
            f"Failed to execute Redshift DDL: {sql_query}"
        ) from e

    finally:
        try:
            if statement:
                statement.close()
            if connection:
                connection.close()
        except Exception:
            pass


# Using to create spark session at Silver Layer
def create_silver_spark_session(appName: str):
    """
    Silver layer:
    - Parquet on S3
    - Overwrite strategy
    - Local (AWS profile) & Airflow (IAM Role)
    """

    is_airflow = os.getenv("AIRFLOW_HOME") is not None

    builder = (
        SparkSession.builder
        .appName(appName)

        # ===== Hadoop S3A (AWS SDK v1) =====
        .config(
            "spark.jars",
            f"{HADOOP_AWS_JAR_PATH},{AWS_JAVA_SDK_BUNDLE_JAR_PATH}"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "s3.ap-southeast-1.amazonaws.com"
        )
    )

    if is_airflow:
        print("🚀 Silver on Airflow → IAM Role (Hadoop default)")
        pass

    else:
        print("💻 Silver Local → AWS Profile")
        builder = (
            builder
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider"
            )
            .config(
                "spark.hadoop.fs.s3a.profile",
                "default"
            )
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# Using to create spark session at Gold Layer
def create_gold_spark_session(appName: str):
    """
    Gold:
    - Parquet on S3
    - Write Redshift
    - Appy Iceberg
    - Works for Local & Airflow
    """

    is_airflow = os.getenv("AIRFLOW_HOME") is not None

    builder = (
        SparkSession.builder
        .appName(appName)
        .config(
            "spark.jars",
            ",".join([
                # Hadoop S3A (AWS SDK v1)
                HADOOP_AWS_JAR_PATH,
                AWS_JAVA_SDK_BUNDLE_JAR_PATH,
                ICEBERG_AWS_BUNDLE_JAR_PATH,

                # Iceberg (AWS SDK v2)
                ICEBERG_SPARK_RUNTIME_JAR_PATH,
                ICEBERG_AWS_BUNDLE_JAR_PATH,

                # Redshift
                REDSHIFT_JDBC_JAR_PATH,
                SPARK_REDSHIFT_JAR_PATH,
            ])
        )

        # ===== Iceberg =====
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.iceberg",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(
            "spark.sql.catalog.iceberg.warehouse",
            S3_ICEBERG_PATH
        )
        .config(
            "spark.sql.catalog.iceberg.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )

        # ===== Hadoop S3A/S3 =====
        .config(
            "spark.hadoop.fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
    )

    if is_airflow:
        print("🚀 Gold on Airflow → IAM Role")
        pass

    else:
        print("💻 Gold Local → AWS Profile")
        builder = (
            builder
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider"
            )
            .config(
                "spark.hadoop.fs.s3a.profile",
                "default"
            )
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Create S3 object if not exist
def ensure_s3_prefix(spark,s3_path):
    try:
        if not s3_path.startswith("s3a://"):
            raise ValueError("S3 path must start with s3a://")

        hadoop_conf = spark._jsc.hadoopConfiguration()

        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(s3_path),
            hadoop_conf
        )

        path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)

        if not fs.exists(path):
            fs.mkdirs(path)
            message = f"Created S3 object: {s3_path}"
        else:
            message = f"{s3_path} is exist"
        
        return print(message)
    except Exception as e:
        return print(f"❌ ERROR: {e}")
    
# Write data to redshift
def write_to_redshift(df: DataFrame,table_name: str,mode: str):
    df.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_JDBC["url"]) \
        .option("aws_iam_role", REDSHIFT_IAM_ROLE_ARN) \
        .option("dbtable", table_name) \
        .option("user", REDSHIFT_JDBC["properties"]["user"]) \
        .option("password", REDSHIFT_JDBC["properties"]["password"]) \
        .option("tempdir", REDSHIFT_JDBC["tempdir"]) \
        .mode(mode) \
        .save()

    print(f"✅ Successfully wrote DataFrame to Redshift table: {table_name}")