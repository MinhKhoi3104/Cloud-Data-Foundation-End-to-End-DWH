import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from pyspark.sql import SparkSession
from _01_config.data_storage_config import *
from _01_config.jar_paths import *

# Using to create spark session at Bronze Layer
def create_spark_s3_session(appName):
    spark = (
            SparkSession.builder
            .appName(appName)
            .config("spark.jars", f"{HADOOP_AWS_JAR_PATH},{AWS_JAVA_SDK_BUNDLE_JAR_PATH}")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider"
            )
            .config(
                "spark.hadoop.fs.s3a.profile",
                "default"
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                "s3.ap-southeast-1.amazonaws.com"
            )
            .getOrCreate()
        )
    return spark

# Execute SQL DDL/DML directly in Redshift
def execute_sql_ddl(spark, sql_query):
    jvm = spark._jvm

    try:
        DriverManager = jvm.java.sql.DriverManager
        connection = DriverManager.getConnection(
            REDSHIFT_JDBC["url"],
            REDSHIFT_JDBC["properties"]["user"],
            REDSHIFT_JDBC["properties"]["password"]
        )
        statement = connection.createStatement()
        statement.execute(sql_query)
        return True

    except Exception as e:
        print(f"❌ Error executing SQL on Redshift: {e}")
        return False

    finally:
        if 'connection' in locals() and connection:
            connection.close()


# Using to create spark session at Silver Layer
def create_spark_iceberg_session(appName):
    """
    Create Spark session for Silver Layer - S3 + Iceberg
    Args:
        appName: Application name
        warehouse_path: S3 path for Iceberg warehouse (ex: s3a://bucket/iceberg-warehouse)
    """
    spark = (
        SparkSession.builder
        .appName(appName)
        # JARs cho S3
        .config("spark.jars", 
                f"{HADOOP_AWS_JAR_PATH},"
                f"{AWS_JAVA_SDK_BUNDLE_JAR_PATH},"
                f"{ICEBERG_SPARK_RUNTIME_JAR_PATH}")
        
        # S3 configs
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
        )
        
        # Iceberg configs
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.local", 
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3a://data-pipeline-e2e-datalake-98c619f9/iceberg-warehouse")
        
        # Optional: Enable Iceberg features
        .config("spark.sql.catalog.local.io-impl", 
                "org.apache.iceberg.aws.s3.S3FileIO")
        
        .getOrCreate()
    )
    return spark

# Using to create spark session at Silver Layer
def create_spark_redshift_session(appName):
    """Gold: Redshift + Iceberg"""
    spark = (
        SparkSession.builder
        .appName(appName)
        .config("spark.jars", 
                f"{HADOOP_AWS_JAR_PATH},{AWS_JAVA_SDK_BUNDLE_JAR_PATH},"
                f"{ICEBERG_SPARK_RUNTIME_JAR_PATH},{REDSHIFT_JDBC_JAR_PATH},{SPARK_REDSHIFT_JAR_PATH}")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3a://data-pipeline-e2e-datalake-98c619f9/iceberg-warehouse")
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.databricks.redshift.jdbc.url", REDSHIFT_JDBC['url'])
        .config("spark.databricks.redshift.tempdir", REDSHIFT_JDBC['tempdir'])
        .getOrCreate()
    )

    # set log level WARN to reduce unnessary uotput line
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
            message = f"Created S3 bronze layer: {s3_path}"
        else:
            message = f"S3 bronze layer: {s3_path} is exist"
        
        return print(message)
    except Exception as e:
        return print(f"❌ ERROR: {e}")