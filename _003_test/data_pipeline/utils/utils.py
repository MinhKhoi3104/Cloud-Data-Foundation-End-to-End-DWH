import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from pyspark.sql import SparkSession, DataFrame
from config.data_storage_config import *
from config.jar_paths import *

# Using to create spark session at Bronze Layer
def create_bronze_spark_session(appName):
    print("💻 Running LOCALLY - using AWS Profile")
    
    builder = (
        SparkSession.builder
        .appName(appName)
        .config("spark.jars", 
                f"{HADOOP_AWS_JAR_PATH},{AWS_JAVA_SDK_BUNDLE_JAR_PATH}")
        .config("spark.hadoop.fs.s3a.impl", 
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint",
                "s3.ap-southeast-1.amazonaws.com")
        .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.profile", "default")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Using to create spark session at Silver Layer
def create_silver_spark_session(appName: str):
    """
    Silver layer:
    - Parquet on S3
    - Overwrite strategy
    """
    print("💻 Silver Local → AWS Profile")
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
    """

    print("💻 Gold Local → AWS Profile")
    builder = (
        SparkSession.builder
        .appName(appName)
        .config(
            "spark.jars",
            ",".join([
                # Hadoop S3A (AWS SDK v1)
                HADOOP_AWS_JAR_PATH,
                AWS_JAVA_SDK_BUNDLE_JAR_PATH,
                SPARK_AVRO_JAR_PATH,

                # Iceberg (AWS SDK v2)
                ICEBERG_SPARK_RUNTIME_JAR_PATH,
                ICEBERG_AWS_BUNDLE_JAR_PATH,

                # Redshift
                REDSHIFT_JDBC_JAR_PATH,
                SPARK_REDSHIFT_JAR_PATH
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


# Read data from Redshift
def read_from_redshift(
    spark,
    table_name: str
) -> DataFrame:
    """
    Read data from Redshift into Spark DataFrame
    """
    df = (
    spark.read
    .format("jdbc")
    .option("url", REDSHIFT_JDBC["url"])
    .option("dbtable", table_name)
    .option("user", REDSHIFT_JDBC["properties"]["user"])
    .option("password", REDSHIFT_JDBC["properties"]["password"])
    .option("driver", "com.amazon.redshift.jdbc.Driver")
    .load()
    )

    print(f"✅ Successfully read Redshift table: {table_name}")

    return df