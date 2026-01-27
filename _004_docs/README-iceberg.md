# Apache Iceberg Application

## 📋 General

This project uses **Apache Iceberg** as an open table format for the **Gold Layer** of the data warehouse. Iceberg provides efficient table management on S3, along with features such as **ACID transactions, time travel, schema evolution, and partition evolution**.

## 🎯 Why using Apache Iceberg?

### 1. **ACID Transactions**
- Ensures **data consistency** during **concurrent read and write operations**
- Safely supports operations such as `MERGE INTO`, `UPDATE`, and `DELETE`
- Prevents issues related to **dirty reads** and **write conflicts**

### 2. **Time Travel & Snapshot Isolation**
- Enables querying data at **any point in time in the past**
- **Supports rollback** to a previous snapshot when needed
- Well-suited for **audit trails and compliance requirements**

### 3. **Schema Evolution**
- Allows **schema changes (adding, dropping, or modifying columns) without rewriting the entire dataset**
- Automatically handles **backward compatibility**
- **Minimizes downtime** and **storage costs**

### 4. **Partition Evolution**
- Allows changing the **partitioning strategy** without requiring data migration
- Provides flexibility to **optimize query performance over time**

### 5. **Hidden Partitioning**
- Partitions are **automatically managed by Iceberg**
- Query engines automatically apply **partition pruning**
- Queries do **not require knowledge of the underlying partition structure**

### 6. **Performance Optimization**
- Metadata is **stored efficiently**, reducing overhead from **file listing operations**
- Supports **file-level statistics for query optimization**
- Integrates well with Spark, Presto, Trino, and other query engines

## 🏗️ Architecture and Configuration

### General Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Gold Layer                           │
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │         Apache Iceberg Tables                    │   │
│  │  - sk_registry                                   │   │
│  │  - bridge_user_plan                              │   │
│  │  - dim_category                                  |   |
|  |  - dim_network                                   |   |
|  |  - dim_platform                                  |   |               
│  │  - dim_subscription                              |   |
|  |  - dim_user                                      |   |
|  |  - fact_customer_search                          |   |
│  └──────────────────────────────────────────────────┘   │
│                          │                              │
│                          ▼                              │
│  ┌──────────────────────────────────────────────────┐   │
│  │      AWS Glue Catalog (Metadata Catalog)         │   │
│  │  - Manage table metadata                         │   │
│  │  - Schema definitions                            │   │
│  │  - Table properties                              │   │
│  └──────────────────────────────────────────────────┘   │
│                          │                              │
│                          ▼                              │
│  ┌──────────────────────────────────────────────────┐   │
│  │      S3 Warehouse (Data + Metadata)              │   │
│  │  - Data files (Parquet)                          │   │
│  │  - Metadata files (JSON)                         │   │
│  │  - Manifest files                                │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Configuration

Iceberg is configured in the `create_gold_spark_session()` function with the following parameters:

```python
# Iceberg Extensions
spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# Catalog Configuration
spark.sql.catalog.iceberg = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.iceberg.warehouse = s3a://data-pipeline-e2e-datalake-98c619f9/iceberg-warehouse
spark.sql.catalog.iceberg.io-impl = org.apache.iceberg.aws.s3.S3FileIO
```

### Core Components

1. **AWS Glue Catalog**: Stores metadata for tables, schemas, and partitions 
   - Integrated with the AWS Glue Data Catalog  
   - Queryable from Athena, Redshift Spectrum, and other tools

![AWS_Glue_Data_Catalog](/image/AWS_Glue_Data_Catalog.png)
<p align="center">
  <em> AWS Glue Data Catalog</em>
</p>


![iceberg_tbl_detail](/image/iceberg_tbl_detail.png)
<p align="center">
  <em> Apache Iceberg table details in AWS Glue Data Catalog</em>
</p>

2. **S3 Warehouse**: Stores data and metadata files 
   - **Data files**: Actual data stored in Parquet format 
   - **Metadata files**: Information about snapshots, schemas, and partition specs
   - **Manifest files**: Lists of data files for each snapshot

![s3_store_iceberg](/image/s3_store_iceberg.png)
<p align="center">
  <em> Gold Layer Iceberg tables stored in the S3 warehouse</em>
</p>

![iceberg_data_stored](/image/iceberg_data_stored.png)
<p align="center">
  <em> Partitioned Iceberg table data stored in S3 (partitioned by date_key)</em>
</p>

![iceberg_metadata_files](/image/iceberg_metadata_files.png)
<p align="center">
  <em> Iceberg table metadata files (snapshots, schemas, and manifest references) stored in S3</em>
</p>



3. **JAR Dependencies**:
   - `iceberg-spark-runtime-3.5_2.12-1.5.2.jar`: Spark runtime cho Iceberg
   - `iceberg-aws-bundle-1.5.2.jar`: AWS integration (Glue Catalog, S3)

## 💻 The ways use Iceberg

### 1. Create Namespace and Table

```python
# Create namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

# Create table with defined schema
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.gold.fact_customer_search(
        event_id   string,
        datetime_log       timestamp,
        date_key    string,
        user_id       string,
        keyword       string,
        keyword_slug       string,
        category       string,
        action       string,
        network_key       int,
        platform_key       int,
        main_keyword_category       int,
        sub1_keyword_category       int,
        sub2_keyword_category       int,
        sub3_keyword_category       int
    )
    USING iceberg
    PARTITIONED BY (date_key);
    """)
```

### 2. Append Data

```python
# Append data vào table
insert_df.writeTo("iceberg.gold.dim_category").append()
```

### 3. Merge Data

```python
# Using MERGE INTO for upsert operations
spark.sql("""
    MERGE INTO iceberg.gold.sk_registry t
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
```

### 4. Query Data

```python
# Query from Iceberg table
df = spark.sql("SELECT * FROM iceberg.gold.dim_category")

# Using DataFrame API
df = spark.read.format("iceberg").load("iceberg.gold.dim_category")
```

### 5. Time Travel Queries

```python
# Query data at a specific snapshot (using as-of-timestamp)
df = spark.read \
    .option("as-of-timestamp", 1768740338401) \
    .format("iceberg") \
    .load("iceberg.gold.dim_category")

# Query data at a specific snapshot (snapshot-id)
df = spark.read \
    .option("snapshot-id", 4566877450833814094) \
    .format("iceberg") \
    .load("iceberg.gold.dim_category")
```

## ✅ Specific Benefits in the Project

### 1. **Data Quality & Consistency**
- ACID transactions ensure data consistency at all times
- Support for upsert operations using `MERGE` INTO
- Prevents partial writes and data corruption

### 2. **Operational Efficiency**
- No need to manually manage partition paths
- Metadata is automatically managed
- Schema changes (add/remove/modify columns) without impacting existing data

### 3. **Cost Optimization**
- Reads only relevant files thanks to metadata optimization
- Supports compaction to optimize storage
- Old snapshots can be expired to reduce storage costs

### 4. **Integration với AWS Services**
- Integrated with AWS Glue Catalog → queryable from Athena and Redshift Spectrum
- Uses S3 as the storage layer → cost-effective and scalable
- Compatible with AWS analytics services

### 5. **Developer Experience**
- SQL-like interface, easy to use
- Strong support for the Spark DataFrame API
- Time travel simplifies debugging and auditing


## 📚 References

- [Apache Iceberg Official Documentation](https://iceberg.apache.org/)
- [Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [AWS Glue Catalog Integration](https://iceberg.apache.org/docs/latest/aws/)
- [Iceberg Specification](https://iceberg.apache.org/spec/)

