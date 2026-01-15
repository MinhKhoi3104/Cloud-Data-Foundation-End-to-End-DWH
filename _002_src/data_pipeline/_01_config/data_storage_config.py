# in reality, this file need add to .gitignore
# Config connect to Redshift
REDSHIFT_JDBC = {
    "url": "jdbc:redshift://my-project-e2e-wg.950242545712.ap-southeast-1.redshift-serverless.amazonaws.com:5439/my-project-e2e-dtb",
    'tempdir': 's3://data-pipeline-e2e-datalake-98c619f9/redshift-temp/',
    "properties": {
        "user": "admin",
        "password": "Devdata123"
    }
}

# S3 data lake path
S3_DATALAKE_PATH = "s3a://data-pipeline-e2e-datalake-98c619f9"

# S3 iceberg warehouse path
S3_ICEBERG_PATH = "s3a://data-pipeline-e2e-datalake-98c619f9/iceberg-warehouse"

# REDSHIFT_IAM_ROLE_ARN
REDSHIFT_IAM_ROLE_ARN = "arn:aws:iam::950242545712:role/project-e2e-dev-redshift-serverless-role"