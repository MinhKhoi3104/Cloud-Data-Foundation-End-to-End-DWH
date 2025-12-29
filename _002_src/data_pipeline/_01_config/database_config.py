# Config connect to Redshift
REDSHIFT_JDBC = {
    "url": "jdbc:redshift://my-project-e2e-wg.950242545712.ap-southeast-1.redshift-serverless.amazonaws.com:5439/my-project-e2e-dtb",
    'tempdir': 's3://data-pipeline-e2e-datalake-98c619f9/redshift-temp/',
    "properties": {
        "user": "admin",
        "password": "Devdata123"
    }
}
