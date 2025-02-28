from environs import Env

env = Env()
env.read_env()

S3_CONFIG = {
    "endpoint_url": env.str("S3_ENDPOINT_URL"),
    "aws_access_key_id": env.str("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": env.str("AWS_SECRET_ACCESS_KEY"),
    "region_name": "ru-1"
}

DB_PARAMS = {
    "test": {
        "dbname": env.str("POSTGRESQL_DBNAME_TEST"),
        "user": env.str("POSTGRESQL_USER_TEST"),
        "password": env.str("POSTGRESQL_PASSWORD_TEST"),
        "host": env.str("POSTGRESQL_HOST_TEST"),
        "port": 5432
    },
    "prod": {
        "dbname": env.str("POSTGRESQL_DBNAME_PROD"),
        "user": env.str("POSTGRESQL_USER_PROD"),
        "password": env.str("POSTGRESQL_PASSWORD_PROD"),
        "host": env.str("POSTGRESQL_HOST_PROD"),
        "port": 5432
    }
}

S3_BUCKET = env.str("S3_BUCKET_NAME", default="my-bucket")

UNIQUE_COLUMNS = {
    "companies": "company_name",
    "cars": "car_code",
    "drivers": "driver_code",
    "contracts": "contract_number",
    "payments": "payment_id"
}