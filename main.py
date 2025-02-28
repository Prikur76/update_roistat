
import psycopg2

from settings import DB_PARAMS, S3_CONFIG, S3_BUCKET, UNIQUE_COLUMNS
from data_processing.processors import (
    process_companies, process_cars, process_drivers, 
    process_contracts, process_payments)
from data_processing.s3_utils import S3Client
from db_schema import DBSchema
from data_processing.db_updater import DBUpdater
from data_processing.validators import validate_data
from app_logger import get_logger

logger = get_logger(__name__)


def main():
    logger.info("Starting...")

    s3 = S3Client(S3_CONFIG)

    processors = {
        "companies": process_companies,
        "cars": process_cars,
        "drivers": process_drivers,
        "contracts": process_contracts,
        "payments": process_payments
    }

    for env in ["test", "prod"]:
        schema_manager = DBSchema(DB_PARAMS[env])
        updater = DBUpdater(DB_PARAMS[env], UNIQUE_COLUMNS)

        with psycopg2.connect(**DB_PARAMS[env]) as conn:
            with conn.cursor() as cursor:
                schema_manager.create_tables()

        for table, processor in processors.items():
            key = s3.get_latest_object_key(S3_BUCKET, table)
            if not key:
                continue

            df = s3.get_dataframe(S3_BUCKET, key)
            if df.empty:
                continue

            if not validate_data(table, df):
                logger.warning(f"Проверка данных для {table} не пройдена.")
                continue

            processed_df = processor(df)            
            if processed_df.empty:
                continue

            updater.update_table(table, processed_df)
    
    logger.info("Finished.")


if __name__ == "__main__":
    main()