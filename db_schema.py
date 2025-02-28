import psycopg2

from app_logger import get_logger

logger = get_logger(__name__)


class DBSchema:
    def __init__(self, db_params):
        self.db_params = db_params

    def create_tables(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS companies (
                company_id SERIAL PRIMARY KEY,
                company_name VARCHAR(100) UNIQUE,
                company_full_name VARCHAR(400),
                company_inn VARCHAR(20),
                company_kpp VARCHAR(20),
                company_ogrn VARCHAR(20)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS cars (
                car_id SERIAL PRIMARY KEY,
                car_code BIGINT UNIQUE,
                car_vin VARCHAR(20),
                car_model VARCHAR(100),
                car_year INT,
                sts_series VARCHAR(10),
                sts_number VARCHAR(20),
                sts_full VARCHAR(30) UNIQUE,
                car_number VARCHAR(20),
                company_name VARCHAR(100),
                is_active BOOLEAN DEFAULT TRUE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS drivers (
                driver_id SERIAL PRIMARY KEY,
                driver_code BIGINT NOT NULL UNIQUE,
                driver_fio VARCHAR(255),
                lastname VARCHAR(100),
                firstname VARCHAR(50),
                middlename VARCHAR(50),
                middlename_suffix VARCHAR(50),
                sex VARCHAR(20),
                birth_date TIMESTAMP,
                phone_number VARCHAR(30),
                email VARCHAR(100),
                marketing VARCHAR(100),
                status VARCHAR(100)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS contracts (
                contract_id SERIAL PRIMARY KEY,
                contract_number VARCHAR(50) NOT NULL UNIQUE,
                contract_date TIMESTAMP NOT NULL,
                company_name VARCHAR(100) NOT NULL,
                car_number VARCHAR(50) NOT NULL,
                contract_status VARCHAR(50) NOT NULL,
                driver_code BIGINT NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS payments (
                payment_id VARCHAR(100) PRIMARY KEY NOT NULL,
                payment_method VARCHAR(150) NOT NULL,
                payment_group VARCHAR(100) NOT NULL,
                balance_name VARCHAR(100) NOT NULL,
                payment_date TIMESTAMP NOT NULL,
                oper_date TIMESTAMP NOT NULL,
                oper_time TIMESTAMP NOT NULL,
                payment_plus DECIMAL(10, 2) DEFAULT 0.0,
                payment_minus DECIMAL(10, 2) DEFAULT 0.0,
                description VARCHAR,
                driver_fio VARCHAR(255) NOT NULL,
                car_number VARCHAR(50),
                sts_full VARCHAR(30),
                hand_correction BOOLEAN NOT NULL DEFAULT FALSE,
                manager_fio VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """,
            # """
            # ALTER TABLE companies ADD CONSTRAINT unique_company_name UNIQUE (company_name);
            # ALTER TABLE cars ADD CONSTRAINT unique_car_code UNIQUE (car_code);
            # ALTER TABLE drivers ADD CONSTRAINT unique_driver_code UNIQUE (driver_code);
            # ALTER TABLE contracts ADD CONSTRAINT unique_contract_number UNIQUE (contract_number);
            # ALTER TABLE payments ADD CONSTRAINT unique_payment_id UNIQUE (payment_id);
            # """
        ]

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cursor:
                for query in queries:
                    try:
                        cursor.execute(query)
                    except Exception as e:
                        logger.error(f"Ошибка при создании таблиц: {e}")
