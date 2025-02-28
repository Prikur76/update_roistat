import pandas as pd

from app_logger import get_logger

logger = get_logger(__name__)


def validate_companies(df):
    required_columns = ["name", "full_name", "inn", "ogrn"]    
    if not all(col in df.columns for col in required_columns):
        logger.error("Отсутствуют необходимые столбцы в данных компаний.")
        return False
    return True


def validate_cars(df):
    required_columns = ["code", "vin", "sts_series", "sts_number", "model"]    
    if not all(col in df.columns for col in required_columns):
        logger.error("Отсутствуют необходимые столбцы в данных автомобилей.")
        return False
    return True

# Добавьте остальные валидаторы...
def validate_drivers(df):
    required_columns = ["id", "fio", "phone_number", "drivers_license_serial_number"]    
    if not all(col in df.columns for col in required_columns):
        logger.error("Отсутствуют необходимые столбцы в данных водителей.")
        return False
    return True


def validate_payments(df):
    required_columns = ["number", "date", "minus", "id"]    
    if not all(col in df.columns for col in required_columns):
        logger.error("Отсутствуют необходимые столбцы в данных платежей.")
        return False
    return True


def validate_contracts(df):
    required_columns = ["driver_id", "date", "number", "company", "car", "status"]    
    if not all(col in df.columns for col in required_columns):
        logger.error("Отсутствуют необходимые столбцы в данных контрактов.")
        return False
    return True


def validate_data(table_name, df):
    validators = {
        "companies": validate_companies,
        "cars": validate_cars,
        "drivers": validate_drivers,
        "payments": validate_payments,
        "contracts": validate_contracts
    }
    validator = validators.get(table_name)
    if validator:
        return validator(df)
    return True
