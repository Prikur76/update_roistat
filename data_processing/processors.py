import pandas as pd
from tools import clean_phone, get_snakecase_row, split_fio
from datetime import datetime


def process_contracts(df: pd.DataFrame) -> pd.DataFrame:
    new_columns = {
        "date": "contract_date",
        "number": "contract_number",
        "driver_id": "driver_code",
        "status": "contract_status",
        "car": "car_number",
        "company": "company_name"
    }
    df = df.rename(columns=new_columns).drop(columns=["timestamp"], errors="ignore")
    return df


def process_payments(df: pd.DataFrame) -> pd.DataFrame:
    rename_columns = {
        "number": "payment_id",
        "date": "payment_date",
        "plus": "payment_plus",
        "minus": "payment_minus",
        "driver": "driver_fio",
        "sts": "sts_full",
        "user": "manager_fio"
    }
    df = df.rename(columns=rename_columns)
    df = df[[
        "payment_id", "payment_method", "payment_group", "balance_name",
        "payment_date", "oper_date", "oper_time", "payment_plus", "payment_minus",
        "description", "driver_fio", "car_number", "sts_full", "hand_correction",
        "manager_fio"
    ]]
    df = df[df.payment_group.isin(["Аренда автомобиля", "Аренда детского кресла", "Аренда устройства"])]
    df["sts_full"] = df["sts_full"].apply(lambda x: str(x).replace(".0", "") if x else None)
    return df


def process_companies(df: pd.DataFrame) -> pd.DataFrame:
    df = df[~df["private_person"].astype(bool)]
    df.columns = [get_snakecase_row(col, prefix="company") for col in df.columns]
    df = df[["company_name", "company_full_name", "company_inn", "company_kpp", "company_ogrn"]].astype(str)
    return df

def process_cars(df: pd.DataFrame) -> pd.DataFrame:
    columns = {
        "code": "car_code",
        "vin": "car_vin",
        "model": "car_model",
        "number": "car_number",
        "year_car": "car_year",
        "sts_series": "sts_series",
        "sts_number": "sts_number",
        "sts_issue_date": "sts_issue_date",
        "sts_validity_date": "sts_validity_date",
        "organization": "company_name",
        "activity": "is_active"
    }
    df = df.rename(columns=columns)
    
    df["sts_series"] = df["sts_series"].apply(lambda x: str(x).replace(".0", "") if pd.notnull(x) else None)
    df["sts_number"] = df["sts_number"].apply(lambda x: str(x).replace(".0", "").zfill(8) if pd.notnull(x) else None)
    df["sts_full"] = df.apply(lambda x: f"{x['sts_series']}{x['sts_number']}", axis=1)
    
    df = df[
        (df["car_vin"].notnull()) &
        (df["car_year"] != "0001-01-01T00:00:00") &
        (df["car_model"].notnull()) &
        (df["car_number"].notnull()) &
        (df["sts_series"].notnull()) &
        (df["sts_number"].notnull())
    ]    
    df["car_year"] = pd.to_datetime(df["car_year"]).dt.year
    df["car_number"] = df["car_number"].str.upper()
    df["car_model"] = df["car_model"].str.upper()
    df = df.dropna(subset=["car_vin", "car_model", "car_number", "sts_full"])
    df = df[
        [
            "car_code", "car_vin", "car_model", "car_number", "car_year", 
            "sts_series", "sts_number", "sts_full", "sts_issue_date", "sts_validity_date",
            "company_name", "is_active"
        ]
    ]
    return df


def process_drivers(df: pd.DataFrame) -> pd.DataFrame:
    df = df\
        .dropna(subset=["drivers_license_serial_number", "fio", "phone_number"])\
        .drop_duplicates(subset=["fio", "phone_number"])\
        .sort_values("id")
    
    df = df.rename(columns={
        "id": "driver_code",
        "fio": "driver_fio",
        "driver_date_create": "created_at"
    })
    
    df["phone_number"] = df["phone_number"].apply(clean_phone)
    df["driver_fio"] = df["driver_fio"].str.strip()
    df[["lastname", "firstname", "middlename", "middlename_suffix"]] = df["driver_fio"].apply(lambda x: pd.Series(split_fio(x)))
    df = df[
        [
            "driver_code", "driver_fio", "lastname", "firstname", "middlename",
            "middlename_suffix", "sex", "birth_date", "phone_number", "email",
            "created_at", "marketing", "status"
        ]
    ].dropna(subset=["driver_fio", "phone_number"])\
     .astype({
        "driver_code": "int64",
        "driver_fio": "string",
        "lastname": "string",
        "firstname": "string",
        "middlename": "string",
        "middlename_suffix": "string",
        "birth_date": "string",
        "phone_number": "string",
        "created_at": "string",
        "status": "string"
    })

    return df