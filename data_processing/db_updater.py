import psycopg2

from psycopg2.extras import execute_batch
from app_logger import get_logger

logger = get_logger(__name__)


def check_unique_constraint(cursor, table_name, column_name):
    query = """
    SELECT 1
    FROM information_schema.constraint_column_usage AS ccu
    LEFT JOIN information_schema.table_constraints AS tc
    ON ccu.constraint_name = tc.constraint_name
    WHERE tc.table_name = %s AND ccu.column_name = %s AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE');
    """
    cursor.execute(query, (table_name, column_name))
    return cursor.fetchone() is not None


class DBUpdater:
    def __init__(self, db_params, unique_columns):
        self.db_params = db_params
        self.unique_columns = unique_columns

    def update_table(self, table_name, df):
        if df.empty:
            return

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cursor:
                columns = ", ".join(df.columns)
                placeholders = ", ".join(["%s"] * len(df.columns))
                unique_col = self.unique_columns.get(table_name)
                
                if not check_unique_constraint(cursor, table_name, unique_col):
                    logger.error(f"Column '{unique_col}' in table '{table_name}' does not have a UNIQUE constraint.")
                    return

                query = f"""
                    INSERT INTO {table_name} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT ({unique_col}) DO UPDATE SET
                    {", ".join(f"{col}=EXCLUDED.{col}" for col in df.columns if col != unique_col)}
                """

                try:
                    execute_batch(cursor, query, df.values.tolist(), page_size=5000)
                except Exception as e:
                    logger.error(f"Error updating table {table_name}: {e}")
