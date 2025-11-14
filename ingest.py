import os
import sqlite3
from pathlib import Path

import pandas as pd


DATA_DIR = Path("data")
DB_PATH = Path("ecommerce.db")

TABLE_SPECS = {
    "users": {
        "primary_key": "user_id",
        "dtype_overrides": {
            "date_joined": "TEXT",
            "is_active": "INTEGER",
        },
    },
    "products": {
        "primary_key": "product_id",
        "dtype_overrides": {
            "created_at": "TEXT",
            "is_active": "INTEGER",
        },
    },
    "orders": {
        "primary_key": "order_id",
        "dtype_overrides": {
            "order_date": "TEXT",
            "ship_date": "TEXT",
        },
    },
    "order_items": {
        "primary_key": "order_item_id",
    },
    "payments": {
        "primary_key": "payment_id",
        "dtype_overrides": {
            "payment_date": "TEXT",
        },
    },
}


def infer_sqlite_type(series: pd.Series) -> str:
    if series.dtype.kind in {"i"}:
        return "INTEGER"
    if series.dtype.kind in {"f"}:
        return "REAL"
    if series.dtype.kind in {"b"}:
        return "INTEGER"
    return "TEXT"


def dataframe_to_table_schema(df: pd.DataFrame, table_name: str) -> str:
    spec = TABLE_SPECS[table_name]
    pk = spec["primary_key"]
    overrides = spec.get("dtype_overrides", {})

    column_defs = []
    for column in df.columns:
        if column in overrides:
            sql_type = overrides[column]
        else:
            sql_type = infer_sqlite_type(df[column])
        pk_clause = " PRIMARY KEY" if column == pk else ""
        column_defs.append(f'"{column}" {sql_type}{pk_clause}')

    return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"


def load_dataframe(csv_name: str) -> pd.DataFrame:
    csv_path = DATA_DIR / f"{csv_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV file: {csv_path}")
    return pd.read_csv(csv_path)


def main():
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Data directory not found: {DATA_DIR.resolve()}")

    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()

        for table_name in TABLE_SPECS.keys():
            df = load_dataframe(table_name)
            df.columns = [col.strip() for col in df.columns]

            create_sql = dataframe_to_table_schema(df, table_name)
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.execute(create_sql)

            df.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Inserted {len(df)} rows into '{table_name}'.")

        conn.commit()
        print(f"SQLite database ready at {DB_PATH.resolve()}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

