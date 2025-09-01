import os
import sqlite3
import json

DB_PATH = "app.db"
CUSTOMERS_JSON = "customers.json"


def init_db() -> None:
    """Initialise the SQLite database with sample customer data."""
    create = not os.path.exists(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    if create:
        conn.execute(
            """
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT
            )
            """
        )
        with open(CUSTOMERS_JSON, "r", encoding="utf-8") as f:
            customers = json.load(f)
        conn.executemany(
            "INSERT INTO customers (customer_id, first_name, last_name, email) VALUES (?, ?, ?, ?)",
            [
                (
                    c["customer_id"],
                    c["first_name"],
                    c["last_name"],
                    c["email"],
                )
                for c in customers
            ],
        )
        conn.commit()
    conn.close()


def get_connection():
    return sqlite3.connect(DB_PATH)

