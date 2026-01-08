"""
ETL-script för att hämta, transformera och ladda transaktionsdata.
Endast nya transaktioner (baserat på TransactionDate) laddas.
"""

import sqlite3
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from pathlib import Path

# Säker sökväg till databasen (samma katalog som detta script)
DB_PATH = Path(__file__).resolve().parents[1] / "transactions.db"
URL = "http://schizoakustik.se/köksglädje/transactions.csv"


def init_db(conn: sqlite3.Connection) -> None:
    """Skapar tabeller om de inte redan finns."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Transactions (
            TransactionID INTEGER PRIMARY KEY,
            StoreID INTEGER,
            CustomerID INTEGER,
            TransactionDate TEXT,
            TotalAmount REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS TransactionDetails (
            TransactionDetailID INTEGER PRIMARY KEY,
            TransactionID INTEGER,
            ProductID INTEGER,
            CampaignID INTEGER,
            Quantity INTEGER,
            PriceAtPurchase REAL,
            TotalPrice REAL,
            FOREIGN KEY (TransactionID) REFERENCES Transactions(TransactionID)
        )
    """)

    conn.commit()


def get_latest_transaction_date(conn: sqlite3.Connection):
    """Hämtar senaste TransactionDate från databasen."""
    cur = conn.cursor()
    cur.execute("SELECT MAX(TransactionDate) FROM Transactions")
    result = cur.fetchone()[0]
    return pd.to_datetime(result) if result else None


def run_etl() -> None:
    """Huvudfunktion för ETL-flödet."""
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    latest_date = get_latest_transaction_date(conn)

    # 1. Hämta data
    response = requests.get(URL)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text))

    # 2. Standardisera datum
    df["TransactionDate"] = pd.to_datetime(
        df["TransactionDate"],
        format="mixed"
    )

    # 3. Filtrera endast nya transaktioner
    if latest_date is not None:
        df = df[df["TransactionDate"] > latest_date]

    if df.empty:
        print("Ingen ny data att ladda.")
        conn.close()
        return

    # 4. Normalisera data
    transactions_df = df[[
        "TransactionID",
        "StoreID",
        "CustomerID",
        "TransactionDate",
        "TotalAmount"
    ]].drop_duplicates()

    transaction_details_df = df[[
        "TransactionDetailID",
        "TransactionID",
        "ProductID",
        "CampaignID",
        "Quantity",
        "PriceAtPurchase",
        "TotalPrice"
    ]]

    # 5. Ladda till databasen
    transactions_df.to_sql(
        "Transactions",
        conn,
        if_exists="append",
        index=False
    )

    transaction_details_df.to_sql(
        "TransactionDetails",
        conn,
        if_exists="append",
        index=False
    )

    conn.close()
    print(f"Laddade {len(transactions_df)} nya transaktioner.")


if __name__ == "__main__":
    run_etl()

