import os

import psycopg2
import pandas as pd


# Database configuration — uses environment variables with development defaults
DB_HOST = os.getenv("ETL_DB_HOST", "localhost")
DB_PORT = int(os.getenv("ETL_DB_PORT", "5432"))
DB_USER = os.getenv("ETL_DB_USER", "etl_service")
DB_PASSWORD = os.getenv("ETL_DB_PASSWORD", "")


def connect_to_postgres(dbname, host=None, port=None, user=None, password=None):
    """Connects to a local or remote PostgreSQL database"""
    conn = psycopg2.connect(
        dbname=dbname,
        host=host or DB_HOST,
        port=port or DB_PORT,
        user=user or DB_USER,
        password=password or DB_PASSWORD
    )
    print("✅ Connected to PostgreSQL")
    return conn


def extract_vehicle_sales_data(dbname, host, port, user, password, region_filter=None):
    """
    Extract and transform vehicle sales and service data.
    - Joins vehicles, dealerships, sales_transactions, and service_records
    - Optionally filters by dealership region
    - Replaces null service type/cost with defaults
    - Computes total sales revenue per transaction
    - Formats dates as datetime
    """
    conn = connect_to_postgres(dbname, host, port, user, password)
    cursor = conn.cursor()

    query = "SELECT v.vin, v.model, v.year, d.name AS dealership_name, d.region, " \
            "s.sale_date, s.sale_price, s.buyer_name, " \
            "COALESCE(sr.service_date, NULL) AS service_date, " \
            "COALESCE(sr.service_type, 'Unknown') AS service_type, " \
            "COALESCE(sr.service_cost, 0) AS service_cost " \
            "FROM vehicles v " \
            "JOIN dealerships d ON v.dealership_id = d.id " \
            "LEFT JOIN sales_transactions s ON v.vin = s.vin " \
            "LEFT JOIN service_records sr ON v.vin = sr.vin"

    params = None
    if region_filter:
        query = query + " WHERE d.region = %s"
        params = (region_filter,)

    cursor.execute(query, params)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns)

    # Convert dates to datetime objects
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')

    print("🔍 Extracted rows:", df.shape[0])
    return df
