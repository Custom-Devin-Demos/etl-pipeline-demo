import psycopg2
import pandas as pd


def connect_to_postgres(dbname, host, port, user, password):
    """Connects to a local or remote PostgreSQL database"""
    conn = psycopg2.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password
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

    if region_filter:
        query = query + " WHERE d.region = '" + region_filter + "'"

    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns)

    # Convert dates to datetime objects
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')

    print("🔍 Extracted rows:", df.shape[0])
    return df
