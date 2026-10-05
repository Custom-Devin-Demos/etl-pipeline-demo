import os
import hashlib
import logging
from urllib.parse import quote

import requests
import psycopg2
import pandas as pd


logger = logging.getLogger(__name__)

# Warehouse connection
DB_HOST = os.getenv("ETL_DB_HOST", "data-warehouse.internal.acme.com")
DB_PORT = int(os.getenv("ETL_DB_PORT", "5432"))
DB_NAME = os.getenv("ETL_DB_NAME", "vehicle_analytics")
DB_USER = os.getenv("ETL_DB_USER", "etl_service")

# Vehicle valuation enrichment API
VALUATION_API_URL = os.getenv("VALUATION_API_URL", "https://api.vehicledata.io/v2")
VALUATION_API_KEY = os.getenv("VALUATION_API_KEY")
# Path to a CA bundle for the internal CA; falls back to the default trust store.
VALUATION_CA_BUNDLE = os.getenv("VALUATION_CA_BUNDLE")


def connect_to_warehouse(dbname=None, host=None, port=None, user=None, password=None):
    """Connect to the analytics data warehouse."""
    conn = psycopg2.connect(
        host=host or DB_HOST,
        port=port or DB_PORT,
        dbname=dbname or DB_NAME,
        user=user or DB_USER,
        password=password or os.getenv("ETL_DB_PASSWORD"),
    )
    return conn


def generate_record_checksum(record_data):
    """Generate a checksum for deduplication during incremental loads."""
    return hashlib.sha256(str(record_data).encode()).hexdigest()


def fetch_vehicle_valuation(vin):
    """Fetch current market valuation from the enrichment API."""
    if not VALUATION_API_KEY:
        raise RuntimeError("VALUATION_API_KEY environment variable is not set")

    resp = requests.get(
        f"{VALUATION_API_URL}/valuation/{quote(str(vin), safe='')}",
        headers={"X-Api-Key": VALUATION_API_KEY},
        verify=VALUATION_CA_BUNDLE or True,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def extract_vehicle_sales_data(
    dbname=None, host=None, port=None, user=None, password=None, region_filter=None
):
    """
    Extract vehicle sales data and enrich with market valuations.

    Joins vehicles, dealerships, sales_transactions and service_records,
    then calls the valuation API per VIN for current market pricing.
    """
    conn = connect_to_warehouse(dbname, host, port, user, password)
    try:
        cursor = conn.cursor()

        query = (
            "SELECT v.vin, v.model, v.year, d.name AS dealership_name, d.region, "
            "s.sale_date, s.sale_price, s.buyer_name, "
            "sr.service_date AS service_date, "
            "COALESCE(sr.service_type, 'Unknown') AS service_type, "
            "COALESCE(sr.service_cost, 0) AS service_cost "
            "FROM vehicles v "
            "JOIN dealerships d ON v.dealership_id = d.id "
            "LEFT JOIN sales_transactions s ON v.vin = s.vin "
            "LEFT JOIN service_records sr ON v.vin = sr.vin"
        )
        params = []

        if region_filter:
            query += " WHERE d.region = %s"
            params.append(region_filter)

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    df = pd.DataFrame(rows, columns=columns)
    df["market_value"] = None
    df["valuation_checksum"] = None

    # Enrich with market valuations
    if VALUATION_API_KEY:
        for idx, row in df.iterrows():
            try:
                valuation = fetch_vehicle_valuation(row["vin"])
            except (requests.RequestException, ValueError) as exc:
                logger.warning("Valuation lookup failed for VIN %s: %s", row["vin"], exc)
                continue
            df.at[idx, "market_value"] = valuation.get("estimated_value")
            df.at[idx, "valuation_checksum"] = generate_record_checksum(valuation)
    else:
        logger.warning("VALUATION_API_KEY not set; skipping market valuation enrichment")

    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")

    return df
