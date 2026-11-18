import os
import logging
from io import BytesIO
from datetime import datetime, timezone

import pyodbc
import pandas as pd
from google.cloud import storage


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

SQL_SERVER = os.environ["SQL_SERVER"]
SQL_DATABASE = os.environ["SQL_DATABASE"]
SQL_USERNAME = os.environ["SQL_USERNAME"]
SQL_PASSWORD = os.environ["SQL_PASSWORD"]

GCS_BUCKET = os.getenv("GCS_BUCKET", "exter_mad")

# Destination object must match the Airflow sensor
GCS_OBJECT = "Messy_Employee_dataset.csv"

SQL_DRIVER = os.getenv(
    "SQL_DRIVER",
    "ODBC Driver 18 for SQL Server"
)

# SQL Server employee table
SQL_QUERY = """
SELECT *
FROM dbo.employee;
"""


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# --------------------------------------------------
# CONNECT TO SQL SERVER
# --------------------------------------------------

def connect_to_sql_server():

    logging.info("Connecting to SQL Server...")

    connection_string = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    connection = pyodbc.connect(connection_string)

    logging.info("SQL Server connection successful")

    return connection


# --------------------------------------------------
# EXTRACT EMPLOYEE DATA
# --------------------------------------------------

def extract_employee_data():

    connection = None

    try:
        connection = connect_to_sql_server()

        logging.info("Extracting employee records...")

        df = pd.read_sql_query(
            SQL_QUERY,
            connection
        )

        if df.empty:
            raise ValueError(
                "No employee records found in SQL Server"
            )

        logging.info(
            "Extracted %s employee records",
            len(df)
        )

        return df

    finally:
        if connection is not None:
            connection.close()
            logging.info("SQL Server connection closed")


# --------------------------------------------------
# CONVERT DATA INTO CSV
# --------------------------------------------------

def convert_to_csv(df):

    logging.info("Converting employee data to CSV...")

    csv_buffer = BytesIO()

    df.to_csv(
        csv_buffer,
        index=False,
        encoding="utf-8"
    )

    csv_buffer.seek(0)

    logging.info("CSV conversion completed")

    return csv_buffer


# --------------------------------------------------
# UPLOAD CSV TO GOOGLE CLOUD STORAGE
# --------------------------------------------------

def upload_to_gcs(csv_buffer):

    logging.info("Connecting to Google Cloud Storage...")

    # Uses Application Default Credentials
    client = storage.Client()

    bucket = client.bucket(GCS_BUCKET)

    blob = bucket.blob(GCS_OBJECT)

    logging.info("Uploading employee CSV to GCS...")

    blob.upload_from_file(
        csv_buffer,
        content_type="text/csv",
        rewind=True
    )

    logging.info(
        "Successfully uploaded gs://%s/%s",
        GCS_BUCKET,
        GCS_OBJECT
    )


# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------

def main():

    logging.info("Employee ingestion pipeline started")

    try:
        # Step 1: Extract from SQL Server
        employee_df = extract_employee_data()

        # Step 2: Convert to CSV
        csv_buffer = convert_to_csv(employee_df)

        # Step 3: Upload to GCS
        upload_to_gcs(csv_buffer)

        logging.info(
            "Employee ingestion pipeline completed successfully"
        )

    except Exception:
        logging.exception(
            "Employee ingestion pipeline failed"
        )
        raise


if __name__ == "__main__":
    main()