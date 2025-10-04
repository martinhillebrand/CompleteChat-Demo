import teradatasql
import os
import json
import csv
from pathlib import Path

def upload_sample_data():
    """
    Upload sample data from sample_data.json into tables in Teradata.
    Creates separate tables for each data category with row_id (INTEGER) and txt (VARCHAR(5000)).
    Uses batch insert via CSV for optimal performance.
    """
    # Get connection parameters from environment
    host = os.getenv("TD_HOST")
    user = os.getenv("TD_USER")
    password = os.getenv("TD_USER_PASSWORD")

    if not all([host, user, password]):
        raise ValueError("TD_HOST, TD_USER, and TD_USER_PASSWORD must be set as environment variables")

    # Load sample data
    sample_data_path = Path(__file__).parent / "sample_data.json"
    with open(sample_data_path, 'r') as f:
        sample_data = json.load(f)

    # Connect to Teradata
    with teradatasql.connect(
        host=host,
        user=user,
        password=password,
        encryptdata="true"
    ) as conn:
        with conn.cursor() as cur:
            tables_created = []

            for category, content in sample_data.items():
                table_name = f"input_{category}"
                data_rows = content['data']

                # Create table
                print(f"Creating table {user}.{table_name}...")
                cur.execute(f"""
                    CREATE TABLE {user}.{table_name} (
                        row_id INTEGER,
                        txt VARCHAR(5000) CHARACTER SET UNICODE
                    )
                    PRIMARY INDEX (row_id)
                """)

                # Create temporary CSV file for batch insert
                csv_file = f"temp_{category}.csv"
                try:
                    # Write data to CSV
                    with open(csv_file, 'w', encoding='UTF8', newline='') as f:
                        writer = csv.writer(f)
                        # Write header
                        writer.writerow(["row_id", "txt"])
                        # Write data rows
                        for row_id, text in data_rows:
                            writer.writerow([row_id, text])

                    # Batch insert from CSV
                    print(f"Batch inserting {len(data_rows)} rows into {table_name}...")
                    cur.execute(f"{{fn teradata_read_csv({csv_file})}} INSERT INTO {table_name} (?, ?)")

                finally:
                    # Clean up temporary CSV file
                    if os.path.exists(csv_file):
                        os.remove(csv_file)

                tables_created.append(table_name)
                print(f"Table {table_name} created and populated successfully")

            print(f"\nAll tables created successfully: {', '.join(tables_created)}")


def remove_sample_data():
    """
    Remove tables created by upload_sample_data() from Teradata.
    Drops all tables corresponding to categories in sample_data.json.
    """
    # Get connection parameters from environment
    host = os.getenv("TD_HOST")
    user = os.getenv("TD_USER")
    password = os.getenv("TD_USER_PASSWORD")

    if not all([host, user, password]):
        raise ValueError("TD_HOST, TD_USER, and TD_USER_PASSWORD must be set as environment variables")

    # Load sample data to get category names
    sample_data_path = Path(__file__).parent / "sample_data.json"
    with open(sample_data_path, 'r') as f:
        sample_data = json.load(f)

    # Connect to Teradata
    with teradatasql.connect(
        host=host,
        user=user,
        password=password,
        encryptdata="true"
    ) as conn:
        with conn.cursor() as cur:
            tables_removed = []

            for category in sample_data.keys():
                table_name = f"input_{category}"

                try:
                    print(f"Dropping table {table_name}...")
                    cur.execute(f"DROP TABLE {user}.{table_name}")
                    tables_removed.append(table_name)
                    print(f"Table {table_name} dropped successfully")
                except Exception as e:
                    print(f"Warning: Could not drop table {table_name}: {e}")

            if tables_removed:
                print(f"\nTables removed successfully: {', '.join(tables_removed)}")
            else:
                print("\nNo tables were removed")