import sqlite3
import os

# --- Configuration ---
DATABASE_FILE = 'chemview_harvest.db'
DATABASE_FILE = 'chemview_test.db'
TABLE_NAME = 'harvest_log'  # Renamed table


def setup_database():
    """
    Connects to the SQLite database and creates the harvest_log table
    if it does not already exist.
    """
    print(f"Attempting to connect to database file: {DATABASE_FILE}")

    try:
        # Connect to the database. If the file doesn't exist, it will be created.
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        print("Successfully connected to SQLite database.")

        # SQL to create the table with the refined schema
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            chemical_id TEXT NOT NULL,
            file_type TEXT NOT NULL,
            local_filepath TEXT,
            last_success_datetime DATETIME,
            last_failure_datetime DATETIME,
            navigate_via TEXT,

            -- Set the primary key as the combination of the two IDs
            PRIMARY KEY (chemical_id, file_type)
        );
        """

        # Execute the table creation command
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{TABLE_NAME}' checked/created successfully.")

        # Also ensure the chemical_info table exists (idempotent)
        create_chemical_info_table(conn)

    except sqlite3.Error as e:
        print(f"An error occurred during database setup: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")


def create_chemical_info_table(conn):
    """Create the `chemical_info` table if it does not already exist.

    Schema:
      - chemical_id TEXT NOT NULL PRIMARY KEY
      - chemview_db_id TEXT NOT NULL
      - name TEXT

    The `name` field may contain long strings with spaces, commas, dashes, etc.
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS chemical_info (
        chemical_id TEXT NOT NULL,
        chemview_db_id TEXT NOT NULL,
        name TEXT,
        PRIMARY KEY (chemical_id)
    );
    """
    try:
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        print("Table 'chemical_info' checked/created successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred creating chemical_info table: {e}")


# --- Execution ---
if __name__ == "__main__":
    setup_database()

    # Optional: Verify file creation
    if os.path.exists(DATABASE_FILE):
        print(f"\nVerification: The database file '{DATABASE_FILE}' exists and is ready for use.")
    else:
        print("\nVerification: Failed to create database file.")