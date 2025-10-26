import sqlite3
import os

# --- Configuration ---
DATABASE_FILE = 'chemview_harvest.db'
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

            -- Set the primary key as the combination of the two IDs
            PRIMARY KEY (chemical_id, file_type)
        );
        """

        # Execute the table creation command
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{TABLE_NAME}' checked/created successfully.")

    except sqlite3.Error as e:
        print(f"An error occurred during database setup: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")


# --- Execution ---
if __name__ == "__main__":
    setup_database()

    # Optional: Verify file creation
    if os.path.exists(DATABASE_FILE):
        print(f"\nVerification: The database file '{DATABASE_FILE}' exists and is ready for use.")
    else:
        print("\nVerification: Failed to create database file.")