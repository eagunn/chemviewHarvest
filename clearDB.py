import sqlite3
import os

# --- Configuration ---
DATABASE_FILE = 'chemview_harvest.db'
TABLE_NAME = 'harvest_log'


def clear_harvest_log():
    """
    Connects to the database and deletes all records from the harvest_log table.
    It then runs VACUUM to reclaim disk space.
    """
    print(f"Attempting to clear all records from table: {TABLE_NAME} in {DATABASE_FILE}")
    conn = None  # Initialize connection to None

    if not os.path.exists(DATABASE_FILE):
        print(f"ERROR: Database file '{DATABASE_FILE}' not found. Please run the setup script first.")
        return

    try:
        # Connect to the database
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()

        # 1. Execute the DELETE command to remove all rows
        print("Executing DELETE FROM command...")
        cursor.execute(f"DELETE FROM {TABLE_NAME};")

        # Get count of deleted rows (optional, for confirmation)
        deleted_count = cursor.rowcount

        # 2. Commit the transaction to make the deletion permanent
        conn.commit()
        print(f"Successfully deleted {deleted_count} records from {TABLE_NAME}.")

        # 3. Execute VACUUM to reclaim space on the disk
        print("Running VACUUM command to optimize database file size...")
        cursor.execute("VACUUM;")
        conn.commit()
        print("VACUUM complete.")

    except sqlite3.Error as e:
        print(f"An error occurred while clearing the table: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")


# --- Execution ---
if __name__ == "__main__":
    clear_harvest_log()