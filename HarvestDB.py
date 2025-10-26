import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

# --- Configuration ---
DATABASE_FILE = 'chemview_harvest.db'
TABLE_NAME = 'harvest_log'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class HarvestDB:
    """
    Wrapper class for all database interactions with the harvest_log table.
    """

    def __init__(self, db_file: str = DATABASE_FILE):
        """Initializes the database connection file path."""
        self.db_file = db_file

    def _execute_query(self, sql: str, params: Tuple = ()) -> Optional[sqlite3.Cursor]:
        """Handles connecting, executing, committing, and closing the connection."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            return cursor
        except sqlite3.Error as e:
            print(f"Database Error: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_harvest_status(self, chemical_id: str, file_type: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the status record for a specific chemical_id and file_type.

        Returns a dict containing all columns, or None if the record doesn't exist.
        """
        sql = f"""
        SELECT local_filepath, last_success_datetime, last_failure_datetime
        FROM {TABLE_NAME}
        WHERE chemical_id = ? AND file_type = ?;
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            conn.row_factory = sqlite3.Row  # This allows accessing columns by name
            cursor = conn.cursor()
            cursor.execute(sql, (chemical_id, file_type))

            row = cursor.fetchone()
            if row:
                # Convert sqlite3.Row object to a standard dictionary
                return dict(row)
            return None

        except sqlite3.Error as e:
            print(f"Database Read Error: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def log_success(self, chemical_id: str, file_type: str, local_filepath: str) -> bool:
        """
        Logs a successful download. Sets success datetime and clears failure datetime.
        Uses INSERT OR REPLACE to either add a new record or update an existing one.
        """
        now = datetime.now().strftime(DATE_FORMAT)
        sql = f"""
        INSERT OR REPLACE INTO {TABLE_NAME} 
        (chemical_id, file_type, local_filepath, last_success_datetime, last_failure_datetime)
        VALUES (?, ?, ?, ?, NULL);
        """
        params = (chemical_id, file_type, local_filepath, now)
        return self._execute_query(sql, params) is not None

    def log_failure(self, chemical_id: str, file_type: str) -> bool:
        """
        Logs a failed download attempt. Updates the failure datetime.
        It preserves any existing success status and local_filepath.
        """
        now = datetime.now().strftime(DATE_FORMAT)

        # SQL to insert a new record if it doesn't exist, or update only the failure column if it does.
        sql = f"""
        INSERT INTO {TABLE_NAME} (chemical_id, file_type, last_failure_datetime) 
        VALUES (?, ?, ?)
        ON CONFLICT (chemical_id, file_type) DO UPDATE SET
            last_failure_datetime = excluded.last_failure_datetime;
        """
        params = (chemical_id, file_type, now)
        return self._execute_query(sql, params) is not None


# --- Example Usage (Demonstration) ---
if __name__ == "__main__":
    # NOTE: You must run the setup script once before running this.
    db = HarvestDB()
    test_id = "CHEM_1234"
    test_file_type = "ReportA"

    print("\n--- 1. Check Initial Status ---")
    status = db.get_harvest_status(test_id, test_file_type)
    print(f"Status before any action: {status}")

    print("\n--- 2. Log First Failure ---")
    db.log_failure(test_id, test_file_type)
    status = db.get_harvest_status(test_id, test_file_type)
    print(f"Status after failure: {status}")

    print("\n--- 3. Log Success ---")
    file_path = f"/data/{test_id}_{test_file_type}.pdf"
    db.log_success(test_id, test_file_type, file_path)
    status = db.get_harvest_status(test_id, test_file_type)
    print(f"Status after success: {status}")

    print("\n--- 4. Log a subsequent Failure (should keep success info) ---")
    db.log_failure(test_id, test_file_type)
    status = db.get_harvest_status(test_id, test_file_type)
    # The important part: last_success_datetime and local_filepath should remain populated.
    print(f"Status after subsequent failure: {status}")