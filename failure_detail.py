import sqlite3
from pathlib import Path

def failure_detail_report(db_path: str, output_file: str):
    """Generate a detailed report of failures for each chemical_id."""
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query to fetch chemical_id, file_type, and last_failure_datetime for failures
        query = """
        SELECT chemical_id, file_type, last_failure_datetime
        FROM harvest_log
        WHERE last_failure_datetime IS NOT NULL
        ORDER BY chemical_id ASC;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        # Write the report to the output file
        output_path = Path(output_file)
        unique_chemicals = set()
        filetype_failures = {}
        with output_path.open('w', encoding='utf-8') as report:
            report.write("Failure Detail Report\n")
            report.write("======================\n")

            current_chemical_id = None
            for chemical_id, file_type, last_failure_datetime in results:
                # Aggregate unique chemicals and file type failures
                unique_chemicals.add(chemical_id)
                if file_type not in filetype_failures:
                    filetype_failures[file_type] = 0
                filetype_failures[file_type] += 1

                # Write the details to the report
                if chemical_id != current_chemical_id:
                    if current_chemical_id is not None:
                        report.write("\n")  # Add a blank line between different chemical_ids
                    report.write(f"Chemical ID: {chemical_id}\n")
                    current_chemical_id = chemical_id
                report.write(f"  File Type: {file_type}, Last Failure: {last_failure_datetime}\n")

            # Add totals to the bottom of the report
            report.write("\nSummary\n")
            report.write("=======\n")
            report.write(f"Total unique chemicals with failures: {len(unique_chemicals)}\n")
            report.write("Total failures by file type:\n")
            for file_type, count in filetype_failures.items():
                report.write(f"  {file_type}: {count}\n")

        print(f"Failure detail report generated successfully: {output_file}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")

    except Exception as e:
        print(f"Unexpected error: {e}")

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Path to the database and output file
    db_path = "chemview_harvest.db"
    output_file = "failure_detail_report.txt"

    # Generate the failure detail report
    failure_detail_report(db_path, output_file)
