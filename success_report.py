import sqlite3
from pathlib import Path

def success_report(db_path: str, output_file: str):
    """Generate a report of successes and failures for each file_type."""
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query to count successes and failures for each file_type
        query = """
        SELECT file_type,
               SUM(CASE WHEN last_success_datetime IS NOT NULL THEN 1 ELSE 0 END) AS success_count,
               SUM(CASE WHEN last_failure_datetime IS NOT NULL AND last_success_datetime IS NULL THEN 1 ELSE 0 END) AS failure_count
        FROM harvest_log
        GROUP BY file_type;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        # Write the report to the output file
        output_path = Path(output_file)
        with output_path.open('w', encoding='utf-8') as report:
            report.write("File Type Report\n")
            report.write("================\n")
            for file_type, success_count, failure_count in results:
                report.write(f"File Type: {file_type}\n")
                report.write(f"  Successes: {success_count}\n")
                report.write(f"  Failures: {failure_count}\n")
                report.write("\n")

        print(f"Report generated successfully: {output_file}")

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
    output_file = "success_report.txt"

    # Generate the report
    success_report(db_path, output_file)
