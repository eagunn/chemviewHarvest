import argparse
import subprocess
from HarvestDB import HarvestDB

def delete_chemical_records(chemical_id):
    """Delete success records for the given chemical_id from the database."""
    try:
        db = HarvestDB()
        db.delete_success_records(chemical_id)
        print(f"Successfully deleted records for chemical_id: {chemical_id}")
    except Exception as e:
        print(f"Error deleting records for chemical_id {chemical_id}: {e}")


def run_harvest_script():
    """Run the harvestSubstantialRisk.py script with the specified commandline arguments."""
    try:
        command = [
            'python',
            './harvestSubstantialRisk.py',
            '--headless',
            '--input-file', './input_files/srExportTest1.csv',
            '--max-downloads', '3'
        ]
        print("About to execute command:", ' '.join(command))
        subprocess.run(command, check=True)
        print("harvestSubstantialRisk.py executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running harvestSubstantialRisk.py: {e}")


def main():
    parser = argparse.ArgumentParser(description="Delete chemical records and run harvest script.")
    parser.add_argument("chemical_id", help="The chemical ID to delete records for.")
    args = parser.parse_args()

    # Delete records for the given chemical_id
    delete_chemical_records(args.chemical_id)

    # Run the harvest script
    run_harvest_script()


if __name__ == "__main__":
    main()
