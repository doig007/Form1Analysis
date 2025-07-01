import sqlite3
import pandas as pd

DATABASE_FILE = 'ferc_form1.db'
OUTPUT_FILE = 'database_structure.txt'  # The file we will write to

print(f"--- Inspecting Database: {DATABASE_FILE} ---")

try:
    # Open the output file in write mode. 'with' ensures it's properly closed.
    with open(OUTPUT_FILE, 'w') as f:
        f.write(f"--- Inspection Report for Database: {DATABASE_FILE} ---\n\n")

        # Connect to the SQLite database
        con = sqlite3.connect(DATABASE_FILE)
        cursor = con.cursor()

        # Get a list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = sorted([table[0] for table in tables])

        f.write("=== TABLES FOUND ===\n")
        for name in table_names:
            f.write(f"{name}\n")
        f.write("\n" + "="*20 + "\n\n")

        f.write("=== SCHEMA FOR EACH TABLE ===\n")
        # For each table, write its name and its columns to the file
        for table_name in table_names:
            f.write(f"\n--- Table: {table_name} ---\n")
            try:
                # A robust way to get column names using pandas
                df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1", con)
                for col in df.columns:
                    f.write(f"  - {col}\n")
            except Exception as e:
                f.write(f"  [Could not read schema for this table: {e}]\n")
        
        con.close()

    print(f"Success! The database structure has been saved to '{OUTPUT_FILE}'")

except Exception as e:
    print(f"An error occurred: {e}")
    print(f"Please ensure the database file '{DATABASE_FILE}' is in the same directory.")