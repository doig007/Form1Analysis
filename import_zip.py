import os
import pandas as pd
from dbfread import DBF
from sqlalchemy import create_engine
import time

# --- 1. CONFIGURATION ---
# IMPORTANT: Update this path to the root directory where your 'f1_2018', 'f1_2019', etc. folders are located.
# The double backslashes are important in Windows paths, or you can use a raw string r"C:\path\..."
BASE_DATA_DIR = r"L:\My Drive\FERC Form 1"

# The specific years you want to process
YEARS_TO_PROCESS = [2018, 2019, 2020]

# The name of the SQLite database file that will be created
DATABASE_FILE = 'ferc_form1.db'

# Create a database engine. This will create the 'ferc_form1.db' file in the same directory as the script.
# The '///' means it's a local file path.
db_uri = f'sqlite:///{DATABASE_FILE}'
engine = create_engine(db_uri)

print(f"Starting data transfer process.")
print(f"Source Directory: {BASE_DATA_DIR}")
print(f"Target Database: {DATABASE_FILE}\n")

# --- 2. MAIN PROCESSING LOOP ---

total_start_time = time.time()

# Loop through each year you want to process
for year in YEARS_TO_PROCESS:
    year_start_time = time.time()
    print(f"--- Processing Year: {year} ---")

    # Construct the path to the 'working' directory based on your structure
    working_dir = os.path.join(BASE_DATA_DIR, f'f1_{year}', 'FORM1', 'working')

    # Check if the directory for the year exists before proceeding
    if not os.path.exists(working_dir):
        print(f"  [Warning] Directory not found, skipping year {year}: {working_dir}")
        continue

    # Get a list of all .dbf files in the working directory
    dbf_files = [f for f in os.listdir(working_dir) if f.lower().endswith('.dbf')]
    
    if not dbf_files:
        print(f"  [Warning] No .dbf files found in {working_dir}")
        continue

    print(f"  Found {len(dbf_files)} DBF files to process.")

    # Loop through each .dbf file found
    for dbf_filename in dbf_files:
        try:
            dbf_path = os.path.join(working_dir, dbf_filename)
            
            # Use the filename (without extension) as the table name. Convert to lowercase for consistency.
            table_name = os.path.splitext(dbf_filename)[0].lower()
            
            print(f"    -> Reading '{dbf_filename}'...")

            # Read the DBF file into a pandas DataFrame
            # Using 'iter' is memory-efficient for potentially large files
            dbf_table = DBF(dbf_path, load=False) 
            df = pd.DataFrame(iter(dbf_table))
            
            if df.empty:
                print(f"      [Info] File '{dbf_filename}' is empty, skipping.")
                continue

            # **CRUCIAL STEP**: Add a 'year' column to identify the data's origin
            df['year'] = year

            # **BEST PRACTICE**: Standardize column names to lowercase
            df.columns = df.columns.str.lower()

            print(f"      -> Loading data into SQL table '{table_name}'...")
            
            # Load the DataFrame into the SQLite database
            # if_exists='append': Creates the table if it doesn't exist, and appends data if it does.
            # This is key to building a multi-year dataset in each table.
            # index=False: Prevents pandas from writing its own index column into the database.
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False
            )
        
        except Exception as e:
            print(f"      [ERROR] Failed to process {dbf_filename} for year {year}. Error: {e}")

    year_end_time = time.time()
    print(f"--- Finished processing {year} in {year_end_time - year_start_time:.2f} seconds. ---\n")

total_end_time = time.time()
print("=====================================================")
print("Data transfer complete!")
print(f"Total execution time: {total_end_time - total_start_time:.2f} seconds.")
print(f"Your data is now in the file: '{DATABASE_FILE}'")
print("=====================================================")