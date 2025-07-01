# --- START OF FILE: analyse_roe.py ---

import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

# --- 0. PRE-REQUISITE ---
# pip install openpyxl

warnings.filterwarnings("ignore", category=UserWarning)

# --- 1. CONFIGURATION ---
DATABASE_FILE = 'ferc_form1.db'
EXCEL_OUTPUT_FILE = 'roe_analysis_qc.xlsx'

# --- ROE Calculation Tables ---
# These tables are used for the main Return on Equity calculation.
INCOME_TABLE2 = 'f1_35'
BALANCE_SHEET_TABLE = 'f1_11'
RESPONDENT_TABLE = 'f1_1'

# --- Asset Calculation Tables & Accounts ---
# This is the detailed "Electric Plant In Service" schedule
ASSET_TABLE = 'f1_52'   # Page 204 of FERC Form 1 (Electric Plant in Service) 
ASSET_VALUE_COLUMN = 'yr_end_bal'
TOTAL_ELECDIST_ASSETS_ROW = 75
TOTAL_ELEC_ASSETS_ROW = 104



# --- Columns and Rows for Calculations ---
# ROE Columns
VALUE_COLUMN = 'cy_other_t'   # For income table (f1_35)
VALUE_COLUMN2 = 'end_qtr_ba'  # For balance sheet table (f1_11)
RESPONDENT_NAME_COLUMN = 'responden2'
# ROE Rows
NET_INCOME_ROW = 78
PROPRIETARY_CAPITAL_ROW = 16

# Create the database connection
db_uri = f'sqlite:///{DATABASE_FILE}'
engine = create_engine(db_uri)

# --- 2. CORE ROE CALCULATION QUERY ---
# This query calculates ROE based on end-of-period equity.
roe_query = f"""
WITH
IncomeData AS (
    SELECT respondent, year, CAST(REPLACE({VALUE_COLUMN}, ',', '') AS REAL) AS net_income
    FROM {INCOME_TABLE2} WHERE row_number = {NET_INCOME_ROW} AND report_prd = 12
),
EquityData AS (
    SELECT respondent, year, CAST(REPLACE({VALUE_COLUMN2}, ',', '') AS REAL) AS total_equity
    FROM {BALANCE_SHEET_TABLE} WHERE row_number = {PROPRIETARY_CAPITAL_ROW} AND report_prd = 12
),
RespondentNames AS (
    SELECT DISTINCT respondent, {RESPONDENT_NAME_COLUMN} AS respondent_name FROM {RESPONDENT_TABLE}
),
CombinedFinancials AS (
    SELECT
        e.respondent, r.respondent_name, e.year, i.net_income, e.total_equity
    FROM EquityData e
    JOIN IncomeData i ON e.respondent = i.respondent AND e.year = i.year
    JOIN RespondentNames r ON e.respondent = r.respondent
),
LaggedEquity AS (
    SELECT *, LAG(total_equity, 1, 0) OVER (PARTITION BY respondent ORDER BY year) AS prev_year_equity
    FROM CombinedFinancials
)
SELECT
    respondent, respondent_name, year, net_income, total_equity, prev_year_equity,
    CASE WHEN total_equity > 0 THEN (net_income / total_equity) * 100 ELSE NULL END AS return_on_equity_pct
FROM LaggedEquity
WHERE return_on_equity_pct IS NOT NULL
ORDER BY respondent_name, year;
"""

# --- 3. EXCEL EXPORT FUNCTION ---
def export_to_excel_for_qc(db_engine):
    """
    Runs several queries and exports the results to a multi-sheet Excel file
    for easy quality control and debugging. This version now includes distribution
    asset percentage calculation.
    """
    print(f"\n--- Generating QC Excel Report: {EXCEL_OUTPUT_FILE} ---")

    # --- Query for ROE Debugging (SQLite Compatible) ---
    raw_join_query = f"""
    WITH
    UnionedData AS (
        SELECT respondent, year, {VALUE_COLUMN} AS net_income_raw, NULL AS common_equity_raw
        FROM {INCOME_TABLE2} WHERE row_number = {NET_INCOME_ROW} AND report_prd = 12
        UNION ALL
        SELECT respondent, year, NULL AS net_income_raw, {VALUE_COLUMN2} AS common_equity_raw
        FROM {BALANCE_SHEET_TABLE} WHERE row_number = {PROPRIETARY_CAPITAL_ROW} AND report_prd = 12
    ),
    RespondentNames AS (
        SELECT DISTINCT respondent, {RESPONDENT_NAME_COLUMN} AS respondent_name FROM {RESPONDENT_TABLE}
    )
    SELECT
        u.respondent AS respondent_id, r.respondent_name, u.year,
        MAX(u.net_income_raw) AS net_income_raw,
        MAX(u.common_equity_raw) AS common_equity_raw
    FROM UnionedData u LEFT JOIN RespondentNames r ON u.respondent = r.respondent
    GROUP BY u.respondent, r.respondent_name, u.year
    ORDER BY r.respondent_name, u.year;
    """
    
    # --- Query for Distribution Asset Percentage ---
    dist_asset_query = f"""
    WITH
    DistributionWiresAssets AS (
        -- Get the single line iteam for Total Distribution Plant
        SELECT
            respondent,
            year,
            SUM(CAST(REPLACE({ASSET_VALUE_COLUMN}, ',', '') AS REAL)) as total_dist_wires_value
        FROM {ASSET_TABLE}
        WHERE row_number = {TOTAL_ELECDIST_ASSETS_ROW} 
        GROUP BY respondent, year
    ),
    TotalPlantAssets AS (
        -- Get the single line item for Total Electric Plant In Service
        SELECT
            respondent,
            year,
            SUM(CAST(REPLACE({ASSET_VALUE_COLUMN}, ',', '') AS REAL)) as total_plant_value
        FROM {ASSET_TABLE}
        WHERE row_number = {TOTAL_ELEC_ASSETS_ROW} 
        GROUP BY respondent, year
    ),
    RespondentNames AS (
        SELECT DISTINCT respondent, {RESPONDENT_NAME_COLUMN} AS respondent_name FROM {RESPONDENT_TABLE}
    )
    -- Join everything together and calculate the percentage
    SELECT
        tpa.respondent AS respondent_id,
        r.respondent_name,
        tpa.year,
        COALESCE(dwa.total_dist_wires_value, 0) AS total_dist_wires_value,
        tpa.total_plant_value,
        CASE
            WHEN tpa.total_plant_value > 0 THEN (COALESCE(dwa.total_dist_wires_value, 0) / tpa.total_plant_value) * 100
            ELSE 0
        END as distribution_asset_pct
    FROM TotalPlantAssets tpa
    LEFT JOIN DistributionWiresAssets dwa ON tpa.respondent = dwa.respondent AND tpa.year = dwa.year
    JOIN RespondentNames r ON tpa.respondent = r.respondent
    ORDER BY r.respondent_name, tpa.year;
    """

    try:
        # Step 1: Get the final, calculated ROE data.
        final_roe_df = pd.read_sql(roe_query, db_engine)
        print("  - Fetched final calculated ROE data.")

        # Step 2: Get the raw joined data for debugging.
        raw_joined_df = pd.read_sql(raw_join_query, db_engine)
        print("  - Fetched raw joined data for debugging.")
        
        # Step 3: Identify data quality issues from the raw join.
        dq_issues_df = raw_joined_df[raw_joined_df['net_income_raw'].isnull() | raw_joined_df['common_equity_raw'].isnull()]
        print("  - Identified data quality issues.")
        
        # Step 4: Get the distribution asset percentage data.
        dist_assets_df = pd.read_sql(dist_asset_query, db_engine)
        print("  - Calculated distribution asset percentages.")

        # Step 5: Write all DataFrames to a multi-sheet Excel file.
        with pd.ExcelWriter(EXCEL_OUTPUT_FILE, engine='openpyxl') as writer:
            final_roe_df.to_excel(writer, sheet_name='Final ROE Analysis', index=False)
            raw_joined_df.to_excel(writer, sheet_name='Raw Joined Data (for QC)', index=False)
            dq_issues_df.to_excel(writer, sheet_name='Data Quality Issues', index=False)
            dist_assets_df.to_excel(writer, sheet_name='Distribution Asset %', index=False)
        
        print(f"\nSuccess! Report saved to '{EXCEL_OUTPUT_FILE}'")
        print("You can now open this file to review all data, including the new 'Distribution Asset %' sheet.")

    except Exception as e:
        print(f"\n--- ERROR during Excel export: {e} ---")
        print("Please check your database connection, table names, and query logic.")

# --- 4. ANALYSIS FUNCTIONS ---
def get_roe_data(query, db_engine):
    print("\nExecuting ROE calculation query...")
    try:
        df = pd.read_sql(query, db_engine)
        if df.empty:
            print("Query ran successfully, but no ROE data was returned.")
        else:
            print(f"Successfully loaded {len(df)} records for ROE analysis.")
        return df
    except Exception as e:
        print(f"--- QUERY FAILED: {e} ---")
        return None

def show_top_performers(df, analysis_year, top_n=10):
    """Prints a table of the top N companies by ROE for a given year."""
    print(f"\n--- Top {top_n} Companies by ROE for {analysis_year} ---")
    year_df = df[df['year'] == analysis_year].copy()
    
    reasonable_roe_df = year_df[(year_df['return_on_equity_pct'] > 0) & (year_df['return_on_equity_pct'] < 50)]
    
    top_df = reasonable_roe_df.sort_values('return_on_equity_pct', ascending=False).head(top_n)

    if top_df.empty:
        print(f"No data available for {analysis_year} with ROE between 0% and 50%.")
        return

    top_df['return_on_equity_pct'] = top_df['return_on_equity_pct'].map('{:.2f}%'.format)
    print(top_df[['respondent_name', 'return_on_equity_pct']].to_string(index=False))

def plot_roe_distribution(df, title="ROE Distribution for All Years", filename="ROE_Distribution_Histogram.png"):
    """
    Analyzes the distribution of ROE for all company-years and plots a histogram.
    """
    print(f"\nGenerating ROE distribution histogram...")
    roe_values = df['return_on_equity_pct'].dropna()
    reasonable_min, reasonable_max = -50, 50
    filtered_roe = roe_values[(roe_values > reasonable_min) & (roe_values < reasonable_max)]

    if filtered_roe.empty:
        print(f"No ROE data found within the range {reasonable_min}% to {reasonable_max}%. Cannot generate plot.")
        return

    print(f"Plotting {len(filtered_roe)} of {len(roe_values)} total data points (filtered for range {reasonable_min}% to {reasonable_max}%).")
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.histplot(data=filtered_roe, ax=ax, kde=True, bins='auto')

    ax.set_title(title, fontsize=16)
    ax.set_xlabel('Return on Equity (ROE) %', fontsize=12)
    ax.set_ylabel('Frequency (Number of Company-Years)', fontsize=12)

    mean_roe = filtered_roe.mean()
    median_roe = filtered_roe.median()
    std_dev = filtered_roe.std()
    
    ax.axvline(mean_roe, color='red', linestyle='--', linewidth=1.5, label=f'Mean: {mean_roe:.2f}%')
    ax.legend()
    
    stats_text = (
        f"Median: {median_roe:.2f}%\n"
        f"Std. Dev: {std_dev:.2f}\n"
        f"Count: {len(filtered_roe):,}"
    )
    ax.text(0.95, 0.95, stats_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round,pad=0.5', fc='wheat', alpha=0.7))

    plt.tight_layout()
    plt.savefig(filename)
    print(f"Histogram saved as '{filename}'")
    plt.show()

# --- 5. MAIN EXECUTION ---

if __name__ == "__main__":
    # First, generate the comprehensive Excel report for quality control.
    export_to_excel_for_qc(engine)

    # Then, proceed with the Python-based analysis.
    roe_df = get_roe_data(roe_query, engine)

    if roe_df is not None and not roe_df.empty:
        print("\n--- Sample of Calculated ROE Data ---")
        print(roe_df.head())

        # Analyze the most recent year available in your data
        latest_year = int(roe_df['year'].max())
        show_top_performers(roe_df, analysis_year=latest_year, top_n=10)
        
        # Call the histogram function
        plot_roe_distribution(roe_df)

    elif roe_df is not None and roe_df.empty:
        print("\nQuery for ROE analysis ran successfully, but no data was returned.")
        print("Please check the 'Data Quality Issues' sheet in the generated Excel file.")

# --- END OF FILE ---