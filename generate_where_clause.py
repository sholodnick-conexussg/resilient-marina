"""
Generate WHERE clause for MERGE statements with proper NVL handling based on column types.
Queries Oracle data dictionary to get actual column data types and generates 
comparison logic that handles NULLs correctly.
"""

import oracledb
import json
import sys
import os


def load_config():
    """Load database configuration from config.json"""
    with open('config.json', 'r') as f:
        return json.load(f)


def setup_oracle_wallet():
    """Set up Oracle wallet environment for Autonomous Database."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    wallet_dir = os.path.join(script_dir, "wallet_demo")
    wallet_dir = os.path.abspath(wallet_dir)
    
    if os.path.exists(wallet_dir):
        os.environ['TNS_ADMIN'] = wallet_dir
        print(f"✅ TNS_ADMIN set to: {wallet_dir}")
    else:
        print(f"❌ Wallet directory not found: {wallet_dir}")
        sys.exit(1)


def initialize_oracle_client():
    """Initialize Oracle Instant Client with common installation paths."""
    try:
        client_paths = [
            "/opt/oracle/instantclient",
            r"C:\oracle\instantclient_21_3",
            r"C:\oracle\instantclient"
        ]
        
        for path in client_paths:
            if os.path.exists(path):
                oracledb.init_oracle_client(lib_dir=path)
                print(f"✅ Oracle Instant Client initialized from: {path}")
                return
        
        print("⚠️  Oracle Instant Client path not found, trying without lib_dir")
        oracledb.init_oracle_client()
    except Exception as e:
        print(f"⚠️  Oracle client already initialized or error: {e}")

def get_column_info(cursor, table_name):
    """
    Query Oracle data dictionary to get column names and types.
    Returns list of tuples: (column_name, data_type, data_length, data_precision, data_scale)
    """
    query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            DATA_LENGTH,
            DATA_PRECISION,
            DATA_SCALE
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = :table_name
        AND COLUMN_NAME NOT IN ('DW_LAST_INSERTED', 'DW_LAST_UPDATED')
        ORDER BY COLUMN_ID
    """
    
    cursor.execute(query, {'table_name': table_name})
    return cursor.fetchall()

def get_nvl_default_value(data_type, data_precision=None, data_scale=None):
    """
    Return appropriate NVL default value based on Oracle data type.
    """
    # String/Character types
    if data_type in ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR', 'CLOB', 'NCLOB'):
        return "'~NULL~'"
    
    # Number types
    elif data_type == 'NUMBER':
        # If it has decimal places, use -999.999
        if data_scale and data_scale > 0:
            return "-999.999"
        else:
            return "-999"
    
    # Integer types
    elif data_type in ('INTEGER', 'INT', 'SMALLINT'):
        return "-999"
    
    # Float/Double types
    elif data_type in ('FLOAT', 'DOUBLE PRECISION', 'REAL', 'BINARY_FLOAT', 'BINARY_DOUBLE'):
        return "-999.999"
    
    # Date types
    elif data_type == 'DATE':
        return "TO_DATE('1900-01-01','YYYY-MM-DD')"
    
    # Timestamp types
    elif data_type.startswith('TIMESTAMP'):
        return "TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')"
    
    # BLOB/RAW types - use special marker
    elif data_type in ('BLOB', 'RAW', 'LONG RAW'):
        return "HEXTORAW('00')"
    
    # Default fallback for unknown types
    else:
        print(f"  Warning: Unknown data type '{data_type}', defaulting to string comparison")
        return "'~NULL~'"

def generate_where_clause(columns_info, indent="        "):
    """
    Generate WHERE clause with NVL comparisons for all columns.
    """
    conditions = []
    
    for col_name, data_type, data_length, data_precision, data_scale in columns_info:
        nvl_default = get_nvl_default_value(data_type, data_precision, data_scale)
        condition = f"NVL(tgt.{col_name}, {nvl_default}) <> NVL(src.{col_name}, {nvl_default})"
        conditions.append(condition)
    
    # Join with OR and proper formatting
    where_clause = f"{indent}WHERE (\n"
    where_clause += f" OR\n".join([f"{indent}    {cond}" for cond in conditions])
    where_clause += f"\n{indent})"
    
    return where_clause

def generate_merge_where_clause(table_name, config):
    """
    Main function to generate WHERE clause for a specific table.
    """
    # Map staging table name to DW table name
    dw_table_name = table_name.replace('STG_', 'DW_')
    
    print(f"\n{'='*80}")
    print(f"Generating WHERE clause for: {table_name} -> {dw_table_name}")
    print(f"{'='*80}")
    
    # Connect to database
    connection = oracledb.connect(
        user=config['database']['user'],
        password=config['database']['password'],
        dsn=config['database']['dsn']
    )
    cursor = connection.cursor()
    
    try:
        # Get column information from DW table
        columns_info = get_column_info(cursor, dw_table_name)
        
        if not columns_info:
            print(f"ERROR: No columns found for table {dw_table_name}")
            print(f"Make sure the table exists and you have access to it.")
            return None
        
        print(f"\nFound {len(columns_info)} columns (excluding DW_LAST_INSERTED, DW_LAST_UPDATED)")
        print("\nColumn Types:")
        for col_name, data_type, data_length, data_precision, data_scale in columns_info:
            type_info = data_type
            if data_precision:
                type_info += f"({data_precision}"
                if data_scale:
                    type_info += f",{data_scale}"
                type_info += ")"
            elif data_length and data_type in ('VARCHAR2', 'CHAR'):
                type_info += f"({data_length})"
            print(f"  {col_name:40s} {type_info}")
        
        # Generate WHERE clause
        where_clause = generate_where_clause(columns_info)
        
        print("\n" + "="*80)
        print("Generated WHERE clause:")
        print("="*80)
        print(where_clause)
        print("="*80)
        
        return where_clause
        
    finally:
        cursor.close()
        connection.close()

def update_stored_procedure(proc_file_path, where_clause):
    """
    Update the stored procedure file with the new WHERE clause.
    Finds the WHEN MATCHED section and replaces/adds the WHERE clause.
    """
    with open(proc_file_path, 'r') as f:
        content = f.read()
    
    # Find the UPDATE SET section and add WHERE clause before WHEN NOT MATCHED
    # Look for pattern: "tgt.DW_LAST_UPDATED = SYSTIMESTAMP" followed by optional WHERE and then "WHEN NOT MATCHED"
    
    import re
    
    # Pattern to match the end of UPDATE SET (DW_LAST_UPDATED line) up to WHEN NOT MATCHED
    # This will capture any existing WHERE clause
    pattern = r'(tgt\.DW_LAST_UPDATED = SYSTIMESTAMP)\s*(WHERE\s*\([^)]+\))?\s*(WHEN NOT MATCHED)'
    
    # Replace with new WHERE clause
    replacement = r'\1\n' + where_clause + r'\n    \3'
    
    new_content = re.sub(pattern, replacement, content, flags=re.DOTALL)
    
    if new_content == content:
        print("\nWARNING: No changes made. Pattern not found or already up to date.")
        return False
    
    # Write updated content
    with open(proc_file_path, 'w') as f:
        f.write(new_content)
    
    print(f"\n✅ Successfully updated: {proc_file_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_where_clause.py <TABLE_NAME> [stored_procedure_file]")
        print("\nExamples:")
        print("  python generate_where_clause.py DW_MOLO_SLIPS")
        print("  python generate_where_clause.py DW_MOLO_SLIPS stored_procedures/sp_merge_molo_slips.sql")
        print("\nNote: Table name should be the DW table (DW_MOLO_* or DW_STELLAR_*)")
        sys.exit(1)
    
    # Set up Oracle wallet and client
    setup_oracle_wallet()
    initialize_oracle_client()
    
    table_name = sys.argv[1].upper()
    
    # Ensure table name starts with DW_
    if not table_name.startswith('DW_'):
        # Try to infer it
        if table_name.startswith('STG_'):
            table_name = table_name.replace('STG_', 'DW_')
        elif table_name.startswith('MOLO_') or table_name.startswith('STELLAR_'):
            table_name = 'DW_' + table_name
        else:
            table_name = 'DW_MOLO_' + table_name
        print(f"Using table name: {table_name}")
    
    # Map to staging table name for function
    stg_table_name = table_name.replace('DW_', 'STG_')
    
    # Load config
    config = load_config()
    
    # Generate WHERE clause
    where_clause = generate_merge_where_clause(stg_table_name, config)
    
    # If stored procedure file provided, update it
    if len(sys.argv) >= 3 and where_clause:
        proc_file = sys.argv[2]
        update_stored_procedure(proc_file, where_clause)
