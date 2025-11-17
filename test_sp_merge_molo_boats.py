#!/usr/bin/env python3
"""
Test script for SP_MERGE_MOLO_BOATS stored procedure.
Verifies compilation and execution.
"""

import oracledb
import json

def main():
    # Load config
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    # Set wallet location
    wallet_location = './wallet_demo'
    
    try:
        oracledb.init_oracle_client(
            lib_dir="/opt/oracle/instantclient",
            config_dir=wallet_location
        )
    except Exception as e:
        print(f"Oracle client initialization: {e}")
    
    print("="*70)
    print("Testing SP_MERGE_MOLO_BOATS")
    print("="*70)
    
    # Connect
    connection = oracledb.connect(
        user=config['database']['user'],
        password=config['database']['password'],
        dsn=config['database']['dsn'],
        config_dir=wallet_location,
        wallet_location=wallet_location,
        wallet_password=''
    )
    
    cursor = connection.cursor()
    
    # Read and compile the procedure
    print("\n1. Compiling procedure...")
    with open('stored_procedures/sp_merge_molo_boats.sql', 'r') as f:
        sql = f.read()
    
    # Remove trailing /
    sql = sql.strip()
    if sql.endswith('/'):
        sql = sql[:-1].strip()
    
    try:
        cursor.execute(sql)
        print("   ✅ Procedure compiled successfully")
    except Exception as e:
        print(f"   ❌ Compilation error: {e}")
        cursor.close()
        connection.close()
        return
    
    # Check compilation status
    print("\n2. Checking compilation status...")
    cursor.execute("""
        SELECT object_name, status 
        FROM user_objects 
        WHERE object_name = 'SP_MERGE_MOLO_BOATS'
        AND object_type = 'PROCEDURE'
    """)
    result = cursor.fetchone()
    if result:
        print(f"   Procedure: {result[0]}")
        print(f"   Status: {result[1]}")
        
        if result[1] != 'VALID':
            print("\n   ❌ Compilation errors found:")
            cursor.execute("""
                SELECT line, position, text
                FROM user_errors
                WHERE name = 'SP_MERGE_MOLO_BOATS'
                AND type = 'PROCEDURE'
                ORDER BY line, position
            """)
            for error in cursor.fetchall():
                print(f"      Line {error[0]}: {error[2]}")
            cursor.close()
            connection.close()
            return
    else:
        print("   ❌ Procedure not found")
        cursor.close()
        connection.close()
        return
    
    # Check row counts before execution
    print("\n3. Checking table counts...")
    cursor.execute("SELECT COUNT(*) FROM STG_MOLO_BOATS")
    stg_count = cursor.fetchone()[0]
    print(f"   STG_MOLO_BOATS: {stg_count} rows")
    
    cursor.execute("SELECT COUNT(*) FROM DW_MOLO_BOATS")
    dw_count = cursor.fetchone()[0]
    print(f"   DW_MOLO_BOATS: {dw_count} rows")
    
    # Execute the procedure
    print("\n4. Executing procedure...")
    try:
        # Enable DBMS_OUTPUT
        cursor.callproc("dbms_output.enable")
        
        # Call the procedure
        cursor.callproc('SP_MERGE_MOLO_BOATS')
        
        # Get DBMS_OUTPUT
        print("   Procedure output:")
        status_var = cursor.var(int)
        line_var = cursor.var(str)
        while True:
            cursor.callproc("dbms_output.get_line", (line_var, status_var))
            if status_var.getvalue() != 0:
                break
            line = line_var.getvalue()
            if line:
                print(f"      📋 {line}")
        
        print("   ✅ Procedure executed successfully")
        
    except Exception as e:
        print(f"   ❌ Execution error: {e}")
        connection.rollback()
        cursor.close()
        connection.close()
        return
    
    # Check row counts after execution
    print("\n5. Verifying results...")
    cursor.execute("SELECT COUNT(*) FROM DW_MOLO_BOATS")
    dw_count_after = cursor.fetchone()[0]
    print(f"   DW_MOLO_BOATS after: {dw_count_after} rows")
    print(f"   Change: {dw_count_after - dw_count:+d} rows")
    
    # Check recent updates
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN DW_LAST_INSERTED >= SYSTIMESTAMP - INTERVAL '1' MINUTE THEN 1 ELSE 0 END) as recent_inserts,
            SUM(CASE WHEN DW_LAST_UPDATED >= SYSTIMESTAMP - INTERVAL '1' MINUTE 
                     AND DW_LAST_UPDATED > DW_LAST_INSERTED THEN 1 ELSE 0 END) as recent_updates
        FROM DW_MOLO_BOATS
    """)
    stats = cursor.fetchone()
    print(f"\n   Statistics:")
    print(f"      Total rows: {stats[0]}")
    print(f"      Recent inserts: {stats[1]}")
    print(f"      Recent updates: {stats[2]}")
    
    cursor.close()
    connection.close()
    
    print("\n" + "="*70)
    print("✅ Test completed successfully")
    print("="*70)

if __name__ == '__main__':
    main()
