"""
Execute SP_MERGE_MOLO_SLIPS and display the insert/update counts.
"""

import oracledb
import json
import os
import sys


def setup_oracle_wallet():
    """Set up Oracle wallet environment."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    wallet_dir = os.path.join(script_dir, "wallet_demo")
    os.environ['TNS_ADMIN'] = os.path.abspath(wallet_dir)


def initialize_oracle_client():
    """Initialize Oracle Instant Client."""
    try:
        client_paths = ["/opt/oracle/instantclient", r"C:\oracle\instantclient_21_3"]
        for path in client_paths:
            if os.path.exists(path):
                oracledb.init_oracle_client(lib_dir=path)
                return
        oracledb.init_oracle_client()
    except Exception:
        pass


def main():
    setup_oracle_wallet()
    initialize_oracle_client()
    
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    print("\n🔌 Connecting to Oracle database...")
    connection = oracledb.connect(
        user=config['database']['user'],
        password=config['database']['password'],
        dsn=config['database']['dsn']
    )
    cursor = connection.cursor()
    
    try:
        # Enable DBMS_OUTPUT to capture procedure logging
        cursor.callproc("dbms_output.enable")
        
        print("🔄 Executing SP_MERGE_MOLO_SLIPS...")
        cursor.callproc('SP_MERGE_MOLO_SLIPS')
        
        # Fetch DBMS_OUTPUT
        line_var = cursor.var(str)
        status_var = cursor.var(int)
        
        print("\n📋 Procedure Output:")
        print("="*80)
        while True:
            cursor.callproc("dbms_output.get_line", (line_var, status_var))
            if status_var.getvalue() != 0:
                break
            output_line = line_var.getvalue()
            if output_line:
                print(f"   {output_line}")
        print("="*80)
        
        connection.commit()
        print("\n✅ Merge completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        connection.rollback()
        sys.exit(1)
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
