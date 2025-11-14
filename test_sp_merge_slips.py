"""
Test sp_merge_molo_slips.sql stored procedure.
Deploys the procedure and verifies it compiles without errors.
"""

import oracledb
import json
import os
import sys


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
    """Initialize Oracle Instant Client."""
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
        
        oracledb.init_oracle_client()
    except Exception as e:
        print(f"⚠️  Oracle client already initialized: {e}")


def load_config():
    """Load database configuration."""
    with open('config.json', 'r') as f:
        return json.load(f)


def deploy_stored_procedure(cursor, proc_file):
    """Deploy stored procedure from SQL file."""
    print(f"\n{'='*80}")
    print(f"Deploying: {proc_file}")
    print(f"{'='*80}")
    
    with open(proc_file, 'r') as f:
        sql = f.read()
    
    # Remove trailing slash if present
    sql = sql.strip()
    if sql.endswith('/'):
        sql = sql[:-1]
    
    try:
        cursor.execute(sql)
        print("✅ Stored procedure compiled successfully!")
        return True
    except Exception as e:
        print(f"❌ Error compiling stored procedure:")
        print(f"   {e}")
        return False


def check_procedure_status(cursor, proc_name):
    """Check if procedure exists and its status."""
    query = """
        SELECT OBJECT_NAME, STATUS, OBJECT_TYPE
        FROM USER_OBJECTS
        WHERE OBJECT_NAME = :proc_name
        AND OBJECT_TYPE = 'PROCEDURE'
    """
    
    cursor.execute(query, {'proc_name': proc_name})
    result = cursor.fetchone()
    
    if result:
        obj_name, status, obj_type = result
        print(f"\n📋 Procedure Status:")
        print(f"   Name: {obj_name}")
        print(f"   Type: {obj_type}")
        print(f"   Status: {status}")
        
        if status == 'VALID':
            print(f"   ✅ Procedure is VALID and ready to use")
            return True
        else:
            print(f"   ⚠️  Procedure has INVALID status")
            return False
    else:
        print(f"\n❌ Procedure {proc_name} not found in database")
        return False


def get_procedure_errors(cursor, proc_name):
    """Get compilation errors for a procedure."""
    query = """
        SELECT LINE, POSITION, TEXT
        FROM USER_ERRORS
        WHERE NAME = :proc_name
        AND TYPE = 'PROCEDURE'
        ORDER BY SEQUENCE
    """
    
    cursor.execute(query, {'proc_name': proc_name})
    errors = cursor.fetchall()
    
    if errors:
        print(f"\n❌ Compilation Errors for {proc_name}:")
        print("="*80)
        for line, position, text in errors:
            print(f"   Line {line}, Position {position}: {text}")
        print("="*80)
        return True
    return False


def test_procedure_syntax(cursor):
    """Test the procedure by checking if it can be described."""
    try:
        # Try to describe the procedure (validates it exists and is accessible)
        cursor.callproc("dbms_describe.describe_procedure", 
                       ["SP_MERGE_MOLO_SLIPS"])
        print("\n✅ Procedure signature is valid")
        return True
    except Exception as e:
        print(f"\n⚠️  Could not describe procedure: {e}")
        return False


def count_table_rows(cursor, table_name):
    """Count rows in a table."""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        print(f"⚠️  Could not count rows in {table_name}: {e}")
        return None


def main():
    """Main test function."""
    print("\n" + "="*80)
    print("Testing SP_MERGE_MOLO_SLIPS Stored Procedure")
    print("="*80)
    
    # Setup
    setup_oracle_wallet()
    initialize_oracle_client()
    config = load_config()
    
    # Connect to database
    print("\n🔌 Connecting to Oracle database...")
    connection = oracledb.connect(
        user=config['database']['user'],
        password=config['database']['password'],
        dsn=config['database']['dsn']
    )
    cursor = connection.cursor()
    print("✅ Connected successfully")
    
    try:
        # Deploy the procedure
        proc_file = 'stored_procedures/sp_merge_molo_slips.sql'
        success = deploy_stored_procedure(cursor, proc_file)
        
        if not success:
            # Check for compilation errors
            get_procedure_errors(cursor, 'SP_MERGE_MOLO_SLIPS')
            sys.exit(1)
        
        # Check procedure status
        check_procedure_status(cursor, 'SP_MERGE_MOLO_SLIPS')
        
        # Get any compilation errors even if it compiled
        has_errors = get_procedure_errors(cursor, 'SP_MERGE_MOLO_SLIPS')
        
        if not has_errors:
            # Check table row counts
            print("\n📊 Table Row Counts:")
            stg_count = count_table_rows(cursor, 'STG_MOLO_SLIPS')
            dw_count = count_table_rows(cursor, 'DW_MOLO_SLIPS')
            
            if stg_count is not None:
                print(f"   STG_MOLO_SLIPS: {stg_count:,} rows")
            if dw_count is not None:
                print(f"   DW_MOLO_SLIPS: {dw_count:,} rows")
            
            print("\n" + "="*80)
            print("✅ ALL TESTS PASSED - Stored procedure is ready to use!")
            print("="*80)
            print("\nYou can now call it with:")
            print("   EXECUTE SP_MERGE_MOLO_SLIPS;")
            print("\nOr from Python:")
            print("   cursor.callproc('SP_MERGE_MOLO_SLIPS')")
        else:
            print("\n" + "="*80)
            print("⚠️  Procedure compiled but has warnings/errors")
            print("="*80)
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cursor.close()
        connection.close()
        print("\n🔌 Database connection closed")


if __name__ == "__main__":
    main()
