"""
Script to add WHERE clauses to all MERGE stored procedures to prevent unnecessary updates.

This script reads existing merge stored procedures and adds WHERE conditions to only
update records when data has actually changed. This prevents:
1. Unnecessary UPDATE operations when data is identical
2. Spurious DW_LAST_UPDATED timestamp changes
3. Reduced write load on the database
"""

import re
import os
from pathlib import Path

def generate_where_clause_for_columns(columns):
    """
    Generate WHERE clause conditions for all columns in the UPDATE.
    
    Args:
        columns: List of column names from the UPDATE SET clause
        
    Returns:
        String containing the WHERE clause with NVL comparisons
    """
    where_conditions = []
    
    for col in columns:
        col_name = col.strip()
        if col_name in ['DW_LAST_UPDATED', 'DW_LAST_INSERTED']:
            continue  # Skip audit columns
            
        # Determine appropriate NULL substitute based on column name patterns
        if any(x in col_name.upper() for x in ['DATE', 'TIME', 'TIMESTAMP']):
            # Timestamp columns
            null_value = "TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')"
            condition = f"NVL(tgt.{col_name}, {null_value}) != NVL(src.{col_name}, {null_value})"
        elif any(x in col_name.upper() for x in ['_ID', 'COUNT', 'NUMBER', 'AMOUNT', 'QUANTITY', 'PRICE', 'FEE', 'TAX', 'RATE', 'MINUTES', 'HOURS', 'DAYS', 'CAP']):
            # Numeric/ID columns
            if 'FLOAT' in col_name.upper() or 'DECIMAL' in col_name.upper() or col_name.upper() in ['TAX', 'PRICE', 'FEE', 'AMOUNT']:
                condition = f"NVL(tgt.{col_name}, -999999) != NVL(src.{col_name}, -999999)"
            else:
                condition = f"NVL(tgt.{col_name}, -1) != NVL(src.{col_name}, -1)"
        else:
            # String columns
            condition = f"NVL(tgt.{col_name}, '~') != NVL(src.{col_name}, '~')"
        
        where_conditions.append(condition)
    
    return where_conditions

def add_where_clause_to_merge(sql_content):
    """
    Add WHERE clause to MERGE statement's UPDATE section.
    
    Args:
        sql_content: String containing the SQL stored procedure
        
    Returns:
        Modified SQL with WHERE clause added
    """
    # Find the UPDATE SET section
    update_pattern = r'(WHEN MATCHED THEN\s+UPDATE SET\s+)(.*?)(WHEN NOT MATCHED)'
    
    match = re.search(update_pattern, sql_content, re.DOTALL | re.IGNORECASE)
    if not match:
        print("  WARNING: Could not find UPDATE SET pattern")
        return sql_content
    
    update_section = match.group(2)
    
    # Extract column assignments (handle multi-line)
    column_assignments = []
    for line in update_section.split('\n'):
        line = line.strip()
        if line.startswith('tgt.') and '=' in line:
            # Extract column name before the =
            col_match = re.match(r'tgt\.(\w+)\s*=', line)
            if col_match:
                column_assignments.append(col_match.group(1))
    
    if not column_assignments:
        print("  WARNING: No column assignments found")
        return sql_content
    
    # Check if WHERE clause already exists
    if re.search(r'tgt\.DW_LAST_UPDATED\s*=\s*SYSTIMESTAMP\s+WHERE\s+', sql_content, re.IGNORECASE):
        print("  INFO: WHERE clause already exists, skipping")
        return sql_content
    
    # Generate WHERE conditions
    where_conditions = generate_where_clause_for_columns(column_assignments)
    
    if not where_conditions:
        print("  WARNING: No WHERE conditions generated")
        return sql_content
    
    # Build the WHERE clause
    where_clause = "\n        WHERE \n            -- Only update if data has actually changed\n            "
    where_clause += "\n            OR ".join(where_conditions)
    
    # Find where to insert the WHERE clause (after DW_LAST_UPDATED = SYSTIMESTAMP)
    # Replace the line ending after DW_LAST_UPDATED
    modified_content = re.sub(
        r'(tgt\.DW_LAST_UPDATED\s*=\s*SYSTIMESTAMP)\s*\n',
        r'\1' + where_clause + '\n',
        sql_content,
        count=1
    )
    
    return modified_content

def process_stored_procedure_file(filepath):
    """
    Process a single stored procedure file.
    
    Args:
        filepath: Path to the SQL file
        
    Returns:
        True if file was modified, False otherwise
    """
    print(f"\nProcessing: {filepath.name}")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        # Check if this is a MERGE procedure
        if 'MERGE INTO' not in original_content.upper():
            print("  SKIP: Not a MERGE procedure")
            return False
        
        # Check if WHERE clause already exists
        if re.search(r'DW_LAST_UPDATED\s*=\s*SYSTIMESTAMP\s+WHERE\s+', original_content, re.IGNORECASE):
            print("  SKIP: WHERE clause already exists")
            return False
        
        modified_content = add_where_clause_to_merge(original_content)
        
        if modified_content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(modified_content)
            print("  ✅ UPDATED: Added WHERE clause")
            return True
        else:
            print("  SKIP: No changes made")
            return False
            
    except Exception as e:
        print(f"  ERROR: {str(e)}")
        return False

def main():
    """Main execution function."""
    script_dir = Path(__file__).parent
    procedures_dir = script_dir / 'stored_procedures'
    
    if not procedures_dir.exists():
        print(f"ERROR: Directory not found: {procedures_dir}")
        return
    
    print("="*80)
    print("UPDATING MERGE STORED PROCEDURES WITH WHERE CLAUSES")
    print("="*80)
    
    # Get all SQL files
    sql_files = list(procedures_dir.glob('sp_merge_*.sql'))
    
    print(f"\nFound {len(sql_files)} merge procedure files")
    
    updated_count = 0
    for sql_file in sorted(sql_files):
        if process_stored_procedure_file(sql_file):
            updated_count += 1
    
    print("\n" + "="*80)
    print(f"COMPLETE: Updated {updated_count} out of {len(sql_files)} files")
    print("="*80)

if __name__ == '__main__':
    main()
