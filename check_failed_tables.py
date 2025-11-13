#!/usr/bin/env python3
"""
Check which Stellar tables failed to import from the latest run.
Parses the log file to extract failed tables and categorizes the failure reasons.
"""

import re
from datetime import datetime

def parse_log_file(log_path='stellar_processing.log'):
    """Parse the log file and extract failed tables with reasons."""
    
    failed_tables = {}
    successful_tables = []
    
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find the most recent run (look for the last SUMMARY section)
        summary_sections = re.findall(
            r'STELLAR BUSINESS DATA PROCESSING - SUMMARY.*?={80}',
            content,
            re.DOTALL
        )
        
        if not summary_sections:
            print("No processing summary found in log file.")
            return failed_tables, successful_tables
        
        # Get the most recent summary
        latest_summary = summary_sections[-1]
        
        # Extract successful count
        success_match = re.search(r'Successfully processed: (\d+)/(\d+) tables', latest_summary)
        if success_match:
            successful_count = int(success_match.group(1))
            total_count = int(success_match.group(2))
            print(f"\n{'='*70}")
            print(f"STELLAR TABLE IMPORT STATUS")
            print(f"{'='*70}")
            print(f"✅ Successful: {successful_count}/{total_count} tables")
            print(f"❌ Failed: {total_count - successful_count}/{total_count} tables")
        
        # Extract failed tables list
        failed_match = re.search(r'Failed tables: (.+)', latest_summary)
        if failed_match:
            failed_list = [t.strip() for t in failed_match.group(1).split(',')]
            
            # Now find the reason for each failure by searching backwards from summary
            run_start = content.rfind('STELLAR BUSINESS DATA PROCESSING - START', 0, content.rfind(latest_summary))
            run_content = content[run_start:content.rfind(latest_summary)]
            
            for table in failed_list:
                # Look for error messages for this table
                table_pattern = rf'Processing table: {table.upper()}.*?(?=Processing table:|STELLAR BUSINESS DATA PROCESSING - SUMMARY)'
                table_section = re.search(table_pattern, run_content, re.DOTALL | re.IGNORECASE)
                
                if table_section:
                    section_text = table_section.group(0)
                    
                    # Categorize the error
                    if 'ORA-01843' in section_text:
                        failed_tables[table] = 'Date format error (ORA-01843: not a valid month)'
                    elif 'ORA-01400' in section_text:
                        null_col = re.search(r'ORA-01400.*?"([^"]+)"', section_text)
                        if null_col:
                            failed_tables[table] = f'NULL constraint violation: {null_col.group(1)}'
                        else:
                            failed_tables[table] = 'NULL constraint violation'
                    elif 'ORA-00932' in section_text:
                        failed_tables[table] = 'Data type mismatch (NCLOB vs TIMESTAMP)'
                    elif 'ORA-01036' in section_text:
                        failed_tables[table] = 'Bind variable count mismatch'
                    elif 'No data rows parsed' in section_text:
                        failed_tables[table] = 'Empty dataset (no data in CSV)'
                    elif 'File not found' in section_text:
                        failed_tables[table] = 'CSV file not found in tarball'
                    else:
                        failed_tables[table] = 'Unknown error (check log for details)'
                else:
                    failed_tables[table] = 'No error details found'
        
        # Extract successful tables
        success_pattern = r'✅ Successfully processed (\w+):'
        successful_tables = re.findall(success_pattern, run_content, re.IGNORECASE)
        
    except FileNotFoundError:
        print(f"Log file not found: {log_path}")
        print("Run the Stellar data processing first to generate the log.")
        return {}, []
    except Exception as e:
        print(f"Error parsing log file: {e}")
        return {}, []
    
    return failed_tables, successful_tables


def print_failed_tables(failed_tables):
    """Print failed tables in a formatted list."""
    
    if not failed_tables:
        print("\n🎉 All tables imported successfully!")
        return
    
    print(f"\n{'='*70}")
    print("FAILED TABLES DETAILS")
    print(f"{'='*70}\n")
    
    # Categorize by error type
    categories = {
        'Data Quality Issues': [],
        'Schema Constraints': [],
        'Empty Datasets': [],
        'Other Errors': []
    }
    
    for table, reason in failed_tables.items():
        if 'Date format error' in reason:
            categories['Data Quality Issues'].append((table, reason))
        elif 'NULL constraint' in reason or 'Data type mismatch' in reason or 'Bind variable' in reason:
            categories['Schema Constraints'].append((table, reason))
        elif 'Empty dataset' in reason or 'no data' in reason.lower():
            categories['Empty Datasets'].append((table, reason))
        else:
            categories['Other Errors'].append((table, reason))
    
    for category, tables_list in categories.items():
        if tables_list:
            print(f"\n{category}:")
            print("-" * 70)
            for table, reason in tables_list:
                print(f"  • {table:25} → {reason}")
    
    print(f"\n{'='*70}")


def print_successful_tables(successful_tables):
    """Print successful tables."""
    
    if not successful_tables:
        return
    
    print(f"\n{'='*70}")
    print("SUCCESSFULLY IMPORTED TABLES")
    print(f"{'='*70}")
    print(f"\n{', '.join(successful_tables)}")
    print(f"\n{'='*70}")


if __name__ == '__main__':
    failed, successful = parse_log_file()
    print_failed_tables(failed)
    print_successful_tables(successful)
    
    # Summary
    print(f"\n📊 SUMMARY:")
    print(f"   • {len(successful)} tables succeeded")
    print(f"   • {len(failed)} tables failed")
    print(f"   • Total: {len(successful) + len(failed)} tables processed\n")
