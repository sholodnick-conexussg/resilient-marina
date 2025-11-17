# Resilient Marina ETL Pipeline - Scripts Documentation

**Project:** Resilient Marina Data Warehouse ETL  
**Purpose:** Extract, transform, and load marina management data from two systems (MOLO & Stellar) into Oracle Autonomous Database  
**Author:** Stefan Holodnick  
**Date:** November 2025

---

## Table of Contents

- [Production Scripts](#production-scripts)
  - [Main ETL Services](#main-etl-services)
  - [Database Function Modules](#database-function-modules)
- [Development & Testing Scripts](#development--testing-scripts)
  - [Deployment Utilities](#deployment-utilities)
  - [Testing Scripts](#testing-scripts)
  - [Development Tools](#development-tools)
- [Script Usage Examples](#script-usage-examples)

---

## Production Scripts

### Main ETL Services

#### **download_csv_from_s3.py**
**Type:** Main Production Service (MOLO & Stellar Orchestrator)  
**Lines:** ~4,300  
**Purpose:** Primary ETL orchestration script that processes data from both MOLO marina management system and Stellar boat rental system.

**Key Features:**
- Downloads latest ZIP files from AWS S3 buckets
- Extracts and parses 47 CSV files for MOLO system
- Optionally processes Stellar data (calls download_stellar_from_s3.py)
- Inserts data into Oracle staging tables (STG_MOLO_*, STG_STELLAR_*)
- Executes merge stored procedures (STG → DW tables)
- Sends email notifications with processing results and ZIP attachments
- Comprehensive logging with table-level statistics

**Data Sources:**
- **MOLO Bucket:** `resilient-molo-backups` (47 CSV files in ZIP format)
- **Stellar Bucket:** `resilient-ims-backups` (29 tables in gzipped SQL dumps)

**Configuration:**
- Reads from `config.json` (AWS credentials, DB credentials, email settings)
- Oracle wallet location: `./wallet_demo/`
- Supports command-line flags: `--skip-molo`, `--skip-stellar`, `--skip-merges`

**CSV Files Processed (MOLO):**
```
Core Tables: MarinaLocations, Piers, SlipTypes, Slips, Reservations
Business: Companies, Contacts, Boats, Accounts
Financial: InvoiceSet, InvoiceItemSet, Transactions
Products: ItemMasters, SeasonalPrices, TransientPrices
Reference: BoatTypes, PowerNeeds, ReservationStatus, ContactTypes
Billing: InvoiceStatusSet, InvoiceTypeSet, TransactionTypeSet
... and 28 more lookup/reference tables
```

**Workflow:**
1. Load configuration from `config.json`
2. Connect to AWS S3 using boto3
3. Download latest MOLO ZIP file (most recent by timestamp)
4. Extract 47 CSV files from ZIP archive
5. Parse CSV data and insert into Oracle staging tables (STG_MOLO_*)
6. Optionally process Stellar data (if not skipped)
7. Execute merge procedures to update data warehouse tables (DW_*)
8. Send email notification with results and original ZIP files attached
9. Display comprehensive summary with per-table record counts

**Email Notification:**
- HTML-formatted report with color-coded status
- Plain text fallback for compatibility
- Attaches original ZIP files from S3
- Shows detailed statistics for both MOLO and Stellar systems
- Per-table record counts (e.g., "DW_MOLO_BOATS → 793 records")

**Usage:**
```bash
# Process both MOLO and Stellar (default)
python download_csv_from_s3.py

# Process MOLO only
python download_csv_from_s3.py --skip-stellar

# Process Stellar only
python download_csv_from_s3.py --skip-molo

# Load data only (skip merge procedures)
python download_csv_from_s3.py --skip-merges
```

**Dependencies:**
- boto3 (AWS S3 access)
- oracledb (Oracle database connectivity)
- molo_db_functions.py (MOLO insert operations)
- download_stellar_from_s3.py (Stellar processing)
- config.json (configuration)
- wallet_demo/ (Oracle Autonomous DB wallet)

**Logging:**
- Console output with color-coded emojis (✅, ❌, ⚠️)
- Per-table processing logs
- Comprehensive summary with total records and table counts
- Error tracking and reporting

---

#### **download_stellar_from_s3.py**
**Type:** Main Production Service (Stellar Data Processor)  
**Lines:** ~1,600  
**Purpose:** Downloads and processes Stellar Business boat rental system data from S3, parses SQL dumps, and inserts into Oracle staging tables.

**Key Features:**
- Downloads gzipped SQL dump files from S3 (format: `prod_resilient_YYYY-MM-DD_HH_MM-DATA.sql.gz`)
- Parses SQL INSERT statements to extract table data
- Processes all 29 Stellar Business tables
- Inserts into Oracle staging tables (STG_STELLAR_*)
- Returns detailed statistics per table

**Tables Processed (29 total):**
```
Core Reference (9 tables):
  - customers, locations, seasons, accessories, amenities
  - categories, club_tiers, coupons, holidays

Booking System (4 tables):
  - bookings, booking_boats, booking_payments, booking_accessories

Boat Inventory & Pricing (11 tables):
  - styles, style_boats, style_groups, style_times, style_prices
  - style_hourly_prices, style_photos, customer_boats, waitlists
  - season_dates, accessory_tiers

Point of Sale (5 tables):
  - pos_items, pos_sales, fuel_sales, closed_dates, blacklists
```

**Data Extraction Method:**
- Uses `extract_table_data_from_sql()` function to parse SQL dumps
- Handles INSERT INTO statements with VALUES clauses
- Supports multi-row INSERT statements
- Properly escapes SQL quotes and special characters

**Workflow:**
1. Connect to S3 bucket: `resilient-ims-backups`
2. Download latest gzipped SQL dump file (most recent by timestamp)
3. Decompress and parse SQL INSERT statements
4. Extract data for each of 29 tables
5. Insert into Oracle staging tables (STG_STELLAR_*)
6. Return dictionary with table names and record counts

**Return Value:**
```python
{
    'successful_tables': {
        'CUSTOMERS': 1250,
        'BOOKINGS': 3450,
        'STYLES': 23,
        ...
    },
    'failed_tables': [],
    'total_records': 15678,
    'total_tables': 29
}
```

**Usage:**
```python
# Called from download_csv_from_s3.py
from download_stellar_from_s3 import process_stellar_data_from_s3

stellar_results = process_stellar_data_from_s3(
    config=config,
    db_connector=db
)
```

**Dependencies:**
- boto3 (AWS S3 access)
- oracledb (Oracle database connectivity)
- stellar_db_functions.py (insert operations)
- gzip (decompression)

**Note:**
- Contains deprecated CSV parsing functions (no longer used)
- Modern approach uses SQL dump parsing via `extract_table_data_from_sql()`
- CSV functions kept for reference only

---

### Database Function Modules

#### **molo_db_functions.py**
**Type:** Database Library Module  
**Lines:** ~1,700  
**Purpose:** Oracle database connector and insert functions for all 47 MOLO marina management system tables.

**Key Features:**
- `OracleConnector` class for database connection management
- Oracle Autonomous Database wallet configuration
- Oracle Instant Client initialization
- 47 insert methods for MOLO staging tables

**OracleConnector Class:**
```python
class OracleConnector:
    def __init__(self, user, password, dsn):
        # Sets up wallet, initializes client, connects to DB
    
    def _setup_oracle_wallet(self):
        # Configures TNS_ADMIN environment variable
        # Points to ./wallet_demo/ directory
    
    def _initialize_oracle_client(self):
        # Tries common Instant Client paths
        # Supports Linux, Windows, Docker
```

**Insert Methods (47 total):**
```python
# Core entities
insert_marina_locations(data)
insert_companies(data)
insert_contacts(data)
insert_boats(data)
insert_accounts(data)

# Operations
insert_piers(data)
insert_slips(data)
insert_slip_types(data)
insert_reservations(data)

# Financial
insert_invoices(data)
insert_invoice_items(data)
insert_transactions(data)

# Products & Pricing
insert_item_masters(data)
insert_seasonal_prices(data)
insert_transient_prices(data)

# Reference/Lookup (32 tables)
insert_boat_types(data)
insert_power_needs(data)
insert_reservation_status(data)
insert_invoice_status(data)
insert_payment_methods(data)
... and 27 more
```

**Insert Pattern:**
```python
def insert_boats(self, data):
    """Insert boat records into STG_MOLO_BOATS."""
    insert_sql = """
    INSERT INTO STG_MOLO_BOATS (
        BOAT_ID, BOAT_NAME, LENGTH, WIDTH, BOAT_TYPE_ID,
        POWER_NEED_ID, INSURANCE_ID, ACCOUNT_ID, ...
    ) VALUES (
        :1, :2, :3, :4, :5, :6, :7, :8, ...
    )
    """
    self.cursor.executemany(insert_sql, data)
    self.connection.commit()
```

**Wallet Configuration:**
- Looks for `./wallet_demo/` directory
- Verifies required files: `cwallet.sso`, `tnsnames.ora`, `sqlnet.ora`
- Sets `TNS_ADMIN` environment variable

**Oracle Client Paths:**
```
Linux/Docker:  /opt/oracle/instantclient
Windows:       C:\oracle\instantclient_21_3
               C:\oracle\instantclient
```

**Usage:**
```python
from molo_db_functions import OracleConnector

# Connect to database
db = OracleConnector(
    user="API_USER",
    password="password",
    dsn="oax4504110443_low"
)

# Insert data
parsed_data = parse_boats_data(csv_content)
db.insert_boats(parsed_data)

# Close connection
db.connection.close()
```

**Dependencies:**
- oracledb (Oracle database driver)
- logging (structured logging)
- os (environment variables, file paths)

---

#### **stellar_db_functions.py**
**Type:** Database Library Module  
**Lines:** ~1,200  
**Purpose:** Oracle database connector and insert functions for all 29 Stellar Business system tables.

**Key Features:**
- Identical `OracleConnector` class structure as molo_db_functions.py
- 29 insert methods for Stellar staging tables
- Same wallet and client initialization logic

**Insert Methods (29 total):**
```python
# Core reference data
insert_customers(data)
insert_locations(data)
insert_seasons(data)
insert_accessories(data)
insert_amenities(data)
insert_categories(data)
insert_club_tiers(data)
insert_coupons(data)
insert_holidays(data)

# Booking system
insert_bookings(data)
insert_booking_boats(data)
insert_booking_payments(data)
insert_booking_accessories(data)

# Boat inventory & pricing
insert_styles(data)
insert_style_boats(data)
insert_style_groups(data)
insert_style_times(data)
insert_style_prices(data)
insert_style_hourly_prices(data)
insert_style_photos(data)
insert_customer_boats(data)
insert_waitlists(data)
insert_season_dates(data)
insert_accessory_tiers(data)
insert_accessory_options(data)

# Point of sale
insert_pos_items(data)
insert_pos_sales(data)
insert_fuel_sales(data)
insert_closed_dates(data)
insert_blacklists(data)
```

**Insert Pattern:**
```python
def insert_bookings(self, data):
    """Insert booking records into STG_STELLAR_BOOKINGS."""
    insert_sql = """
    INSERT INTO STG_STELLAR_BOOKINGS (
        BOOKING_ID, CUSTOMER_ID, LOCATION_ID, 
        START_DATE, END_DATE, TOTAL_PRICE, STATUS, ...
    ) VALUES (
        :1, :2, :3, :4, :5, :6, :7, ...
    )
    """
    self.cursor.executemany(insert_sql, data)
    self.connection.commit()
```

**Usage:**
```python
from stellar_db_functions import OracleConnector

# Connect to database
db = OracleConnector(
    user="API_USER",
    password="password",
    dsn="oax4504110443_low"
)

# Insert data
parsed_data = extract_customers_from_sql(sql_dump)
db.insert_customers(parsed_data)

# Close connection
db.connection.close()
```

**Dependencies:**
- oracledb (Oracle database driver)
- logging (structured logging)
- os (environment variables, file paths)

**Design Philosophy:**
- Mirrors molo_db_functions.py structure for consistency
- Separated from download_stellar_from_s3.py for modularity
- Single responsibility: database operations only

---

## Development & Testing Scripts

### Deployment Utilities

#### **deploy_updated_procedures.py**
**Type:** Deployment Utility  
**Lines:** ~150  
**Purpose:** Deploys updated stored procedures with conditional WHERE clause pattern to Oracle database.

**Key Features:**
- Deploys 20 updated merge procedures (9 MOLO + 11 Stellar)
- Reads SQL files from `stored_procedures/` directory
- Compiles procedures in Oracle database
- Verifies compilation status (VALID/INVALID)
- Reports errors if compilation fails

**Procedures Deployed:**
```
MOLO Procedures (9):
  - sp_merge_molo_accounts.sql
  - sp_merge_molo_boats.sql
  - sp_merge_molo_contacts.sql
  - sp_merge_molo_invoice_items.sql
  - sp_merge_molo_invoices.sql
  - sp_merge_molo_piers.sql
  - sp_merge_molo_reservations.sql
  - sp_merge_molo_slips.sql
  - sp_merge_molo_transactions.sql

Stellar Procedures (11):
  - sp_merge_stellar_booking_boats.sql
  - sp_merge_stellar_booking_payments.sql
  - sp_merge_stellar_bookings.sql
  - sp_merge_stellar_customers.sql
  - sp_merge_stellar_pos_items.sql
  - sp_merge_stellar_pos_sales.sql
  - sp_merge_stellar_style_boats.sql
  - sp_merge_stellar_style_hourly_prices.sql
  - sp_merge_stellar_style_prices.sql
  - sp_merge_stellar_style_times.sql
  - sp_merge_stellar_styles.sql
```

**Workflow:**
1. Load config from `config.json`
2. Initialize Oracle client and connect to database
3. For each procedure file:
   - Read SQL file content
   - Clean up SQL (remove trailing `/`)
   - Execute CREATE OR REPLACE PROCEDURE
   - Check compilation status
   - Report VALID/INVALID status
4. Display summary of deployed procedures

**Usage:**
```bash
python deploy_updated_procedures.py
```

**Output:**
```
======================================================================
Deploying Updated Stored Procedures
======================================================================

Deploying SP_MERGE_MOLO_ACCOUNTS...
✅ Procedure SP_MERGE_MOLO_ACCOUNTS is VALID

Deploying SP_MERGE_MOLO_BOATS...
✅ Procedure SP_MERGE_MOLO_BOATS is VALID

...

Summary:
Total procedures: 20
Deployed successfully: 20
Failed: 0
```

**Dependencies:**
- oracledb
- json (config loading)
- config.json
- stored_procedures/*.sql files

---

#### **deploy_procedures_simple.py**
**Type:** Simple Deployment Utility  
**Lines:** ~105  
**Purpose:** Executes consolidated deployment SQL file (`deploy_all_procedures.sql`) containing all stored procedures.

**Key Features:**
- Reads single consolidated SQL file
- Splits by `/` delimiter to separate procedures
- Executes each CREATE OR REPLACE statement
- Simpler alternative to deploy_updated_procedures.py

**Workflow:**
1. Load config from `config.json`
2. Connect to Oracle database
3. Read `stored_procedures/deploy_all_procedures.sql`
4. Split SQL by `/` delimiter
5. Execute each CREATE OR REPLACE PROCEDURE statement
6. Report success/failure for each

**Usage:**
```bash
python deploy_procedures_simple.py
```

**Input File:**
- `stored_procedures/deploy_all_procedures.sql` (generated file containing all 46 procedures)

**Note:**
- Simpler than deploy_updated_procedures.py
- No individual file reading
- Relies on consolidated SQL file
- Good for bulk deployment

**Dependencies:**
- oracledb
- json (config loading)
- config.json

---

#### **run_merges.py**
**Type:** Manual Execution Utility  
**Lines:** ~91  
**Purpose:** Manually executes the master merge procedure `SP_RUN_ALL_MOLO_STELLAR_MERGES` to process all staging tables into data warehouse tables.

**Key Features:**
- Calls master stored procedure that runs all 46 merge procedures
- Executes: STG_MOLO_* → DW_MOLO_* and STG_STELLAR_* → DW_STELLAR_*
- Used for manual merge execution (bypassing main ETL script)

**Master Procedure Called:**
```sql
SP_RUN_ALL_MOLO_STELLAR_MERGES(
    p_verbose => TRUE
)
```

**Workflow:**
1. Load database config from `config.json`
2. Initialize Oracle client
3. Connect to database
4. Execute `SP_RUN_ALL_MOLO_STELLAR_MERGES` stored procedure
5. Display DBMS_OUTPUT from procedure (table-by-table progress)
6. Report success/failure

**Usage:**
```bash
python run_merges.py
```

**Output:**
```
======================================================================
Execute MOLO & Stellar Merges: STG_* → DW_*
======================================================================

🔌 Connecting to oax4504110443_low...
✅ Connected successfully

⚙️  Executing SP_RUN_ALL_MOLO_STELLAR_MERGES...

📊 Merging STG_MOLO_ACCOUNTS → DW_MOLO_ACCOUNTS (234 rows)
📊 Merging STG_MOLO_BOATS → DW_MOLO_BOATS (793 rows)
...
✅ All merges completed successfully
```

**Use Case:**
- Testing merge procedures independently
- Re-running merges without re-loading data from S3
- Troubleshooting merge logic
- Manual data warehouse updates

**Dependencies:**
- oracledb
- json (config loading)
- config.json
- SP_RUN_ALL_MOLO_STELLAR_MERGES stored procedure must exist

---

### Testing Scripts

#### **test_sp_merge_molo_boats.py**
**Type:** Unit Test Script  
**Lines:** ~165  
**Purpose:** Tests the `SP_MERGE_MOLO_BOATS` stored procedure for compilation and execution.

**Test Coverage:**
1. **Compilation Test:** Verifies procedure compiles without errors
2. **Execution Test:** Runs procedure with sample data
3. **Status Check:** Queries USER_OBJECTS to check VALID/INVALID status
4. **Error Detection:** Checks USER_ERRORS for compilation errors

**Workflow:**
1. Load config from `config.json`
2. Connect to Oracle database
3. Read `stored_procedures/sp_merge_molo_boats.sql`
4. Compile procedure using CREATE OR REPLACE
5. Check compilation status in USER_OBJECTS
6. If errors, display from USER_ERRORS
7. Execute procedure with test data
8. Verify records merged successfully

**Usage:**
```bash
python test_sp_merge_molo_boats.py
```

**Output:**
```
======================================================================
Testing SP_MERGE_MOLO_BOATS
======================================================================

1. Compiling procedure...
✅ Procedure compiled

2. Checking compilation status...
Status: VALID

3. Testing execution...
✅ Procedure executed successfully
Merged 5 records

4. Verifying data...
✅ Data verified in DW_MOLO_BOATS
```

**Similar Test Scripts:**
- `test_sp_merge_slips.py` - Tests MOLO slips merge
- `test_invoice_item_types.py` - Tests invoice item types
- `test_deploy_connection.py` - Tests database connection

**Dependencies:**
- oracledb
- json (config loading)
- config.json
- stored_procedures/sp_merge_molo_boats.sql

---

### Development Tools

#### **generate_where_clause.py**
**Type:** Code Generation Tool  
**Lines:** ~261  
**Purpose:** Auto-generates type-safe WHERE clause conditions for MERGE statements by querying Oracle data dictionary.

**Key Features:**
- Queries `USER_TAB_COLUMNS` to get column data types
- Generates NVL-wrapped comparisons for nullable columns
- Handles different data types (NUMBER, VARCHAR2, DATE, TIMESTAMP, CLOB, BLOB)
- Creates proper NULL-safe comparison logic

**Data Type Handling:**
```sql
-- For VARCHAR2/CHAR columns:
NVL(tgt.COLUMN_NAME, '@#NULL#@') = NVL(src.COLUMN_NAME, '@#NULL#@')

-- For NUMBER columns:
NVL(tgt.COLUMN_NAME, -999999) = NVL(src.COLUMN_NAME, -999999)

-- For DATE/TIMESTAMP columns:
NVL(tgt.COLUMN_NAME, TO_DATE('1900-01-01','YYYY-MM-DD')) = 
NVL(src.COLUMN_NAME, TO_DATE('1900-01-01','YYYY-MM-DD'))

-- For CLOB/BLOB columns:
DBMS_LOB.COMPARE(tgt.COLUMN_NAME, src.COLUMN_NAME) = 0
```

**Workflow:**
1. Load config from `config.json`
2. Connect to Oracle database
3. Accept table name as input (e.g., "STG_MOLO_BOATS")
4. Query data dictionary for column definitions
5. Generate WHERE clause with proper NVL wrappers
6. Output formatted SQL to console or file

**Usage:**
```bash
python generate_where_clause.py STG_MOLO_BOATS
```

**Output Example:**
```sql
-- WHERE clause for STG_MOLO_BOATS
WHERE 
    NVL(tgt.BOAT_NAME, '@#NULL#@') <> NVL(src.BOAT_NAME, '@#NULL#@')
    OR NVL(tgt.LENGTH, -999999) <> NVL(src.LENGTH, -999999)
    OR NVL(tgt.WIDTH, -999999) <> NVL(src.WIDTH, -999999)
    OR NVL(tgt.BOAT_TYPE_ID, -999999) <> NVL(src.BOAT_TYPE_ID, -999999)
    OR NVL(tgt.REGISTRATION, '@#NULL#@') <> NVL(src.REGISTRATION, '@#NULL#@')
    ...
```

**Use Cases:**
- Generating WHERE clauses for new merge procedures
- Ensuring NULL-safe comparisons
- Standardizing merge logic across all procedures
- Reducing manual SQL coding errors

**Dependencies:**
- oracledb
- json (config loading)
- config.json
- Database access to query data dictionary

---

## Script Usage Examples

### **Daily Production Run**
```bash
# Full ETL pipeline - MOLO + Stellar + Merges + Email
python download_csv_from_s3.py
```

### **MOLO Only (Exclude Stellar)**
```bash
# Process only MOLO data (47 CSV files)
python download_csv_from_s3.py --skip-stellar
```

### **Stellar Only (Exclude MOLO)**
```bash
# Process only Stellar data (29 tables from SQL dumps)
python download_csv_from_s3.py --skip-molo
```

### **Load Data Without Merging**
```bash
# Load to staging tables but skip merge procedures
python download_csv_from_s3.py --skip-merges
```

### **Manual Merge Execution**
```bash
# Manually run merge procedures (after data is loaded)
python run_merges.py
```

### **Deploy Updated Procedures**
```bash
# Deploy 20 updated stored procedures with WHERE clause pattern
python deploy_updated_procedures.py
```

### **Generate WHERE Clause for New Table**
```bash
# Auto-generate NULL-safe WHERE clause for merge procedure
python generate_where_clause.py STG_MOLO_NEW_TABLE
```

### **Test Specific Procedure**
```bash
# Test boats merge procedure
python test_sp_merge_molo_boats.py

# Test slips merge procedure
python test_sp_merge_slips.py
```

---

## Configuration Management

### **config.json Structure**
```json
{
  "aws": {
    "access_key_id": "AKIA...",
    "secret_access_key": "...",
    "region": "us-east-1"
  },
  "database": {
    "user": "API_USER",
    "password": "...",
    "dsn": "oax4504110443_low"
  },
  "s3": {
    "molo_bucket": "resilient-molo-backups",
    "stellar_bucket": "resilient-ims-backups"
  },
  "email": {
    "enabled": true,
    "smtp_server": "smtp.office365.com",
    "smtp_port": 587,
    "from_email": "user@domain.com",
    "to_emails": ["user1@domain.com", "user2@domain.com"],
    "username": "user@domain.com",
    "password": "...",
    "subject_prefix": "[Resilient Marina ETL]"
  },
  "logging": {
    "level": "INFO"
  }
}
```

### **Environment Variables**
```bash
TNS_ADMIN=./wallet_demo  # Set automatically by scripts
```

---

## Dependencies Summary

### **Python Packages** (requirements.txt)
```
boto3>=1.26.0          # AWS S3 access
oracledb>=1.3.0        # Oracle database driver
pytest>=7.4.0          # Testing framework
moto[s3]>=4.1.0        # AWS mocking for tests
```

### **Oracle Components**
- Oracle Instant Client (19c or later)
- Oracle Autonomous Database wallet files
- Oracle stored procedures (46 merge procedures)

### **AWS Resources**
- S3 bucket: `resilient-molo-backups` (MOLO ZIP files)
- S3 bucket: `resilient-ims-backups` (Stellar SQL dumps)
- IAM credentials with S3 read access

---

## Logging & Monitoring

### **Log Formats**

**Console Output:**
```
2025-11-17 10:30:15 - INFO - Starting MOLO data processing...
2025-11-17 10:30:16 - INFO - ✅ Downloaded: molo_backup_2025-11-17_08-30-00.zip
2025-11-17 10:30:18 - INFO - ✅ Processed 793 boat records
2025-11-17 10:30:20 - INFO - ✅ DW_MOLO_BOATS                  →    793 records
```

**Email Report:**
```
Subject: [Resilient Marina ETL] Processing Complete - Nov 17, 2025

🏢 MOLO System:
   Total Records: 12,345
   Tables Processed: 47
   • BOATS: 793 records
   • SLIPS: 1,234 records
   • RESERVATIONS: 2,345 records

⭐ Stellar Business System:
   Total Records: 15,678
   Tables Processed: 29
   • CUSTOMERS: 1,250 records
   • BOOKINGS: 3,450 records
   • STYLES: 23 records
```

---

## Error Handling

### **Common Errors & Solutions**

**1. Oracle Wallet Not Found**
```
❌ Wallet directory not found: /path/to/wallet_demo
```
**Solution:** Ensure `wallet_demo/` directory exists with wallet files

**2. Oracle Client Not Found**
```
⚠️ Oracle Instant Client path not found
```
**Solution:** Install Oracle Instant Client or update path in scripts

**3. S3 Access Denied**
```
❌ Error accessing S3: Access Denied
```
**Solution:** Verify AWS credentials in `config.json` and IAM permissions

**4. Procedure Compilation Error**
```
❌ Procedure SP_MERGE_MOLO_BOATS is INVALID
```
**Solution:** Run `generate_where_clause.py` to verify column types and update procedure

**5. Email Send Failure**
```
❌ Failed to send email: Authentication failed
```
**Solution:** Verify SMTP credentials in `config.json` email section

---

## Performance Metrics

### **Typical Run Times**

| Operation | Records | Time |
|-----------|---------|------|
| MOLO Data Download | 1 ZIP file (~50 MB) | 5-10 seconds |
| MOLO CSV Parsing | 47 files | 15-20 seconds |
| MOLO Staging Insert | ~12,000 records | 10-15 seconds |
| Stellar Data Download | 1 SQL dump (~30 MB) | 3-5 seconds |
| Stellar Parsing | 29 tables | 20-30 seconds |
| Stellar Staging Insert | ~15,000 records | 15-20 seconds |
| All Merge Procedures | 76 tables | 45-60 seconds |
| Email Notification | 1 email + 2 attachments | 2-3 seconds |
| **Total End-to-End** | **~27,000 records** | **2-3 minutes** |

---

## Security Considerations

### **Sensitive Files (NEVER COMMIT)**
- `config.json` - Contains AWS keys, DB passwords, email credentials
- `wallet_demo/*.sso` - Oracle wallet files
- `wallet_demo/*.p12` - Private keys

### **Git Configuration**
```gitignore
# .gitignore
config.json
wallet_demo/*.sso
wallet_demo/*.p12
*.log
__pycache__/
*.pyc
```

### **File Permissions** (Linux/Mac)
```bash
chmod 600 config.json
chmod 700 wallet_demo/
chmod 600 wallet_demo/*
```

---

## Maintenance & Support

### **Regular Maintenance Tasks**
1. Monitor S3 bucket sizes (archive old files)
2. Review log files for errors
3. Verify stored procedure status monthly
4. Update Oracle wallet before expiration
5. Rotate AWS credentials annually
6. Test email notifications quarterly

### **Adding New Tables**

**For MOLO Tables:**
1. Add CSV filename to `TARGET_CSV_FILES` in `download_csv_from_s3.py`
2. Create `parse_X_data()` function
3. Add `insert_X()` method to `molo_db_functions.py`
4. Create staging table `STG_MOLO_X` in Oracle
5. Create data warehouse table `DW_MOLO_X` in Oracle
6. Generate WHERE clause using `generate_where_clause.py`
7. Create merge procedure `sp_merge_molo_x.sql`
8. Add to `SP_RUN_ALL_MOLO_STELLAR_MERGES` master procedure

**For Stellar Tables:**
1. Add table name to `STELLAR_TABLES` in `download_stellar_from_s3.py`
2. Create `insert_X()` method to `stellar_db_functions.py`
3. Create staging table `STG_STELLAR_X` in Oracle
4. Create data warehouse table `DW_STELLAR_X` in Oracle
5. Create merge procedure `sp_merge_stellar_x.sql`
6. Add to `SP_RUN_ALL_MOLO_STELLAR_MERGES` master procedure

---

## Contact & Support

**Project Owner:** Stefan Holodnick  
**Organization:** Conexus Strategic Group  
**Email:** sholodnick@conexussg.com  

**Repository:** `resilient-marina` (GitHub)  
**Branch:** `main`

---

## Document Version

**Version:** 1.0  
**Last Updated:** November 17, 2025  
**Maintained By:** Stefan Holodnick

---

**End of Documentation**
