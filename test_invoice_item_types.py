#!/usr/bin/env python3
"""
Test script to check what CSV files exist in the latest S3 MOLO ZIP
"""
import json
import sys

# Add logging
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Import the download function
sys.path.insert(0, '/mnt/c/Users/StefanHolodnick/Documents/aws-retrieve-csv')
from download_csv_from_s3 import TARGET_CSV_FILES

# Load config
with open('/mnt/c/Users/StefanHolodnick/Documents/aws-retrieve-csv/config.json') as f:
    config = json.load(f)

logger.info("="*70)
logger.info("TARGET CSV FILES LIST:")
logger.info("="*70)

# Find InvoiceItemTypeSet in the list
for i, filename in enumerate(TARGET_CSV_FILES, 1):
    if 'Invoice' in filename and 'Item' in filename:
        logger.info(f"  {i:2d}. {filename} ✅ <-- InvoiceItem related")
    else:
        logger.info(f"  {i:2d}. {filename}")

logger.info("")
logger.info("InvoiceItemTypeSet is in the TARGET_CSV_FILES list: %s", 
           'InvoiceItemTypeSet' in TARGET_CSV_FILES)
