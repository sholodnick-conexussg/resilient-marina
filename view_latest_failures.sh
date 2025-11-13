#!/bin/bash
# View the most recent Stellar table failure report

LATEST_REPORT=$(ls -t stellar_failed_tables_*.txt 2>/dev/null | head -1)

if [ -z "$LATEST_REPORT" ]; then
    echo "No failure reports found."
    echo "Run the Stellar data processing to generate a report."
else
    echo "Displaying latest report: $LATEST_REPORT"
    echo ""
    cat "$LATEST_REPORT"
fi
