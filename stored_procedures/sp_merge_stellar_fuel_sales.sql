
-- ============================================================================
-- Merge STG_STELLAR_FUEL_SALES to DW_STELLAR_FUEL_SALES
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_FUEL_SALES
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_FUEL_SALES tgt
    USING STG_STELLAR_FUEL_SALES src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.LOCATION_ID = src.LOCATION_ID,
            tgt.ADMIN_ID = src.ADMIN_ID,
            tgt.CUSTOMER_NAME = src.CUSTOMER_NAME,
            tgt.FUEL_TYPE = src.FUEL_TYPE,
            tgt.QTY = src.QTY,
            tgt.PRICE = src.PRICE,
            tgt.SUB_TOTAL = src.SUB_TOTAL,
            tgt.TIP = src.TIP,
            tgt.GRAND_TOTAL = src.GRAND_TOTAL,
            tgt.AMOUNT_PAID = src.AMOUNT_PAID,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DELETED_AT = src.DELETED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.LOCATION_ID, -1) != NVL(src.LOCATION_ID, -1)
            OR NVL(tgt.ADMIN_ID, -1) != NVL(src.ADMIN_ID, -1)
            OR NVL(tgt.CUSTOMER_NAME, '~') != NVL(src.CUSTOMER_NAME, '~')
            OR NVL(tgt.FUEL_TYPE, '~') != NVL(src.FUEL_TYPE, '~')
            OR NVL(tgt.QTY, '~') != NVL(src.QTY, '~')
            OR NVL(tgt.PRICE, -999999) != NVL(src.PRICE, -999999)
            OR NVL(tgt.SUB_TOTAL, '~') != NVL(src.SUB_TOTAL, '~')
            OR NVL(tgt.TIP, '~') != NVL(src.TIP, '~')
            OR NVL(tgt.GRAND_TOTAL, '~') != NVL(src.GRAND_TOTAL, '~')
            OR NVL(tgt.AMOUNT_PAID, -1) != NVL(src.AMOUNT_PAID, -1)
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.DELETED_AT, '~') != NVL(src.DELETED_AT, '~')
    WHEN NOT MATCHED THEN
        INSERT (
            ID, LOCATION_ID, ADMIN_ID, CUSTOMER_NAME, FUEL_TYPE, QTY, PRICE, SUB_TOTAL, TIP, GRAND_TOTAL, AMOUNT_PAID, CREATED_AT, UPDATED_AT, DELETED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.LOCATION_ID, src.ADMIN_ID, src.CUSTOMER_NAME, src.FUEL_TYPE, src.QTY, src.PRICE, src.SUB_TOTAL, src.TIP, src.GRAND_TOTAL, src.AMOUNT_PAID, src.CREATED_AT, src.UPDATED_AT, src.DELETED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_FUEL_SALES: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_FUEL_SALES: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_FUEL_SALES;
/
