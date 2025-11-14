
-- ============================================================================
-- Merge STG_STELLAR_CUSTOMER_BOATS to DW_STELLAR_CUSTOMER_BOATS
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_CUSTOMER_BOATS
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_CUSTOMER_BOATS tgt
    USING STG_STELLAR_CUSTOMER_BOATS src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.CUSTOMER_ID = src.CUSTOMER_ID,
            tgt.SLIP_ID = src.SLIP_ID,
            tgt.BOAT_NAME = src.BOAT_NAME,
            tgt.BOAT_NUMBER = src.BOAT_NUMBER,
            tgt.LENGTH_FEET = src.LENGTH_FEET,
            tgt.WIDTH_FEET = src.WIDTH_FEET,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.CUSTOMER_ID, -1) != NVL(src.CUSTOMER_ID, -1)
            OR NVL(tgt.SLIP_ID, -1) != NVL(src.SLIP_ID, -1)
            OR NVL(tgt.BOAT_NAME, '~') != NVL(src.BOAT_NAME, '~')
            OR NVL(tgt.BOAT_NUMBER, -1) != NVL(src.BOAT_NUMBER, -1)
            OR NVL(tgt.LENGTH_FEET, -1) != NVL(src.LENGTH_FEET, -1)
            OR NVL(tgt.WIDTH_FEET, -1) != NVL(src.WIDTH_FEET, -1)
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, CUSTOMER_ID, SLIP_ID, BOAT_NAME, BOAT_NUMBER, LENGTH_FEET, WIDTH_FEET, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.CUSTOMER_ID, src.SLIP_ID, src.BOAT_NAME, src.BOAT_NUMBER, src.LENGTH_FEET, src.WIDTH_FEET, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_CUSTOMER_BOATS: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_CUSTOMER_BOATS: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_CUSTOMER_BOATS;
/
