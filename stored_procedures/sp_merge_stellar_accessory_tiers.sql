
-- ============================================================================
-- Merge STG_STELLAR_ACCESSORY_TIERS to DW_STELLAR_ACCESSORY_TIERS
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_ACCESSORY_TIERS
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_ACCESSORY_TIERS tgt
    USING STG_STELLAR_ACCESSORY_TIERS src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.ACCESSORY_ID = src.ACCESSORY_ID,
            tgt.MIN_HOURS = src.MIN_HOURS,
            tgt.MAX_HOURS = src.MAX_HOURS,
            tgt.PRICE = src.PRICE,
            tgt.ACCESSORY_OPTION_ID = src.ACCESSORY_OPTION_ID,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.ACCESSORY_ID, -1) != NVL(src.ACCESSORY_ID, -1)
            OR NVL(tgt.MIN_HOURS, -1) != NVL(src.MIN_HOURS, -1)
            OR NVL(tgt.MAX_HOURS, -1) != NVL(src.MAX_HOURS, -1)
            OR NVL(tgt.PRICE, -999999) != NVL(src.PRICE, -999999)
            OR NVL(tgt.ACCESSORY_OPTION_ID, -1) != NVL(src.ACCESSORY_OPTION_ID, -1)
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, ACCESSORY_ID, MIN_HOURS, MAX_HOURS, PRICE, ACCESSORY_OPTION_ID, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.ACCESSORY_ID, src.MIN_HOURS, src.MAX_HOURS, src.PRICE, src.ACCESSORY_OPTION_ID, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_ACCESSORY_TIERS: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_ACCESSORY_TIERS: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_ACCESSORY_TIERS;
/
