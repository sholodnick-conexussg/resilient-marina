
-- ============================================================================
-- Merge STG_STELLAR_ACCESSORY_OPTIONS to DW_STELLAR_ACCESSORY_OPTIONS
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_ACCESSORY_OPTIONS
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_ACCESSORY_OPTIONS tgt
    USING STG_STELLAR_ACCESSORY_OPTIONS src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.ACCESSORY_ID = src.ACCESSORY_ID,
            tgt.VALUE_TEXT = src.VALUE_TEXT,
            tgt.USE_STRIPED_BACKGROUND = src.USE_STRIPED_BACKGROUND,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.ACCESSORY_ID, -1) != NVL(src.ACCESSORY_ID, -1)
            OR NVL(tgt.VALUE_TEXT, '~') != NVL(src.VALUE_TEXT, '~')
            OR NVL(tgt.USE_STRIPED_BACKGROUND, '~') != NVL(src.USE_STRIPED_BACKGROUND, '~')
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, ACCESSORY_ID, VALUE_TEXT, USE_STRIPED_BACKGROUND, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.ACCESSORY_ID, src.VALUE_TEXT, src.USE_STRIPED_BACKGROUND, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_ACCESSORY_OPTIONS: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_ACCESSORY_OPTIONS: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_ACCESSORY_OPTIONS;
/
