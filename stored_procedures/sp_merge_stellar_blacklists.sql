
-- ============================================================================
-- Merge STG_STELLAR_BLACKLISTS to DW_STELLAR_BLACKLISTS
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_BLACKLISTS
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_BLACKLISTS tgt
    USING STG_STELLAR_BLACKLISTS src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.LOCATION_ID = src.LOCATION_ID,
            tgt.FIRST_NAME = src.FIRST_NAME,
            tgt.LAST_NAME = src.LAST_NAME,
            tgt.PHONE = src.PHONE,
            tgt.CELL = src.CELL,
            tgt.EMAIL = src.EMAIL,
            tgt.DL_NUMBER = src.DL_NUMBER,
            tgt.NOTES = src.NOTES,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.LOCATION_ID, -1) != NVL(src.LOCATION_ID, -1)
            OR NVL(tgt.FIRST_NAME, '~') != NVL(src.FIRST_NAME, '~')
            OR NVL(tgt.LAST_NAME, '~') != NVL(src.LAST_NAME, '~')
            OR NVL(tgt.PHONE, '~') != NVL(src.PHONE, '~')
            OR NVL(tgt.CELL, '~') != NVL(src.CELL, '~')
            OR NVL(tgt.EMAIL, '~') != NVL(src.EMAIL, '~')
            OR NVL(tgt.DL_NUMBER, -1) != NVL(src.DL_NUMBER, -1)
            OR NVL(tgt.NOTES, '~') != NVL(src.NOTES, '~')
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, LOCATION_ID, FIRST_NAME, LAST_NAME, PHONE, CELL, EMAIL, DL_NUMBER, NOTES, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.LOCATION_ID, src.FIRST_NAME, src.LAST_NAME, src.PHONE, src.CELL, src.EMAIL, src.DL_NUMBER, src.NOTES, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_BLACKLISTS: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_BLACKLISTS: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_BLACKLISTS;
/
