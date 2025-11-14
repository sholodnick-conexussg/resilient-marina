
-- ============================================================================
-- Merge STG_STELLAR_SEASON_DATES to DW_STELLAR_SEASON_DATES
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_SEASON_DATES
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_SEASON_DATES tgt
    USING STG_STELLAR_SEASON_DATES src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.SEASON_ID = src.SEASON_ID,
            tgt.START_DATE = src.START_DATE,
            tgt.END_DATE = src.END_DATE,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.SEASON_ID, -1) != NVL(src.SEASON_ID, -1)
            OR NVL(tgt.START_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.START_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.END_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.END_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, SEASON_ID, START_DATE, END_DATE,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.SEASON_ID, src.START_DATE, src.END_DATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_SEASON_DATES: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_SEASON_DATES: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_SEASON_DATES;
/
