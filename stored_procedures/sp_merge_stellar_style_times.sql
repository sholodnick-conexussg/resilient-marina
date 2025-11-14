
-- ============================================================================
-- Merge STG_STELLAR_STYLE_TIMES to DW_STELLAR_STYLE_TIMES
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_STYLE_TIMES
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_STYLE_TIMES tgt
    USING STG_STELLAR_STYLE_TIMES src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.STYLE_ID = src.STYLE_ID,
            tgt.SEASON_ID = src.SEASON_ID,
            tgt.DESCRIPTION_TEXT = src.DESCRIPTION_TEXT,
            tgt.FRONTEND_DISPLAY = src.FRONTEND_DISPLAY,
            tgt.START_1 = src.START_1,
            tgt.END_1 = src.END_1,
            tgt.END_DAYS_1 = src.END_DAYS_1,
            tgt.STATUS_1 = src.STATUS_1,
            tgt.START_2 = src.START_2,
            tgt.END_2 = src.END_2,
            tgt.END_DAYS_2 = src.END_DAYS_2,
            tgt.STATUS_2 = src.STATUS_2,
            tgt.START_3 = src.START_3,
            tgt.END_3 = src.END_3,
            tgt.END_DAYS_3 = src.END_DAYS_3,
            tgt.STATUS_3 = src.STATUS_3,
            tgt.START_4 = src.START_4,
            tgt.END_4 = src.END_4,
            tgt.END_DAYS_4 = src.END_DAYS_4,
            tgt.STATUS_4 = src.STATUS_4,
            tgt.VALID_DAYS = src.VALID_DAYS,
            tgt.HOLIDAYS_ONLY_IF_VALID_DAY = src.HOLIDAYS_ONLY_IF_VALID_DAY,
            tgt.MAPPED_TIME_ID = src.MAPPED_TIME_ID,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.STYLE_ID, -1) != NVL(src.STYLE_ID, -1)
            OR NVL(tgt.SEASON_ID, -1) != NVL(src.SEASON_ID, -1)
            OR NVL(tgt.DESCRIPTION_TEXT, '~') != NVL(src.DESCRIPTION_TEXT, '~')
            OR NVL(tgt.FRONTEND_DISPLAY, '~') != NVL(src.FRONTEND_DISPLAY, '~')
            OR NVL(tgt.START_1, '~') != NVL(src.START_1, '~')
            OR NVL(tgt.END_1, '~') != NVL(src.END_1, '~')
            OR NVL(tgt.END_DAYS_1, -1) != NVL(src.END_DAYS_1, -1)
            OR NVL(tgt.STATUS_1, '~') != NVL(src.STATUS_1, '~')
            OR NVL(tgt.START_2, '~') != NVL(src.START_2, '~')
            OR NVL(tgt.END_2, '~') != NVL(src.END_2, '~')
            OR NVL(tgt.END_DAYS_2, -1) != NVL(src.END_DAYS_2, -1)
            OR NVL(tgt.STATUS_2, '~') != NVL(src.STATUS_2, '~')
            OR NVL(tgt.START_3, '~') != NVL(src.START_3, '~')
            OR NVL(tgt.END_3, '~') != NVL(src.END_3, '~')
            OR NVL(tgt.END_DAYS_3, -1) != NVL(src.END_DAYS_3, -1)
            OR NVL(tgt.STATUS_3, '~') != NVL(src.STATUS_3, '~')
            OR NVL(tgt.START_4, '~') != NVL(src.START_4, '~')
            OR NVL(tgt.END_4, '~') != NVL(src.END_4, '~')
            OR NVL(tgt.END_DAYS_4, -1) != NVL(src.END_DAYS_4, -1)
            OR NVL(tgt.STATUS_4, '~') != NVL(src.STATUS_4, '~')
            OR NVL(tgt.VALID_DAYS, -1) != NVL(src.VALID_DAYS, -1)
            OR NVL(tgt.HOLIDAYS_ONLY_IF_VALID_DAY, -1) != NVL(src.HOLIDAYS_ONLY_IF_VALID_DAY, -1)
            OR NVL(tgt.MAPPED_TIME_ID, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.MAPPED_TIME_ID, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, STYLE_ID, SEASON_ID, DESCRIPTION_TEXT, FRONTEND_DISPLAY, START_1, END_1, END_DAYS_1, STATUS_1, START_2, END_2, END_DAYS_2, STATUS_2, START_3, END_3, END_DAYS_3, STATUS_3, START_4, END_4, END_DAYS_4, STATUS_4, VALID_DAYS, HOLIDAYS_ONLY_IF_VALID_DAY, MAPPED_TIME_ID, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.STYLE_ID, src.SEASON_ID, src.DESCRIPTION_TEXT, src.FRONTEND_DISPLAY, src.START_1, src.END_1, src.END_DAYS_1, src.STATUS_1, src.START_2, src.END_2, src.END_DAYS_2, src.STATUS_2, src.START_3, src.END_3, src.END_DAYS_3, src.STATUS_3, src.START_4, src.END_4, src.END_DAYS_4, src.STATUS_4, src.VALID_DAYS, src.HOLIDAYS_ONLY_IF_VALID_DAY, src.MAPPED_TIME_ID, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_STYLE_TIMES: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_STYLE_TIMES: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_STYLE_TIMES;
/
