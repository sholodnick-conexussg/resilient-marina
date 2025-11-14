
-- ============================================================================
-- Merge STG_STELLAR_STYLE_PRICES to DW_STELLAR_STYLE_PRICES
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_STYLE_PRICES
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_STYLE_PRICES tgt
    USING STG_STELLAR_STYLE_PRICES src
    ON (tgt.TIME_ID = src.TIME_ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.DEFAULT_PRICE = src.DEFAULT_PRICE,
            tgt.HOLIDAY = src.HOLIDAY,
            tgt.SATURDAY = src.SATURDAY,
            tgt.SUNDAY = src.SUNDAY,
            tgt.MONDAY = src.MONDAY,
            tgt.TUESDAY = src.TUESDAY,
            tgt.WEDNESDAY = src.WEDNESDAY,
            tgt.THURSDAY = src.THURSDAY,
            tgt.FRIDAY = src.FRIDAY,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.DEFAULT_PRICE, -1) != NVL(src.DEFAULT_PRICE, -1)
            OR NVL(tgt.HOLIDAY, '~') != NVL(src.HOLIDAY, '~')
            OR NVL(tgt.SATURDAY, '~') != NVL(src.SATURDAY, '~')
            OR NVL(tgt.SUNDAY, '~') != NVL(src.SUNDAY, '~')
            OR NVL(tgt.MONDAY, '~') != NVL(src.MONDAY, '~')
            OR NVL(tgt.TUESDAY, '~') != NVL(src.TUESDAY, '~')
            OR NVL(tgt.WEDNESDAY, '~') != NVL(src.WEDNESDAY, '~')
            OR NVL(tgt.THURSDAY, '~') != NVL(src.THURSDAY, '~')
            OR NVL(tgt.FRIDAY, '~') != NVL(src.FRIDAY, '~')
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            TIME_ID, DEFAULT_PRICE, HOLIDAY, SATURDAY, SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.TIME_ID, src.DEFAULT_PRICE, src.HOLIDAY, src.SATURDAY, src.SUNDAY, src.MONDAY, src.TUESDAY, src.WEDNESDAY, src.THURSDAY, src.FRIDAY, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_STYLE_PRICES: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_STYLE_PRICES: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_STYLE_PRICES;
/
