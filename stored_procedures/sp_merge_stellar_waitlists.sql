
-- ============================================================================
-- Merge STG_STELLAR_WAITLISTS to DW_STELLAR_WAITLISTS
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_WAITLISTS
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_WAITLISTS tgt
    USING STG_STELLAR_WAITLISTS src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.LOCATION_ID = src.LOCATION_ID,
            tgt.CATEGORY_ID = src.CATEGORY_ID,
            tgt.STYLE_ID = src.STYLE_ID,
            tgt.CUSTOMER_ID = src.CUSTOMER_ID,
            tgt.TIME_ID = src.TIME_ID,
            tgt.TIMEFRAME_ID = src.TIMEFRAME_ID,
            tgt.FIRST_NAME = src.FIRST_NAME,
            tgt.LAST_NAME = src.LAST_NAME,
            tgt.EMAIL = src.EMAIL,
            tgt.PHONE = src.PHONE,
            tgt.DEPARTURE_DATE = src.DEPARTURE_DATE,
            tgt.LENGTH_REQUESTED = src.LENGTH_REQUESTED,
            tgt.WAIT_LIST_TIME = src.WAIT_LIST_TIME,
            tgt.FULFILLED = src.FULFILLED,
            tgt.FULFILLED_DATE = src.FULFILLED_DATE,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.LOCATION_ID, -1) != NVL(src.LOCATION_ID, -1)
            OR NVL(tgt.CATEGORY_ID, -1) != NVL(src.CATEGORY_ID, -1)
            OR NVL(tgt.STYLE_ID, -1) != NVL(src.STYLE_ID, -1)
            OR NVL(tgt.CUSTOMER_ID, -1) != NVL(src.CUSTOMER_ID, -1)
            OR NVL(tgt.TIME_ID, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.TIME_ID, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.TIMEFRAME_ID, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.TIMEFRAME_ID, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.FIRST_NAME, '~') != NVL(src.FIRST_NAME, '~')
            OR NVL(tgt.LAST_NAME, '~') != NVL(src.LAST_NAME, '~')
            OR NVL(tgt.EMAIL, '~') != NVL(src.EMAIL, '~')
            OR NVL(tgt.PHONE, '~') != NVL(src.PHONE, '~')
            OR NVL(tgt.DEPARTURE_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.DEPARTURE_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.LENGTH_REQUESTED, '~') != NVL(src.LENGTH_REQUESTED, '~')
            OR NVL(tgt.WAIT_LIST_TIME, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.WAIT_LIST_TIME, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.FULFILLED, '~') != NVL(src.FULFILLED, '~')
            OR NVL(tgt.FULFILLED_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.FULFILLED_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, LOCATION_ID, CATEGORY_ID, STYLE_ID, CUSTOMER_ID, TIME_ID, TIMEFRAME_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, DEPARTURE_DATE, LENGTH_REQUESTED, WAIT_LIST_TIME, FULFILLED, FULFILLED_DATE, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.LOCATION_ID, src.CATEGORY_ID, src.STYLE_ID, src.CUSTOMER_ID, src.TIME_ID, src.TIMEFRAME_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.PHONE, src.DEPARTURE_DATE, src.LENGTH_REQUESTED, src.WAIT_LIST_TIME, src.FULFILLED, src.FULFILLED_DATE, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_WAITLISTS: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_WAITLISTS: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_WAITLISTS;
/
