
-- ============================================================================
-- Merge STG_STELLAR_AMENITIES to DW_STELLAR_AMENITIES
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_AMENITIES
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_AMENITIES tgt
    USING STG_STELLAR_AMENITIES src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.LOCATION_ID = src.LOCATION_ID,
            tgt.AMENITY_NAME = src.AMENITY_NAME,
            tgt.FRONTEND_DISPLAY = src.FRONTEND_DISPLAY,
            tgt.FRONTEND_NAME = src.FRONTEND_NAME,
            tgt.FRONTEND_POSITION = src.FRONTEND_POSITION,
            tgt.FEATURED = src.FEATURED,
            tgt.FILTERABLE = src.FILTERABLE,
            tgt.ICON = src.ICON,
            tgt.AMENITY_TYPE = src.AMENITY_TYPE,
            tgt.OPTIONS_TEXT = src.OPTIONS_TEXT,
            tgt.PREFIX_TEXT = src.PREFIX_TEXT,
            tgt.SUFFIX_TEXT = src.SUFFIX_TEXT,
            tgt.DESCRIPTION_TEXT = src.DESCRIPTION_TEXT,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.LOCATION_ID, -1) != NVL(src.LOCATION_ID, -1)
            OR NVL(tgt.AMENITY_NAME, '~') != NVL(src.AMENITY_NAME, '~')
            OR NVL(tgt.FRONTEND_DISPLAY, '~') != NVL(src.FRONTEND_DISPLAY, '~')
            OR NVL(tgt.FRONTEND_NAME, '~') != NVL(src.FRONTEND_NAME, '~')
            OR NVL(tgt.FRONTEND_POSITION, '~') != NVL(src.FRONTEND_POSITION, '~')
            OR NVL(tgt.FEATURED, '~') != NVL(src.FEATURED, '~')
            OR NVL(tgt.FILTERABLE, '~') != NVL(src.FILTERABLE, '~')
            OR NVL(tgt.ICON, '~') != NVL(src.ICON, '~')
            OR NVL(tgt.AMENITY_TYPE, '~') != NVL(src.AMENITY_TYPE, '~')
            OR NVL(tgt.OPTIONS_TEXT, '~') != NVL(src.OPTIONS_TEXT, '~')
            OR NVL(tgt.PREFIX_TEXT, '~') != NVL(src.PREFIX_TEXT, '~')
            OR NVL(tgt.SUFFIX_TEXT, '~') != NVL(src.SUFFIX_TEXT, '~')
            OR NVL(tgt.DESCRIPTION_TEXT, '~') != NVL(src.DESCRIPTION_TEXT, '~')
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, LOCATION_ID, AMENITY_NAME, FRONTEND_DISPLAY, FRONTEND_NAME, FRONTEND_POSITION, FEATURED, FILTERABLE, ICON, AMENITY_TYPE, OPTIONS_TEXT, PREFIX_TEXT, SUFFIX_TEXT, DESCRIPTION_TEXT, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.LOCATION_ID, src.AMENITY_NAME, src.FRONTEND_DISPLAY, src.FRONTEND_NAME, src.FRONTEND_POSITION, src.FEATURED, src.FILTERABLE, src.ICON, src.AMENITY_TYPE, src.OPTIONS_TEXT, src.PREFIX_TEXT, src.SUFFIX_TEXT, src.DESCRIPTION_TEXT, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_AMENITIES: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_AMENITIES: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_AMENITIES;
/
