
-- ============================================================================
-- Merge STG_STELLAR_POS_ITEMS to DW_STELLAR_POS_ITEMS
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_POS_ITEMS
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_POS_ITEMS tgt
    USING STG_STELLAR_POS_ITEMS src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.LOCATION_ID = src.LOCATION_ID,
            tgt.SKU = src.SKU,
            tgt.ITEM_NAME = src.ITEM_NAME,
            tgt.COST = src.COST,
            tgt.PRICE = src.PRICE,
            tgt.TAX_EXEMPT = src.TAX_EXEMPT,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.LOCATION_ID, -1) != NVL(src.LOCATION_ID, -1)
            OR NVL(tgt.SKU, '~') != NVL(src.SKU, '~')
            OR NVL(tgt.ITEM_NAME, '~') != NVL(src.ITEM_NAME, '~')
            OR NVL(tgt.COST, '~') != NVL(src.COST, '~')
            OR NVL(tgt.PRICE, -999999) != NVL(src.PRICE, -999999)
            OR NVL(tgt.TAX_EXEMPT, -1) != NVL(src.TAX_EXEMPT, -1)
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, LOCATION_ID, SKU, ITEM_NAME, COST, PRICE, TAX_EXEMPT, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.LOCATION_ID, src.SKU, src.ITEM_NAME, src.COST, src.PRICE, src.TAX_EXEMPT, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_POS_ITEMS: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_POS_ITEMS: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_POS_ITEMS;
/
