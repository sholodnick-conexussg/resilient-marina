
-- ============================================================================
-- Merge STG_MOLO_CURRENCIES to DW_MOLO_CURRENCIES
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_MOLO_CURRENCIES
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_MOLO_CURRENCIES tgt
    USING STG_MOLO_CURRENCIES src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.NAME = src.NAME,
            tgt.CODE = src.CODE,
            tgt.SYMBOL = src.SYMBOL,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (
            ID, NAME, CODE, SYMBOL,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.NAME, src.CODE, src.SYMBOL,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_MOLO_CURRENCIES: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_MOLO_CURRENCIES: ' || SQLERRM);
        RAISE;
END SP_MERGE_MOLO_CURRENCIES;
/
