
-- ============================================================================
-- Merge STG_STELLAR_STYLE_BOATS to DW_STELLAR_STYLE_BOATS
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_STYLE_BOATS
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_STELLAR_STYLE_BOATS tgt
    USING STG_STELLAR_STYLE_BOATS src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.STYLE_ID = src.STYLE_ID,
            tgt.BOAT_NUMBER = src.BOAT_NUMBER,
            tgt.PAPER_LESS_NUMBER = src.PAPER_LESS_NUMBER,
            tgt.MOTOR = src.MOTOR,
            tgt.MANUFACTURER = src.MANUFACTURER,
            tgt.SERIAL_NUMBER = src.SERIAL_NUMBER,
            tgt.IN_FLEET = src.IN_FLEET,
            tgt.HULL_NUMBER = src.HULL_NUMBER,
            tgt.STATE_NUMBER = src.STATE_NUMBER,
            tgt.CYLINDERS = src.CYLINDERS,
            tgt.HP = src.HP,
            tgt.MODEL = src.MODEL,
            tgt.BOAT_TYPE = src.BOAT_TYPE,
            tgt.PURCHASED_DATE = src.PURCHASED_DATE,
            tgt.PURCHASED_COST = src.PURCHASED_COST,
            tgt.SALE_DATE = src.SALE_DATE,
            tgt.SALE_PRICE = src.SALE_PRICE,
            tgt.CLUB_LOCATION = src.CLUB_LOCATION,
            tgt.DEALER_NAME = src.DEALER_NAME,
            tgt.DEALER_CITY = src.DEALER_CITY,
            tgt.DEALER_STATE = src.DEALER_STATE,
            tgt.PO_NUMBER = src.PO_NUMBER,
            tgt.BOAT_YEAR_MODEL = src.BOAT_YEAR_MODEL,
            tgt.MOTOR_YEAR_MODEL = src.MOTOR_YEAR_MODEL,
            tgt.MOTOR_MANUFACTURER_MODEL = src.MOTOR_MANUFACTURER_MODEL,
            tgt.STATE_REG_DATE = src.STATE_REG_DATE,
            tgt.STATE_REG_EXP_DATE = src.STATE_REG_EXP_DATE,
            tgt.ENGINE_PURCHASED_COST = src.ENGINE_PURCHASED_COST,
            tgt.BACKEND_DISPLAY = src.BACKEND_DISPLAY,
            tgt.POSITION_ORDER = src.POSITION_ORDER,
            tgt.STATUS_BOAT = src.STATUS_BOAT,
            tgt.SERVICE_START = src.SERVICE_START,
            tgt.SERVICE_END = src.SERVICE_END,
            tgt.CLEAN_STATUS = src.CLEAN_STATUS,
            tgt.INSURANCE_REG_NO = src.INSURANCE_REG_NO,
            tgt.BUOY_INSURANCE_STATUS = src.BUOY_INSURANCE_STATUS,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.STYLE_ID, -1) != NVL(src.STYLE_ID, -1)
            OR NVL(tgt.BOAT_NUMBER, -1) != NVL(src.BOAT_NUMBER, -1)
            OR NVL(tgt.PAPER_LESS_NUMBER, -1) != NVL(src.PAPER_LESS_NUMBER, -1)
            OR NVL(tgt.MOTOR, '~') != NVL(src.MOTOR, '~')
            OR NVL(tgt.MANUFACTURER, '~') != NVL(src.MANUFACTURER, '~')
            OR NVL(tgt.SERIAL_NUMBER, -1) != NVL(src.SERIAL_NUMBER, -1)
            OR NVL(tgt.IN_FLEET, '~') != NVL(src.IN_FLEET, '~')
            OR NVL(tgt.HULL_NUMBER, -1) != NVL(src.HULL_NUMBER, -1)
            OR NVL(tgt.STATE_NUMBER, -1) != NVL(src.STATE_NUMBER, -1)
            OR NVL(tgt.CYLINDERS, '~') != NVL(src.CYLINDERS, '~')
            OR NVL(tgt.HP, '~') != NVL(src.HP, '~')
            OR NVL(tgt.MODEL, '~') != NVL(src.MODEL, '~')
            OR NVL(tgt.BOAT_TYPE, '~') != NVL(src.BOAT_TYPE, '~')
            OR NVL(tgt.PURCHASED_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.PURCHASED_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.PURCHASED_COST, '~') != NVL(src.PURCHASED_COST, '~')
            OR NVL(tgt.SALE_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.SALE_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.SALE_PRICE, -1) != NVL(src.SALE_PRICE, -1)
            OR NVL(tgt.CLUB_LOCATION, '~') != NVL(src.CLUB_LOCATION, '~')
            OR NVL(tgt.DEALER_NAME, '~') != NVL(src.DEALER_NAME, '~')
            OR NVL(tgt.DEALER_CITY, '~') != NVL(src.DEALER_CITY, '~')
            OR NVL(tgt.DEALER_STATE, '~') != NVL(src.DEALER_STATE, '~')
            OR NVL(tgt.PO_NUMBER, -1) != NVL(src.PO_NUMBER, -1)
            OR NVL(tgt.BOAT_YEAR_MODEL, '~') != NVL(src.BOAT_YEAR_MODEL, '~')
            OR NVL(tgt.MOTOR_YEAR_MODEL, '~') != NVL(src.MOTOR_YEAR_MODEL, '~')
            OR NVL(tgt.MOTOR_MANUFACTURER_MODEL, '~') != NVL(src.MOTOR_MANUFACTURER_MODEL, '~')
            OR NVL(tgt.STATE_REG_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.STATE_REG_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.STATE_REG_EXP_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.STATE_REG_EXP_DATE, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
            OR NVL(tgt.ENGINE_PURCHASED_COST, '~') != NVL(src.ENGINE_PURCHASED_COST, '~')
            OR NVL(tgt.BACKEND_DISPLAY, '~') != NVL(src.BACKEND_DISPLAY, '~')
            OR NVL(tgt.POSITION_ORDER, '~') != NVL(src.POSITION_ORDER, '~')
            OR NVL(tgt.STATUS_BOAT, '~') != NVL(src.STATUS_BOAT, '~')
            OR NVL(tgt.SERVICE_START, '~') != NVL(src.SERVICE_START, '~')
            OR NVL(tgt.SERVICE_END, '~') != NVL(src.SERVICE_END, '~')
            OR NVL(tgt.CLEAN_STATUS, '~') != NVL(src.CLEAN_STATUS, '~')
            OR NVL(tgt.INSURANCE_REG_NO, '~') != NVL(src.INSURANCE_REG_NO, '~')
            OR NVL(tgt.BUOY_INSURANCE_STATUS, '~') != NVL(src.BUOY_INSURANCE_STATUS, '~')
            OR NVL(tgt.CREATED_AT, '~') != NVL(src.CREATED_AT, '~')
            OR NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD')) != NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD'))
    WHEN NOT MATCHED THEN
        INSERT (
            ID, STYLE_ID, BOAT_NUMBER, PAPER_LESS_NUMBER, MOTOR, MANUFACTURER, SERIAL_NUMBER, IN_FLEET, HULL_NUMBER, STATE_NUMBER, CYLINDERS, HP, MODEL, BOAT_TYPE, PURCHASED_DATE, PURCHASED_COST, SALE_DATE, SALE_PRICE, CLUB_LOCATION, DEALER_NAME, DEALER_CITY, DEALER_STATE, PO_NUMBER, BOAT_YEAR_MODEL, MOTOR_YEAR_MODEL, MOTOR_MANUFACTURER_MODEL, STATE_REG_DATE, STATE_REG_EXP_DATE, ENGINE_PURCHASED_COST, BACKEND_DISPLAY, POSITION_ORDER, STATUS_BOAT, SERVICE_START, SERVICE_END, CLEAN_STATUS, INSURANCE_REG_NO, BUOY_INSURANCE_STATUS, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.STYLE_ID, src.BOAT_NUMBER, src.PAPER_LESS_NUMBER, src.MOTOR, src.MANUFACTURER, src.SERIAL_NUMBER, src.IN_FLEET, src.HULL_NUMBER, src.STATE_NUMBER, src.CYLINDERS, src.HP, src.MODEL, src.BOAT_TYPE, src.PURCHASED_DATE, src.PURCHASED_COST, src.SALE_DATE, src.SALE_PRICE, src.CLUB_LOCATION, src.DEALER_NAME, src.DEALER_CITY, src.DEALER_STATE, src.PO_NUMBER, src.BOAT_YEAR_MODEL, src.MOTOR_YEAR_MODEL, src.MOTOR_MANUFACTURER_MODEL, src.STATE_REG_DATE, src.STATE_REG_EXP_DATE, src.ENGINE_PURCHASED_COST, src.BACKEND_DISPLAY, src.POSITION_ORDER, src.STATUS_BOAT, src.SERVICE_START, src.SERVICE_END, src.CLEAN_STATUS, src.INSURANCE_REG_NO, src.BUOY_INSURANCE_STATUS, src.CREATED_AT, src.UPDATED_AT,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_STYLE_BOATS: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_STYLE_BOATS: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_STYLE_BOATS;
/
