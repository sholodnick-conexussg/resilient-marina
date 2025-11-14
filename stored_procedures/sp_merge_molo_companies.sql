
-- ============================================================================
-- Merge STG_MOLO_COMPANIES to DW_MOLO_COMPANIES
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_MOLO_COMPANIES
IS
    v_merged NUMBER := 0;
BEGIN
    MERGE INTO DW_MOLO_COMPANIES tgt
    USING STG_MOLO_COMPANIES src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.NAME = src.NAME,
            tgt.OWNER = src.OWNER,
            tgt.PRIMARY_FAX_NUMBER = src.PRIMARY_FAX_NUMBER,
            tgt.PRIMARY_PHONE_NUMBER = src.PRIMARY_PHONE_NUMBER,
            tgt.CITY_ID = src.CITY_ID,
            tgt.IMAGE = src.IMAGE,
            tgt.DESCRIPTION = src.DESCRIPTION,
            tgt.PARTNER_ID = src.PARTNER_ID,
            tgt.MOLO_API_PARTNER_ID = src.MOLO_API_PARTNER_ID,
            tgt.COMPANY_MOLO_API_PARTNER_COMPANY_ID = src.COMPANY_MOLO_API_PARTNER_COMPANY_ID,
            tgt.INVOICE_AT_COMPANY_LEVEL = src.INVOICE_AT_COMPANY_LEVEL,
            tgt.MOLO_CONTACT_ID = src.MOLO_CONTACT_ID,
            tgt.STRIPE_CUSTOMER_ID = src.STRIPE_CUSTOMER_ID,
            tgt.LOGIN_PROVIDER_ID = src.LOGIN_PROVIDER_ID,
            tgt.DEFAULT_CC_FEE = src.DEFAULT_CC_FEE,
            tgt.TIER1_PERCENT_ACH_FEE = src.TIER1_PERCENT_ACH_FEE,
            tgt.TIER2_PERCENT_ACH_FEE = src.TIER2_PERCENT_ACH_FEE,
            tgt.DW_LAST_UPDATED = SYSTIMESTAMP
        WHERE 
            -- Only update if data has actually changed
            NVL(tgt.NAME, '~') != NVL(src.NAME, '~')
            OR NVL(tgt.OWNER, '~') != NVL(src.OWNER, '~')
            OR NVL(tgt.PRIMARY_FAX_NUMBER, -1) != NVL(src.PRIMARY_FAX_NUMBER, -1)
            OR NVL(tgt.PRIMARY_PHONE_NUMBER, -1) != NVL(src.PRIMARY_PHONE_NUMBER, -1)
            OR NVL(tgt.CITY_ID, -1) != NVL(src.CITY_ID, -1)
            OR NVL(tgt.IMAGE, '~') != NVL(src.IMAGE, '~')
            OR NVL(tgt.DESCRIPTION, '~') != NVL(src.DESCRIPTION, '~')
            OR NVL(tgt.PARTNER_ID, -1) != NVL(src.PARTNER_ID, -1)
            OR NVL(tgt.MOLO_API_PARTNER_ID, -1) != NVL(src.MOLO_API_PARTNER_ID, -1)
            OR NVL(tgt.COMPANY_MOLO_API_PARTNER_COMPANY_ID, -1) != NVL(src.COMPANY_MOLO_API_PARTNER_COMPANY_ID, -1)
            OR NVL(tgt.INVOICE_AT_COMPANY_LEVEL, '~') != NVL(src.INVOICE_AT_COMPANY_LEVEL, '~')
            OR NVL(tgt.MOLO_CONTACT_ID, -1) != NVL(src.MOLO_CONTACT_ID, -1)
            OR NVL(tgt.STRIPE_CUSTOMER_ID, -1) != NVL(src.STRIPE_CUSTOMER_ID, -1)
            OR NVL(tgt.LOGIN_PROVIDER_ID, -1) != NVL(src.LOGIN_PROVIDER_ID, -1)
            OR NVL(tgt.DEFAULT_CC_FEE, -1) != NVL(src.DEFAULT_CC_FEE, -1)
            OR NVL(tgt.TIER1_PERCENT_ACH_FEE, -1) != NVL(src.TIER1_PERCENT_ACH_FEE, -1)
            OR NVL(tgt.TIER2_PERCENT_ACH_FEE, -1) != NVL(src.TIER2_PERCENT_ACH_FEE, -1)
    WHEN NOT MATCHED THEN
        INSERT (
            ID, NAME, OWNER, PRIMARY_FAX_NUMBER, PRIMARY_PHONE_NUMBER, CITY_ID, IMAGE, DESCRIPTION, PARTNER_ID, MOLO_API_PARTNER_ID, COMPANY_MOLO_API_PARTNER_COMPANY_ID, INVOICE_AT_COMPANY_LEVEL, MOLO_CONTACT_ID, STRIPE_CUSTOMER_ID, LOGIN_PROVIDER_ID, DEFAULT_CC_FEE, TIER1_PERCENT_ACH_FEE, TIER2_PERCENT_ACH_FEE,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.NAME, src.OWNER, src.PRIMARY_FAX_NUMBER, src.PRIMARY_PHONE_NUMBER, src.CITY_ID, src.IMAGE, src.DESCRIPTION, src.PARTNER_ID, src.MOLO_API_PARTNER_ID, src.COMPANY_MOLO_API_PARTNER_COMPANY_ID, src.INVOICE_AT_COMPANY_LEVEL, src.MOLO_CONTACT_ID, src.STRIPE_CUSTOMER_ID, src.LOGIN_PROVIDER_ID, src.DEFAULT_CC_FEE, src.TIER1_PERCENT_ACH_FEE, src.TIER2_PERCENT_ACH_FEE,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    
    v_merged := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_MOLO_COMPANIES: Merged ' || v_merged || ' records');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_MOLO_COMPANIES: ' || SQLERRM);
        RAISE;
END SP_MERGE_MOLO_COMPANIES;
/
