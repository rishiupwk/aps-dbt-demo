-- Active memberships - contains all active memberships (most expensive selected)
-- Target table: watson.stg_active_memberships_dbt

SELECT legacy_id, membership_category_name, update_timestamp, src_modified_ts
FROM (
    SELECT
        r.legacy_id,
        md.membership_category_name,
        update_timestamp,
        sa.effective_date::date AS src_modified_ts,
        ROW_NUMBER() over (PARTITION BY r.legacy_id ORDER BY md.membership_fee desc, update_timestamp desc) AS row_rank
    FROM SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.subscription_account_summary sa
    INNER JOIN SHASTA_SDC_UPWORK.eo_monetization.receiver r 
        ON sa.receiver_id::BIGINT = r.id::BIGINT
    INNER JOIN SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.membership_dim md 
        ON sa.product_id::BIGINT = md.membership_id::BIGINT
    WHERE md.membership_name <> 'Connects' 
        AND r.legacy_id NOT ILIKE '%INVALID'
        AND (sa.expiry_date IS NULL OR sa.expiry_date::date > current_date)
) m
WHERE m.row_rank = 1 