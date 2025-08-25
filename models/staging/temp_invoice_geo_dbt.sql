-- Invoice geolocation data with address standardization
-- Target table: watson.temp_invoice_geo_dbt

WITH temp_invoice_country AS (
    SELECT
        ia.inva_developer_rid as freelancer_id,
        ia.inva_company_rid as client_id,
        ia.inva_country,
        ia.inva_state,
        ia.inva_postal_code,
        ia.inva_city,
        ia.inva_street,
        ia.inva_created_ts,
        md5(array_to_string(array_construct(ia.inva_country,ia.inva_state,ia.inva_city),'~!~')) as geo_hash
    FROM SHASTA_SDC_UPWORK.odesk_bpa_odesk_bp.invoice_addresses ia
    WHERE ia.inva_removed_ts IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ia.inva_company_rid, ia.inva_developer_rid ORDER BY ia.inva_created_ts DESC) = 1
)

SELECT
    txc.freelancer_id,
    txc.client_id,
    g.g_region_name as invoice_region_name,
    g.g_country_code as invoice_country_code,
    g.g_country_name as invoice_country_name,
    g.g_state_code as invoice_state_code,
    g.g_state_name as invoice_state_name,
    coalesce(g.g_city_name, txc.inva_city) as invoice_city_name,
    txc.inva_postal_code as invoice_postal_code,
    txc.inva_street as invoice_street
FROM temp_invoice_country txc
LEFT JOIN SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.user_geo_location_map_dim g 
    on g.user_geo_uid = txc.geo_hash 