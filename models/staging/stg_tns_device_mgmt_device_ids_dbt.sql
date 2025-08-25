-- TNS device management device IDs (simplified without incremental logic)
-- Target table: watson.stg_tns_device_mgmt_device_ids_dbt

select
    s.id,
    lower(s.username) as username,
    s.device_browser_type,
    s.device_os,
    s.device_type,
    s.ts::timestamp as ts,
    s.realipaddress_loc_country,
    s.realipaddress_loc_countrycode,
    s.realipaddress_loc_city,
    s.realipaddress_loc_region,
    s.event_id,
    current_timestamp as dw_create_ts,
    0 as dw_create_user_id
from SHASTA_SDC_UPWORK.tns_datamart_tns_device_mgmt.device_ids s
where s.event_id = 2 