{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail'
) }}

-- Incremental model for TNS device management device IDs
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
from {{ source('tns_datamart_tns_device_mgmt', 'device_ids') }} s
{% if is_incremental() %}
    left join {{ this }} t 
        on s.id = t.id 
        and t.event_id = 2
{% endif %}
where s.event_id = 2 
{% if is_incremental() %}
    and t.id is null
{% endif %} 