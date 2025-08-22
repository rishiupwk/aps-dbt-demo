{{
    config(
        materialized='incremental',
        unique_key='agora_bid_id',
        merge_exclude_columns=['dw_create_ts'],
        on_schema_change='fail'
    )
}}

select
    -- Dimension keys
    post_key,
    post_history_key,
    client_key,
    freelancer_key,
    freelancer_history_key,
    freelancer_agency_key,
    engagement_duration_key,
    engagement_type_key,
    job_type_key,
    bid_status_key,
    ats_status_key,
    ar_accounting_entity_key,
    ap_accounting_entity_key,
    hide_status_key,
    
    -- Business identifiers
    bid_id,
    agora_bid_id,
    origin_agora_invite_id,
    post_id,
    agora_post_id,
    freelancer_id,
    
    -- Bid details
    bid_date,
    bid_ts,
    recommend_score,
    is_recommended,
    is_shortlisted,
    bid_amount,
    hourly_rate,
    is_invited,
    is_on_hold,
    is_uma_proposal,
    connects_used,
    dash_room_uid,
    
    -- Source metadata
    src_table_name,
    src_create_type,
    src_create_user_id,
    src_create_ts,
    src_modified_user_id,
    src_modified_ts,
    
    -- Business attributes
    accountability_revenue_key,
    sales_division,
    is_ts_recruiter_services_from_shortlist,
    ic_agency_eor,
    is_sri_enabled,
    is_boosted,
    boosted_bid_amount,
    is_boosted_cleared,
    clearing_charge,
    paid_clearing_charge,
    
    -- Lifecycle attributes (will be updated by post-hooks)
    null::timestamp_ntz as client_first_response_ts,
    null::varchar as client_first_response_message_type,
    null::boolean as is_first_post_bid,
    null::boolean as is_first_freelancer_bid,
    null::boolean as is_qualified_bid,
    
    -- Data warehouse metadata
    true as dw_active_flag,
    {% if is_incremental() %}
        case 
            when target.agora_bid_id is null then current_timestamp()
            else target.dw_create_ts
        end as dw_create_ts,
    {% else %}
        current_timestamp() as dw_create_ts,
    {% endif %}
    current_timestamp() as dw_modified_ts,
    md5_hash as dw_md5_hash

from {{ ref('int_bid_enriched') }} source

{% if is_incremental() %}
    left join {{ this }} target
        on source.agora_bid_id = target.agora_bid_id
    where 
        target.agora_bid_id is null  -- new records
        or source.md5_hash != target.dw_md5_hash  -- changed records
{% endif %} 