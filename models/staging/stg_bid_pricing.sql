{{ config(materialized='view') }}

with vendor_applications as (
    select
        application_uid as agora_bid_id,
        opening_uid as agora_post_id,
        vendor_uid as freelancer_id,
        applying_as,
        vendor_org_uid as agora_company_id,
        vendor_sub_org_uid as agora_team_id,
        status as modernized_status,
        legacy_rid as bid_id,
        terms as amount,
        duration,
        created_ts as src_create_ts,
        modified_ts as src_modified_ts,
        'vendor_job_applications' as src_table_name,
        'Professional' as src_create_type,
        date(created_ts) as bid_date,
        created_ts as bid_ts
    from {{ source('vendor_job_applications', 'vendor_job_applications') }}
    where new_uid is null  -- Get current version only
),

client_applications as (
    select
        application_uid,
        hidden_by_client,
        shortlisted as is_shortlisted,
        recommended as is_recommended,
        recommendation_score as recommend_score
    from {{ source('client_job_applications', 'client_job_applications') }}
),

connects_usage as (
    select 
        ctl.job_application_id,
        sum(ctl.amount) as connects_used,
        sum(ctl.amount) as connects_amount
    from {{ source('eo_monetization', 'connects_transaction_log') }} ctl
    join {{ source('eo_monetization', 'connects_transaction_reason') }} ctr
        on ctl.reason_id = ctr.id
    where ctr.name in ('PROPOSAL_SUBMISSION', 'PROPOSAL_APPLICATION')
    group by ctl.job_application_id
),

boosted_bids as (
    select
        job_application_uid,
        sum(case when type_id = 1 then amount else 0 end) as boosted_amount
    from {{ source('connects_pricing', 'bid') }}
    group by job_application_uid
)

select
    va.agora_bid_id,
    va.agora_post_id,
    va.agora_post_id::string as post_id,  -- Legacy compatibility
    va.freelancer_id,
    va.applying_as,
    va.agora_company_id,
    va.agora_team_id,
    va.modernized_status,
    va.bid_id,
    va.bid_date,
    va.bid_ts,
    va.amount,
    null::decimal(10,2) as hourly_rate,  -- Simplified for POC
    va.duration,
    
    -- Client-side data
    coalesce(ca.is_recommended, false) as is_recommended,
    coalesce(ca.is_shortlisted, false) as is_shortlisted,
    coalesce(ca.recommend_score, 0) as recommend_score,
    coalesce(ca.hidden_by_client, false) as hidden_by_client,
    
    -- Connects and boosting
    coalesce(cu.connects_used, 0) as connects_used,
    coalesce(cu.connects_amount, 0) as connects_amount,
    coalesce(bb.boosted_amount, 0) as boosted_amount,
    case when bb.boosted_amount > 0 then true else false end as is_boosted,
    
    -- Additional flags
    false as is_invited,  -- Simplified for POC
    false as is_on_hold,  -- Simplified for POC
    false as is_uma_proposal,  -- Simplified for POC
    null as invitation_uid,  -- Simplified for POC
    
    -- Source metadata
    va.src_table_name,
    va.src_create_type,
    va.src_create_ts,
    va.src_modified_ts
    
from vendor_applications va
left join client_applications ca
    on va.agora_bid_id = ca.application_uid
left join connects_usage cu
    on va.agora_bid_id = cu.job_application_id
left join boosted_bids bb
    on va.agora_bid_id = bb.job_application_uid 