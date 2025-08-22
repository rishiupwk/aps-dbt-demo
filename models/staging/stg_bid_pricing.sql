{{ config(materialized='view') }}

with bid_pricing as (
    select
        job_application_uid,
        type_id,
        amount as boosted_bid_amount,
        created_ts
    from {{ source('connects_pricing', 'bid') }} bid
    qualify row_number() over(
        partition by job_application_uid, type_id 
        order by created_ts desc
    ) = 1
),

clearing_charges as (
    select 
        ctl.job_application_id,
        ctl.amount as clearing_charge,
        ctle.paid_amount as paid_clearing_charge
    from {{ source('eo_monetization', 'connects_transaction_log') }} ctl
    join {{ source('eo_monetization', 'connects_transaction_reason') }} ctr
        on ctl.reason_id = ctr.id
    left join {{ source('eo_monetization', 'connects_transaction_log_ext') }} ctle
        on ctl.id = ctle.transaction_id
    where ctr.name = 'SPONSORED_PROPOSAL_AUCTION_CLEARING_CHARGE'
    qualify row_number() over(
        partition by job_application_id 
        order by ctl.timestamp desc
    ) = 1
)

select
    agora_bid_id,
    post_id,
    agora_post_id,
    agora_company_id,
    agora_team_id,
    applying_as,
    agora_agency_id,
    agora_agency_team_id,
    freelancer_person_id,
    engagement_type_id,
    engagement_duration,
    engagement_duration_uid,
    job_type,
    modernized_status,
    reason_id,
    ats_status,
    hide_status_name,
    hidden_by_client_reason_rid,
    bid_id,
    bid_date,
    bid_ts,
    recommend_score,
    is_recommended,
    is_shortlisted,
    is_ts_recruiter_services_from_shortlist,
    amount,
    hourly_rate,
    is_invited,
    invitation_uid,
    dashroom_uid,
    is_on_hold,
    is_uma_proposal,
    src_table_name,
    src_create_type,
    src_create_person_id,
    src_modified_person_id,
    invite_src_create_person_id,
    hiring_manager_person_id,
    src_create_ts,
    src_modified_ts,
    _loaded_at,
    connects_used,
    
    -- Add boosted bid information
    case when bcps1.job_application_uid is not null then true else false end as is_boosted,
    bcps1.boosted_bid_amount,
    case when bcps3.job_application_uid is not null then true else false end as is_boosted_cleared,
    cc.clearing_charge,
    cc.paid_clearing_charge
    
from {{ ref('stg_connects_usage') }} vja 
left join bid_pricing bcps1 
    on vja.agora_bid_id = bcps1.job_application_uid 
    and bcps1.type_id = 1
left join bid_pricing bcps3 
    on vja.agora_bid_id = bcps3.job_application_uid 
    and bcps3.type_id = 3
left join clearing_charges cc 
    on vja.agora_bid_id = cc.job_application_id 