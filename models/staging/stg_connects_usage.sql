{{ config(materialized='view') }}

with connects_transactions as (
    select 
        ctl.job_application_id,
        sum(ctl.amount) as connects_used
    from {{ source('eo_monetization', 'connects_transaction_log') }} ctl
    inner join {{ source('eo_monetization', 'connects_transaction_reason') }} ctr 
        on ctr.id::bigint = ctl.reason_id::bigint
    where 
        ctr.name = 'JOB_APPLICATION'
        and ctl.job_application_id != 0
        and ctl.job_application_id is not null
    group by 1
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
    -- Add connects usage information
    sn.connects_used
from {{ ref('stg_vendor_job_applications') }} vja 
left join connects_transactions sn 
    on sn.job_application_id = vja.agora_bid_id 