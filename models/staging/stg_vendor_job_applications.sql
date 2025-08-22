{{ config(materialized='view') }}

with vendor_applications as (
    select
        vja.application_uid as agora_bid_id,
        op.legacy_rid as post_id,
        vja.opening_uid as agora_post_id,
        coalesce(inv.client_org_uid, op.company_uid) as agora_company_id,
        coalesce(inv.client_team_org_uid, op.organization_uid) as agora_team_id,
        vja.applying_as,
        case 
            when vja.applying_as = 'agency' then vja.vendor_org_uid 
        end as agora_agency_id,
        case 
            when vja.applying_as = 'agency' then vja.vendor_sub_org_uid 
        end as agora_agency_team_id,
        vja.vendor_uid as freelancer_person_id,
        op.derived_engagement_type as engagement_type_id,
        vja.duration as engagement_duration,
        op.duration_uid as engagement_duration_uid,
        op.derived_type as job_type,
        vja.status as modernized_status,
        
        -- Handle various reason fields
        coalesce(
            try_to_number(vja.withdraw_reason_rid),
            try_to_number(vja.decline_reason_rid), 
            try_to_number(vja.archive_reason_rid), 
            try_to_number(vja.invalid_reason_rid)
        )::bigint as reason_id,
        
        -- ATS status logic
        case
            when cja.hidden_by_client::boolean is null and cja.shortlisted::boolean is null then null
            when cja.hidden_by_client::boolean then 'hidden'
            when cja.shortlisted::boolean then 'shortlisted'
            else 'undecided'
        end as ats_status,
        
        -- Hide status logic
        case
            when cja.hidden_by_client::boolean then 'Hidden by Buyer'
            when cja.auto_hidden::boolean then 'Auto Hidden'
            else 'Not Hidden'
        end as hide_status_name,
        
        nullif(cja.hidden_by_client_reason_rid, '') as hidden_by_client_reason_rid,
        coalesce(vja.legacy_rid, cja.legacy_rid) as bid_id,
        vja.created_ts::date as bid_date,
        vja.created_ts::timestamp_ntz(9) as bid_ts,
        cja.recommendation_score as recommend_score,
        coalesce(cja.recommended::boolean, false) as is_recommended,
        coalesce(cja.shortlisted::boolean, false) as is_shortlisted,
        coalesce(
            lower(cja.other_annotations) like '%shortlistedbyrecruiter%', 
            false
        ) as is_ts_recruiter_services_from_shortlist,
        
        -- Bid amount logic based on engagement type
        case 
            when op.type = 'FIXED_PRICE' then nullif(vja.terms, '')::numeric(38,8) 
        end as amount,
        case 
            when op.type = 'HOURLY' then nullif(vja.terms, '')::numeric(38,8) 
        end as hourly_rate,
        
        -- Invitation and dashboard room info
        coalesce(vja.invite_to_interview_uid, cja.invite_to_interview_uid) is not null as is_invited,
        coalesce(vja.invite_to_interview_uid, cja.invite_to_interview_uid) as invitation_uid,
        coalesce(vja.dashroom_uid, cja.dash_room_uid) as dashroom_uid,
        vja.on_hold as is_on_hold,
        coalesce(vja.other_annotations ilike '%umatouched%', false) as is_uma_proposal,
        
        -- Source metadata
        'vendor_job_applications' as src_table_name,
        case 
            when coalesce(vja.invite_to_interview_uid, cja.invite_to_interview_uid) is not null 
            then 'Client' 
            else 'Professional' 
        end as src_create_type,
        coalesce(vja.created_by, cja.created_by) as src_create_person_id,
        coalesce(vja.last_modified_by, cja.last_modified_by) as src_modified_person_id,
        inv.created_by as invite_src_create_person_id,
        op.created_by_uid as hiring_manager_person_id,
        vja.created_ts::timestamp_ntz(9) as src_create_ts,
        vja.modified_ts::timestamp_ntz(9) as src_modified_ts,
        
        current_timestamp() as _loaded_at
        
    from {{ source('vendor_job_applications', 'vendor_job_applications') }} vja
    left join {{ source('client_job_applications', 'client_job_applications') }} cja 
        on vja.application_uid = cja.application_uid
    left join {{ source('watson', 'stg_odeskdb_openings') }} op 
        on vja.opening_uid = op.uid
    left join {{ source('client_job_application_invitations', 'aggregates') }} inv 
        on vja.invite_to_interview_uid = inv.invitation_uid
    where 
        vja.new_uid is null -- excluding previous versions of the bid
        and vja.status <> 'pending' -- hidden job apps from QT
        and vja.modified_ts >= current_date - {{ var('bid_lookback_days') }} -- configurable lookback period
)

select * from vendor_applications 