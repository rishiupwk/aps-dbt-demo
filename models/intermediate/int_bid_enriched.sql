{{ config(materialized='view') }}

with bid_base as (
    select * from {{ ref('stg_bid_pricing') }}
),

enriched_bids as (
    select
        -- Original bid data
        b.post_id,
        b.agora_post_id,
        f.freelancer_id,
        
        -- Dimension keys resolved from business keys
        coalesce(p.post_key, 0) as post_key,
        coalesce(ph.post_history_key, 0) as post_history_key,
        coalesce(cd.client_key, 0) as client_key,
        coalesce(ad.agency_key, 0) as freelancer_agency_key,
        coalesce(f.freelancer_key, 0) as freelancer_key,
        coalesce(fh.freelancer_history_key, 0) as freelancer_history_key,
        coalesce(et.engagement_type_key, 0) as engagement_type_key,
        coalesce(ed.engagement_duration_key, ed2.engagement_duration_key, 0) as engagement_duration_key,
        coalesce(jt.job_type_key, 0) as job_type_key,
        coalesce(s2.status_key, 0) as bid_status_key,
        coalesce(s1.status_key, 0) as ats_status_key,
        coalesce(arae.accounting_entity_key, 0) as ar_accounting_entity_key,
        coalesce(apae_ag.accounting_entity_key, apae_f.accounting_entity_key, 0) as ap_accounting_entity_key,
        coalesce(s3.status_key, 0) as hide_status_key,
        
        -- Bid details
        b.bid_id,
        b.agora_bid_id,
        b.bid_date,
        b.bid_ts,
        b.recommend_score,
        coalesce(b.is_recommended, false) as is_recommended,
        coalesce(b.is_shortlisted, false) as is_shortlisted,
        coalesce(b.amount, 0) as bid_amount,
        b.hourly_rate,
        coalesce(b.is_invited, false) as is_invited,
        coalesce(b.is_on_hold, false) as is_on_hold,
        coalesce(b.is_uma_proposal, false) as is_uma_proposal,
        b.invitation_uid as origin_agora_invite_id,
        b.src_table_name,
        b.src_create_type,
        coalesce(u.user_id, 0) as src_create_user_id,
        b.src_create_ts,
        coalesce(u2.user_id, 0) as src_modified_user_id,
        greatest(
            coalesce(b.src_modified_ts, '1900-01-01 00:00:00'), 
            coalesce(fh.src_modified_ts, '1900-01-01 00:00:00'),
            coalesce(ph.src_modified_ts, '1900-01-01 00:00:00')
        )::timestamp_ntz(9) as src_modified_ts,
        b.dashroom_uid as dash_room_uid,
        coalesce(p.accountability_revenue_key, 0) as accountability_revenue_key,
        p.sales_division,
        b.is_ts_recruiter_services_from_shortlist,
        
        -- IC/Agency/EOR classification
        case 
            when b.applying_as = 'freelancer' then 'IC'
            when b.applying_as = 'agency' and ad.is_w2_agency then 'EOR'
            when b.applying_as = 'agency' then 'Agency'
            else 'unknown'
        end as ic_agency_eor,
        
        b.connects_used,
        
        -- Client first response attributes
        b.agora_agency_id,
        b.agora_company_id,
        b.freelancer_person_id,
        b.hiring_manager_person_id as post_hiring_manager_person_id,
        b.invite_src_create_person_id as bid_hiring_manager_person_id,
        b.src_create_person_id as bid_freelancer_person_id,
        
        -- SRI and boosting flags
        e.domain_id is not null as is_sri_enabled,
        b.is_boosted,
        b.boosted_bid_amount,
        b.is_boosted_cleared,
        b.clearing_charge,
        b.paid_clearing_charge
        
    from bid_base b
    left join {{ source('sherlock', 'post_dim') }} p 
        on b.agora_post_id::bigint = p.agora_post_id::bigint 
        and p.platform = 'Upwork'
    left join {{ source('sherlock', 'post_history_dim') }} ph 
        on p.post_key::bigint = ph.post_key::bigint 
        and b.src_create_ts between ph.start_ts and ph.end_ts 
        and p.platform = 'Upwork'
    left join {{ source('sherlock', 'client_dim') }} cd 
        on b.agora_company_id = cd.agora_company_id 
        and cd.grain_name = 'client'
    left join {{ source('sherlock', 'agency_dim') }} ad 
        on b.agora_agency_team_id = ad.agora_team_id
    left join {{ source('sherlock', 'freelancer_dim') }} f 
        on b.freelancer_person_id = f.person_id
    left join {{ source('sherlock', 'freelancer_history_dim') }} fh 
        on f.freelancer_key = fh.freelancer_key 
        and b.bid_ts between fh.start_ts and fh.end_ts
    left join {{ source('sherlock', 'engagement_type_dim') }} et 
        on b.engagement_type_id = et.engagement_type_id 
    left join {{ source('sherlock', 'engagement_duration_dim') }} ed 
        on b.engagement_duration = ed.engagement_duration_id
    left join {{ source('sherlock', 'engagement_duration_dim') }} ed2 
        on b.engagement_duration_uid = ed2.engagement_duration_uid
    left join {{ source('sherlock', 'job_type_dim') }} jt 
        on b.job_type = jt.job_type_name 
        and jt.job_subtype_code = 'N/A'
    left join {{ source('odb2_odesk_db', 'reasons') }} r 
        on b.reason_id = r."Record_ID#"
    left join {{ source('sherlock', 'status_dim') }} s1 
        on b.ats_status = s1.status_name 
        and s1.recipient_table_name = 'bid_fact' 
        and s1.recipient_column_name = 'ats_status_key'
    left join {{ source('sherlock', 'status_dim') }} s2 
        on b.modernized_status = s2.status_name 
        and coalesce(r.reason, b.modernized_status) = s2.reason_name 
        and s2.recipient_table_name = 'bid_fact' 
        and s2.recipient_column_name = 'bid_status_key'
    left join {{ source('sherlock', 'status_dim') }} s3 
        on b.hide_status_name::varchar = s3.status_name 
        and s3.recipient_table_name = 'bid_fact' 
        and s3.recipient_column_name = 'hide_status_key'
    left join {{ source('sherlock', 'accounting_entity_dim') }} arae 
        on arae.accounting_entity_id = cd.accounting_entity
    left join {{ source('sherlock', 'accounting_entity_dim') }} apae_f 
        on f.accounting_entity_id = apae_f.accounting_entity_id
    left join {{ source('sherlock', 'accounting_entity_dim') }} apae_ag 
        on ad.team_accounting_entity_id = apae_ag.accounting_entity_id
    left join {{ source('sherlock', 'user_dim') }} u 
        on b.src_create_person_id::varchar = u.person_id::varchar
    left join {{ source('sherlock', 'user_dim') }} u2 
        on b.src_modified_person_id::varchar = u2.person_id::varchar
    left join {{ source('watson', 'tmp_sri_entities') }} e 
        on b.agora_bid_id = e.domain_id 
        and e.domain ilike 'jobapplication'
)

select
    *,
    -- Generate MD5 hash for change detection (excluding agora_bid_id and ETL-only columns)
    {{ dbt_utils.generate_surrogate_key([
        'post_id', 'agora_post_id', 'freelancer_id', 'post_key', 'post_history_key', 
        'client_key', 'freelancer_agency_key', 'freelancer_key', 'freelancer_history_key',
        'engagement_type_key', 'engagement_duration_key', 'job_type_key', 'bid_status_key', 
        'ats_status_key', 'ar_accounting_entity_key', 'ap_accounting_entity_key',
        'hide_status_key', 'bid_id', 'bid_date', 'bid_ts', 'recommend_score', 
        'is_recommended', 'is_shortlisted', 'bid_amount', 'hourly_rate', 'is_invited', 
        'is_on_hold', 'is_uma_proposal', 'origin_agora_invite_id', 'src_table_name', 
        'src_create_type', 'src_create_user_id', 'src_create_ts', 'src_modified_user_id', 
        'src_modified_ts', 'accountability_revenue_key', 'sales_division', 
        'is_ts_recruiter_services_from_shortlist', 'ic_agency_eor', 'connects_used', 
        'is_sri_enabled', 'is_boosted', 'boosted_bid_amount', 'is_boosted_cleared', 
        'clearing_charge', 'paid_clearing_charge'
    ]) }} as md5_hash
from enriched_bids 