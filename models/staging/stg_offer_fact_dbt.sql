{{ config(materialized='table') }}

-- Complex staging model for offer fact data with JSON parsing and business logic
-- Target table: watson.stg_offer_fact_dbt

with offer_events_data as (
    select
        offer_instance_id as offer_id,
        max(case when state in ('ACCEPTED','DECLINED','WITHDRAWN','EXPIRED','CANCELED','CHANGED') then timestamp end) as offer_response_ts,
        max(case when state = 'ACTIVE' then true else false end) as is_visible_offer
    from {{ source('wms_offers', 'events') }}
    -- these are events that are being considered a "response"
    where state in ('ACCEPTED','DECLINED','WITHDRAWN','EXPIRED','CANCELED','CHANGED','ACTIVE')
    group by all
)

select
    o.instance_id as offer_id,
    o.previous_instance_id as previous_offer_id,
    o.next_instance_id as new_offer_id,
    case when o.source_type = 'JOB_APPLICATION' then o.source_id end as agora_bid_id,
    o.job_posting_uid as agora_post_id,
    ct.contract_id,
    o.title as offer_title,
    case when try_parse_json(o.offer_terms):milestoneTerms is not null then 'Fixed'
         when try_parse_json(o.offer_terms):hourlyTerms is not null
            or try_parse_json(o.offer_terms):stipendTerms is not null then 'Hourly'
    end as job_type_name,
    try_parse_json(offer_terms):hourlyTerms:rate:amount::numeric(38,2) as hourly_rate,
    try_parse_json(offer_terms):milestoneTerms:budget:amount::numeric(38,2) as bid_amount,
    coalesce(try_parse_json(offer_terms):stipendTerms:amount:amount::numeric(38,2),0) as weekly_payment_amount,
    o.contract_start::date as proposed_start_date,
    o.client_org_id,
    o.client_person_id,
    o.client_team_id,
    o.vendor_person_id,
    o.vendor_team_id,
    o.vendor_type,
    o.creation_timestamp::date as offer_date,
    o.creation_timestamp::timestamp_ntz(9) as offer_ts,
    oed.offer_response_ts::timestamp_ntz(9) as offer_response_ts,
    oed.offer_response_ts::date as offer_response_date,
    lower(o.state) as offer_status_name,
    o.termination_reason_id as offer_reason_id,
    r.reason as offer_reason_name,
    'wms_offers.offers'::varchar as src_table_name,
    o.creation_timestamp as src_create_ts,
    o.modification_timestamp as src_modified_ts,
    case when o.delivery_model in ('CATALOG_PROJECT','TALENT_SCOUT') then initcap(split(o.delivery_model,'_')[0]) || initcap(split(o.delivery_model,'_')[1])
         when traits like '%PAYROLL%' then 'Payroll'
         else 'N/A'
    end as offer_job_subtype,
    case when o.source_type = 'CONTRACT_PROPOSAL' then o.source_id end as contract_proposal_uid,
    coalesce(byo.is_byo and o.delivery_model not ilike 'manage_and_pay', false) as is_byo,
    coalesce(byo.is_manage_and_pay or o.delivery_model ilike 'manage_and_pay', false) as is_manage_and_pay,
    e.domain_id is not null as is_sri_enabled,
    e.rate as sri_rate_increase_percent,
    coalesce(e.cadence_months,0) as sri_cadence_months,
    coalesce(oed.is_visible_offer,false) as is_visible_offer,
    -- duplicating data from platform, cause stg is filtered for ghost posts
    oo.created_by_uid as src_post_created_person_id,
    oo.organization_uid,
    oo.company_uid,
    coalesce(oe.direct_hire,false) as is_direct_hire, 
    o.delivery_model,
    o.source_type,
    ct.id as contract_terms_id
from {{ source('wms_offers', 'offers') }} o
left join {{ source('contracts', 'terms') }} ct on ct.id=o.contract_id
left join {{ source('odb2_odesk_db', 'reasons') }} r on o.termination_reason_id = r."Record_ID#"
left join offer_events_data oed on o.instance_id = oed.offer_id
left join {{ ref('stg_byo_dbt') }} byo 
    on o.client_org_id = byo.client_agora_company_id 
    and o.vendor_person_id = byo.freelancer_person_id 
    and byo.invitee_type ilike 'freelancer'
left join {{ ref('tmp_sri_entities_dbt') }} e 
    on o.instance_id = e.domain_id 
    and e.domain ilike 'offer'
left join {{ source('openings', 'openings') }} oo on o.job_posting_uid = oo.uid
left join {{ source('openings_extra', 'openings_extra') }} oe on o.job_posting_uid = oe.uid 