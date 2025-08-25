{{ config(materialized='table') }}

-- Bring Your Own (BYO) and Manage & Pay relationship staging model
-- Target table: watson.stg_byo_dbt

select
    byo.invitee_type,
    o.top_level_org_uid as client_agora_company_id,
    iff(byo.invitee_type ilike 'agency', byo.agency_uid, null) as agency_agora_company_id,
    iff(byo.invitee_type ilike 'freelancer', byo.uid, null) as freelancer_person_id,
    max(byo.relationship_type ilike 'legacy_byo') as is_byo,
    max(byo.relationship_type ilike 'manage_and_pay') as is_manage_and_pay,    
    min(iff(byo.relationship_type ilike 'legacy_byo', to_timestamp(offer_created_on_ts/1000), null)) as byo_ts
from {{ source('bring_your_own', 'registration_invitations') }} byo
join {{ source('directory', 'organizations') }} o 
    on byo.sender_company_recno::bigint = o.legacy_ref::bigint
where coalesce(byo.uid, byo.agency_uid) is not null
group by all
qualify row_number() over(
    partition by byo.invitee_type, client_agora_company_id, agency_agora_company_id, freelancer_person_id 
    order by byo_ts desc
) = 1 