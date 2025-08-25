-- Holds all distinct persons records with extra fields for person/user information
-- Target table: watson.persons_directory_mapping_dbt

with default_agency as (
    select
        po.prs_uid as person_id,
        o.name as default_team_name,
        o.legacy_ref as default_agency_team_id
    from SHASTA_SDC_UPWORK.directory.persons_organizations po
    join SHASTA_SDC_UPWORK.directory.organizations o on po.org_uid = o.uid
    where po.act_status = 1
        and po.is_default
        and o.type = 1
        and o.legacy_type = 2
    qualify row_number() over(partition by po.prs_uid order by po.created_on desc nulls last) = 1
)

select
    p.uid::BIGINT AS person_id,
    LOWER(p.legacy_id) AS username,
    p.legacy_ref AS freelancer_id,
    u."Record_ID#"::BIGINT AS user_id,
    not p.is_active::boolean as is_account_closed,
    p.created_on::TIMESTAMP_NTZ(9) AS account_creation_ts,
    p.last_updated_on::TIMESTAMP_NTZ(9) AS account_modified_ts,
    p.first_name,
    p.last_name,
    p.email,
    p.timezone,
    p.country,
    p.state,
    substring(p.city,1,255) AS city,
    substring(p.zip,1,255) AS zip,
    md5(array_to_string(array_construct(p.country, p.state, p.city),'~!~'))::varchar as profile_geo_hash,
    -- Replaces isprovider flag from legacy odesk developers
    COALESCE(dm.is_owner_active, FALSE) as is_provider,
    COALESCE(dm.is_owner_active, FALSE) as is_owner_active,
    da.default_agency_team_id,
    da.default_team_name,
    dm.agora_team_id as agora_company_id,
    dm.is_account_closed as is_company_account_closed,
    dm.account_creation_ts::date as company_account_creation_date,
    dm.account_creation_ts as company_account_creation_ts
from SHASTA_SDC_UPWORK.directory.persons p
left join SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.watson.directory_mapping_dbt dm 
    on p.uid::BIGINT = dm.person_id::BIGINT
    and dm.account_type = 'Freelancer' and dm.is_latest_org
left join SHASTA_SDC_UPWORK.odb2_odesk_db.users u on lower(p.legacy_id) = lower(u.userid)
left join default_agency da on p.uid = da.person_id 