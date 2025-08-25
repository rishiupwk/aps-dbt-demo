-- Directory - organization ownership and rid-nid-uid mappings
-- Target table: watson.directory_mapping_dbt

with clean_persons_organizations as (
    select *
    from SHASTA_SDC_UPWORK.directory.persons_organizations
    where is_owner = 1
    qualify row_number() over(partition by org_uid order by act_status asc nulls last, created_on desc nulls last) = 1
)

select
    p.uid as person_id,
    lower(p.legacy_id) as username,
    u."Record_ID#"::bigint as user_id,
    p.legacy_id as developer_pii_id, -- this is not developer_pii_id, it's username, needs to be corrected
    p.legacy_ref::bigint as freelancer_id,
    o.uid as agora_team_id,
    lower(o.legacy_id) as team_username,
    o.legacy_ref::bigint as team_id,
    o.parent_uid::bigint as agora_parent_team_id,
    o.top_level_org_uid::bigint as agora_top_level_team_id,
    a."Record_ID#" as agency_team_id,
    o.created_on::timestamp_ntz(9) as account_creation_ts,
    o.updated_on::timestamp_ntz(9) as account_modified_ts,
    case
        when o.type = 1 and o.legacy_type = 1 then 'Client'
        when o.type = 1 and o.legacy_type = 2 then 'Agency'
        when o.type = 2 and o.legacy_type = 2 then 'Freelancer'
    end as account_type,
    not o.is_active::boolean as is_account_closed,
    o.name AS team_name,
    o.description AS team_description,
    substring(o.city,1,255) AS team_city,
    o.state AS team_state,
    o.country AS team_country,
    substring(o.zip,1,255) AS team_zip,
    o.timezone_name AS team_timezone,
    o.website AS team_url,
    case when po.act_status = 1 then true when po.act_status = 2 then false else null end is_owner_active,
    md5(array_to_string(array_construct(o.country,o.state,o.city),'~!~')) as org_geo_hash,
    row_number() over(partition by person_id, account_type order by is_owner_active::int desc nulls last, o.created_on desc nulls last) = 1 as is_latest_org
from SHASTA_SDC_UPWORK.directory.organizations o
/* some orgs don't have an owner, we still want them added, thus left joins */
left join clean_persons_organizations po on o.uid = po.org_uid
left join SHASTA_SDC_UPWORK.directory.persons p on p.uid = po.prs_uid
left join SHASTA_SDC_UPWORK.odb2_odesk_db.users u on p.legacy_id = lower(u.userid)
left join SHASTA_SDC_UPWORK.odb2_odesk_db.agencies a on a.related_company::BIGINT = o.legacy_ref::bigint
where o.legacy_ref IS DISTINCT FROM 0 