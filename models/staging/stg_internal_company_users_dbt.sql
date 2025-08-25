-- Internal company users and usernames based on Account Executive data
-- Target table: watson.stg_internal_company_users_dbt

with unpivot as (
    select 'company' as grain_name
    union all
    select 'user'
)

select
    u.grain_name,
    case u.grain_name when 'company' then a.ae_root_team_rid end::bigint as client_id,
    case u.grain_name when 'user' then a.ae_user_nid end::varchar as username
from SHASTA_SDC_UPWORK.odesk_bpa_odesk_bp.aes a
join SHASTA_SDC_UPWORK.finance_apps.xxeo_pf_lkp_internal_ae_v i 
    on a.ae_uid::varchar = i.accounting_entity_number::varchar
join unpivot u on 1 = 1
where i.entity_type_flag = 'I'
    and coalesce(a.ae_root_team_rid::varchar, a.ae_user_nid) is not null
group by all 