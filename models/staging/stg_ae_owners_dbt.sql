-- AE owners - relationship between freelancer/team and accounting_entity_id
-- Target table: watson.stg_ae_owners_dbt

select 
    aeo.*,
    ae.ae_cost_center,
    -- subtype of the AE. records with empty aeo_ace_rid(ae_id) will be Unknown
    CASE 
        WHEN ae.ae_type = 'V' AND ae.ae_developer_rid IS NULL AND ae.ae_root_team_rid IS NOT NULL THEN 'Agency'
        WHEN ae.ae_type = 'V' AND ae.ae_developer_rid IS NOT NULL THEN 'Freelancer'
        WHEN ae.ae_type = 'C' THEN 'Client'
        ELSE 'Unknown' 
    end as accounting_entity_subtype,
    -- number of the row by person_uid
    row_number() over (
        partition by aeo.aeo_person_uid, accounting_entity_subtype
        order by coalesce(aeo.aeo_removed_ts,'9999-12-31'::timestamp)::timestamp desc,
                 aeo.aeo_rid desc
    ) as rn_fl,
    -- number of the row by org_uid(team)
    row_number() over (
        partition by aeo_org_uid
        order by coalesce(aeo.aeo_removed_ts,'9999-12-31'::timestamp)::timestamp desc,
                 aeo.aeo_rid desc
    ) as rn_team,
    -- when we join with this table by org_uid or person_uid we can use is_latest flag to have "latest" AE in case of duplicates
    CASE 
        WHEN accounting_entity_subtype = 'Freelancer' and rn_fl = 1 then TRUE
        WHEN accounting_entity_subtype in ('Client', 'Agency') and rn_team = 1 then TRUE
        ELSE FALSE 
    END as is_latest
from SHASTA_SDC_UPWORK.odesk_bpa_odesk_bp.ae_owners aeo
left join SHASTA_SDC_UPWORK.odesk_bpa_odesk_bp.aes ae 
    on ae.ae_uid = aeo.aeo_ace_rid 