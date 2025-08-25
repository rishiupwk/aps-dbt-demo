-- Client quality stage model combining FJP LTV and CLTVv2 data
-- Target table: watson.client_quality_stage_dbt

select
    s.*,
    row_number() over (
        partition by client_key 
        order by is_fjp desc, post_date, src_table_name
    ) as row_rank
from (
    select
        c.client_id::bigint as client_id,
        c.agora_company_id,
        m.opening::bigint as post_id,
        p.agora_post_id::bigint as agora_post_id,
        qs.quality_value as projected_client_quality_value,
        qs.quality_segment_name as projected_client_quality_segment,
        c.client_key as client_key,
        p.post_key as post_key,
        p.post_date as post_date,
        p.is_fjp as is_fjp,
        'match.fjp_ltv' as src_table_name
    from SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.match_fjp_ltv m
    inner join SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.post_dim p 
        on m.opening::bigint = p.post_id::bigint
    inner join SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.client_dim c 
        on m.employer::bigint = c.client_id::bigint 
        and c.grain_name = 'client'
    inner join SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.quality_segment_dim qs 
        on m.pred_label = qs.quality_value 
        and qs.recipient_table_name = 'client_dim' 
        and qs.recipient_column_name = 'projected_client_quality_segment'
        
    union all
    
    select
        c.client_id::bigint as client_id,
        c.agora_company_id,
        m.opening::bigint as post_id,
        m.opening_uid::bigint as agora_post_id,
        qs.quality_value as projected_client_quality_value,
        qs.quality_segment_name as projected_client_quality_segment,
        c.client_key as client_key,
        p.post_key as post_key,
        p.post_date as post_date,
        p.is_fjp as is_fjp,
        'cltvv2.openings' as src_table_name
    from SHASTA_SDC_UPWORK.cltvv2.openings m
    inner join SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.post_dim p 
        on m.opening::bigint = p.post_id::bigint
    inner join SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.client_dim c 
        on p.agora_company_id::bigint = c.agora_company_id::bigint 
        and c.grain_name = 'client'
    inner join SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.sherlock.quality_segment_dim qs 
        on m.dollar_amount between qs.min_value and qs.max_value  
        and qs.recipient_table_name = 'client_dim' 
        and qs.recipient_column_name = 'projected_client_quality_segment'
) s 