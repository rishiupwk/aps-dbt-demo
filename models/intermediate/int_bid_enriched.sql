{{ config(materialized='view') }}

with bid_base as (
    select * from {{ ref('stg_bid_pricing') }}
),

enriched_bids as (
    select
        -- Original bid data
        b.post_id,
        b.agora_post_id,
        b.freelancer_id,
        
        -- Dimension keys (using available sources only)
        coalesce(p.post_key, 0) as post_key,
        0 as post_history_key,  -- Simplified for POC
        coalesce(cd.client_key, 0) as client_key,
        0 as freelancer_agency_key,  -- Simplified for POC
        coalesce(f.freelancer_key, 0) as freelancer_key,
        0 as freelancer_history_key,  -- Simplified for POC
        0 as engagement_type_key,  -- Simplified for POC
        0 as engagement_duration_key,  -- Simplified for POC
        0 as job_type_key,  -- Simplified for POC
        0 as bid_status_key,  -- Simplified for POC
        0 as ats_status_key,  -- Simplified for POC
        0 as ar_accounting_entity_key,  -- Simplified for POC
        0 as ap_accounting_entity_key,  -- Simplified for POC
        0 as hide_status_key,  -- Simplified for POC
        
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
        0 as src_create_user_id,  -- Simplified for POC
        b.src_create_ts,
        0 as src_modified_user_id,  -- Simplified for POC
        greatest(
            coalesce(b.src_modified_ts, '1900-01-01 00:00:00'), 
            coalesce(b.src_create_ts, '1900-01-01 00:00:00')
        ) as src_modified_ts,
        
        -- Connects usage
        coalesce(c.connects_used, 0) as connects_used,
        coalesce(c.connects_amount, 0) as connects_amount,
        
        -- Boosted bid details  
        coalesce(b.boosted_amount, 0) as boosted_bid_amount,
        case when b.boosted_amount > 0 then true else false end as is_boosted,
        
        -- Business classifications
        case 
            when b.applying_as in ('agency', 'team') then 'Agency'
            when b.applying_as = 'freelancer' then 'IC'
            else 'unknown'
        end as ic_agency_eor,
        
        -- First bid flags
        false as is_first_post_bid,  -- Simplified for POC
        false as is_first_freelancer_bid,  -- Simplified for POC
        
        -- Response tracking
        null::timestamp_ntz as client_first_response_ts,  -- Simplified for POC
        
        -- Qualification flag
        case 
            when coalesce(b.amount, b.hourly_rate) > 0 
            and b.src_create_ts is not null
            then true 
            else false 
        end as is_qualified_bid,
        
        -- Data warehouse metadata
        true as dw_active_flag,
        current_timestamp() as dw_create_ts,
        current_timestamp() as dw_modified_ts
        
    from bid_base b
    
    -- Join with available dimension tables only
    left join {{ source('sherlock', 'post_dim') }} p
        on b.agora_post_id = p.agora_post_id
        and p.dw_active_flag = true
        
    left join {{ source('sherlock', 'client_dim') }} cd
        on b.agora_company_id = cd.agora_company_id
        and cd.dw_active_flag = true
        
    left join {{ source('sherlock', 'freelancer_dim') }} f
        on b.freelancer_id = f.freelancer_id
        and f.dw_active_flag = true
),

final as (
    select
        *,
        -- Generate MD5 hash for change detection
        md5(
            coalesce(post_key::string, '') || '|' ||
            coalesce(client_key::string, '') || '|' ||
            coalesce(freelancer_key::string, '') || '|' ||
            coalesce(bid_amount::string, '') || '|' ||
            coalesce(hourly_rate::string, '') || '|' ||
            coalesce(is_recommended::string, '') || '|' ||
            coalesce(is_shortlisted::string, '') || '|' ||
            coalesce(connects_used::string, '') || '|' ||
            coalesce(boosted_bid_amount::string, '') || '|' ||
            coalesce(src_modified_ts::string, '')
        ) as dw_md5_hash
        
    from enriched_bids
)

select * from final 