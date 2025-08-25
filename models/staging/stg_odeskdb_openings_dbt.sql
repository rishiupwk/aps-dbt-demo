-- Complex staging model for job openings with qualifications, featured job data, and location enrichment
-- Target table: watson.stg_odeskdb_openings_dbt

with qualifications as (
    select
        qual.*,
        parse_json(qual.locations)[0][0] as area_type,
        parse_json(qual.locations)[0][1] as area_id,
        parse_json(qual.locations)[0][2] as area_name
    from SHASTA_SDC_UPWORK.openings.qualifications qual
),

jpgv2_posts as (
    select
        opening_uid
    from SHASTA_SDC_UPWORK.openings.annotations_custom_fields
    where name ilike 'optInDescriptionAIv2'
        and value ilike 'yes'
    group by 1
),

ad_price as (
    select 
        job_post_uid, 
        price::number(34,8) featured_job_price
    from SHASTA_SDC_UPWORK.client_ads.ad_price_calculation
    qualify row_number() over(partition by job_post_uid order by calculation_ts desc nulls last) = 1
),

featured_jobs as (
    select 
        a.job_post_uid,
        min(ash.created_ts)::timestamp_ntz(9) as job_featured_ts
    from SHASTA_SDC_UPWORK.client_ads.ad_status_history ash
    inner join SHASTA_SDC_UPWORK.client_ads.ad a
        on ash.ad_id = a.id
    where ash.status = 'ACTIVE'
    group by a.job_post_uid
)

select distinct
    o.*,
    astatus.assignment_status,
    nullif(amount,'')::numeric(38,2) as derived_amount,
    nullif(max_amount,'')::numeric(38,2) as derived_max_amount,
    initcap(o.status) as derived_status,
    case o.type 
        when 'FIXED_PRICE' then 'Fixed' 
        when 'HOURLY' then 'Hourly' 
    end as derived_type,
    case o.contractor_tier 
        when 'ENTRY_LEVEL' then 1 
        when 'INTERMEDIATE' then 2 
        when 'EXPERT' then 3 
    end as derived_contractor_tier,
    case o.engagement_type 
        when 'FULL_TIME' then 1 
        when 'PART_TIME' then 2 
        when 'AS_NEEDED' then 4 
        else 0 
    end as derived_engagement_type,
    case q.english_proficiency 
        when 'BASIC' then 1 
        when 'CONVERSATIONAL' then 2 
        when 'FLUENT' then 3 
        when 'NATIVE' then 4 
        else 0 
    end derived_english_proficiency,
    q.onsite_type as pref_onsite_type,
    coalesce(pc.latitude, a.latitude, c.latitude) as job_location_latitude,
    coalesce(pc.longitude, a.longitude, c.longitude) as job_location_longitude,
    cm.metro_code as job_postal_metro_code,
    jai.opening_uid is not null is_ai_generated_post,
    apc.featured_job_price,
    fj.job_featured_ts,
    coalesce(oe.direct_hire, false) as is_direct_hire
from SHASTA_SDC_UPWORK.openings.openings o
left join qualifications q on o.uid::bigint = q.opening_uid::bigint
left join jpgv2_posts jai on o.uid::bigint = jai.opening_uid::bigint
left join SHASTA_SDC_UPWORK.openings_extra.openings_extra oe on o.uid = oe.uid
left join (
    select 
        opening_uid, 
        max(status) as assignment_status
    from SHASTA_SDC_UPWORK.contracts.contracts 
    where coalesce(delivery_model,'N/A') <> 'MICRO_PAYMENTS'
    group by 1
) astatus on astatus.opening_uid::bigint = o.uid::bigint
left join SHASTA_SDC_UPWORK.geods.postal_code pc 
    on q.area_id::bigint = pc.id::bigint 
    and q.area_type = 'POSTAL_CODE' 
    and nullif(q.locations,'[]') is not null
left join SHASTA_SDC_UPWORK.geods.area a 
    on q.area_id::bigint = a.id::bigint 
    and q.area_type = 'AREA' 
    and nullif(q.locations,'[]') is not null
left join SHASTA_SDC_UPWORK.geods.city c 
    on q.area_id::bigint = c.id::bigint 
    and q.area_type = 'CITY' 
    and nullif(q.locations,'[]') is not null
left join SHASTA_SDC_UPWORK.geods.city cm 
    on pc.city_id::bigint = cm.id::bigint 
    and q.area_type = 'POSTAL_CODE' 
    and nullif(q.locations,'[]') is not null
left join ad_price apc 
    on o.is_premium = TRUE 
    and o.uid = apc.job_post_uid
left join featured_jobs fj 
    on o.is_premium = TRUE 
    and o.uid = fj.job_post_uid
where o.status in ('CANCELLED','FILLED','REQUESTED')
    and not (coalesce(o.site_source,'N/A') = 'micro_payments' and o.off_the_network) -- excluding ghost Project Triton posts
    and (astatus.opening_uid is not null
        or not (o.is_hidden and coalesce(o.site_source,'N/A') in ('direct_hire','fls-direct-contract'))
    ) 