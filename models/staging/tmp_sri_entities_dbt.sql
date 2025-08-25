{{ config(materialized='table') }}

-- Temporary table with all Scheduled Rate Increase (SRI) data
-- Target table: watson.tmp_sri_entities_dbt

select
    domain,
    domain_id::bigint as domain_id,
    cadence_months,
    rate
from {{ source('scheduled_rate_increases', 'planned_rate_increases') }}
where cadence_months > 0
    and not deleted
    and try_cast(domain_id as bigint) is not null 