-- Simple passthrough model for contracts hourly limit terms
-- Target table: watson.stg_contracts_hourly_limit_terms_dbt

select * 
from SHASTA_SDC_UPWORK.contracts.hourly_limit_terms 