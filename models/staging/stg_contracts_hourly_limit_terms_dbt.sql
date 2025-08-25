{{ config(materialized='table') }}

-- Simple passthrough model for contracts hourly limit terms
-- Target table: watson.stg_contracts_hourly_limit_terms_dbt

select * 
from {{ source('contracts', 'hourly_limit_terms') }} 