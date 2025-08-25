{{ config(materialized='table') }}

-- Simple passthrough model for project dimension data
-- Target table: watson.stg_project_dim_dbt

select *
from {{ source('catalogdb', 'project') }} 