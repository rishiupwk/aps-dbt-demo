{{ config(materialized='table') }}

-- Simple passthrough model for legacy ODesk companies
-- Target table: watson.stg_odeskdb_companies_dbt

select * 
from {{ source('odb2_odesk_db', 'companies') }} 