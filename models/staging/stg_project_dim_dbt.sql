-- Simple passthrough model for project dimension data
-- Target table: watson.stg_project_dim_dbt

select *
from SHASTA_SDC_UPWORK.catalogdb.project 