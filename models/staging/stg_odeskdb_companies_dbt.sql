-- Simple passthrough model for legacy ODesk companies
-- Target table: watson.stg_odeskdb_companies_dbt

select * 
from SHASTA_SDC_UPWORK.odb2_odesk_db.companies 