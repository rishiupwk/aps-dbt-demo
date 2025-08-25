-- Simple passthrough model for legacy ODesk agencies
-- Target table: watson.stg_odeskdb_agencies_dbt

select * 
from SHASTA_SDC_UPWORK.odb2_odesk_db.agencies 