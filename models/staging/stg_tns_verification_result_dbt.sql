-- Client and freelancer verifications from TNS
-- Target table: watson.stg_tns_verification_result_dbt

SELECT
    nid,
    max(case when (verification_type = 'REALNESS' and verification_status = 'PASSED') then verification_result_ts else null end)::timestamp_ntz(0) as realness_verification_ts,
    max(case when (verification_type = 'LIVENESS' and verification_status = 'PASSED') then verification_result_ts else null end)::timestamp_ntz(0) as liveness_verification_ts,
    max(case when (verification_type = 'ID_BADGE' and verification_status = 'PASSED') then verification_result_ts else null end)::timestamp_ntz(0) as id_badge_verification_ts,
    max(case when (verification_type = 'PARTIAL_BADGE' and verification_status = 'PASSED') then verification_result_ts else null end)::timestamp_ntz(0) as partial_badge_verification_ts,
    max(case when (verification_type = 'IDV_NO_BADGE' and verification_status = 'PASSED') then verification_result_ts else null end)::timestamp_ntz(0) as idv_no_badge_verification_ts
from SHASTA_SDC_UPWORK.tns_datamart.tns_verification_result
group by all 