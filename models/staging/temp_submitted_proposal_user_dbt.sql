-- Teams proposals submitted proposal user mapping
-- Target table: watson.temp_submitted_proposal_user_dbt

SELECT
    draft_proposal_uid,
    job_application_uid
FROM (
    SELECT
        draft_proposal_uid,
        job_application_uid,
        ROW_NUMBER() OVER (PARTITION BY draft_proposal_uid, job_application_uid ORDER BY created_at desc) as rnk
    FROM SHASTA_SDC_UPWORK.teams_proposals.submitted_proposal_user
) subqry
WHERE rnk = 1 