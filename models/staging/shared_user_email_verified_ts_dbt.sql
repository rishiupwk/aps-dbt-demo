-- User email verification timestamps from registration events
-- Target table: watson.shared_user_email_verified_ts_dbt

SELECT
    LOWER(r.username) AS username,
    MIN(e.time_stamp)::timestamp AS email_verified_ts
FROM SHASTA_SDC_UPWORK.registration.registrations r
INNER JOIN SHASTA_SDC_UPWORK.registration.registration_events e 
    ON r.id::BIGINT = e.registration_id::BIGINT
WHERE e.event_type IN (1,6)
GROUP BY 1 