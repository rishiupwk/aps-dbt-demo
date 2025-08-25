-- User login dates from UA (User Activity) stories
-- Target table: watson.temp_ua_login_date_dbt

SELECT
    uu.id AS user_id,
    uu.username AS username,
    MIN(s.created_ts) as first_login_ts,
    MAX(CASE WHEN  template_id::int = 2 THEN s.created_ts ELSE NULL end ) as last_login_ts,
    CASE WHEN MAX(CASE WHEN template_id::int = 130 THEN 1 ELSE 0 end) = 0 THEN FALSE ELSE TRUE end AS is_not_fls
FROM SHASTA_SDC_UPWORK.odesk_ua_ua.ua_stories s
INNER JOIN SHASTA_SDC_UPWORK.odesk_ua_ua.ua_requests r 
    ON coalesce(s.request_id::BIGINT,s.request_id1::BIGINT) = coalesce(r.id::BIGINT,r.id1::BIGINT)
INNER JOIN SHASTA_SDC_UPWORK.odesk_ua_ua.ua_users uu 
    ON uu.id::BIGINT = s.user_id::BIGINT
WHERE s.template_id::int in (2, 130)
    AND r.success
GROUP BY 1,2 