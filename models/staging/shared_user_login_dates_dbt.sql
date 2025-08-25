-- Combined user login dates from all sources
-- Target table: watson.shared_user_login_dates_dbt

SELECT
    username,
    MIN(first_login_ts) AS first_login_ts,
    MAX(last_login_ts) AS last_login_ts
FROM (
    SELECT
        username,
        first_login_ts,
        last_login_ts
    FROM SBX_RISHISRIVASTAVA_DM13471_SHASTA_SDC_DPS.watson.temp_ua_login_date_dbt
    WHERE first_login_ts IS NOT NULL
) x
GROUP BY username 