WITH yesterday AS (
    SELECT * FROM hosts_cumulated_daily
    WHERE snapshot_date = DATE '2023-01-30'
),

today AS (
    SELECT
        host,
        DATE(event_time) AS today_date
    FROM events
    WHERE DATE(event_time) = DATE '2023-01-31'
    GROUP BY host, DATE(event_time)
)
INSERT INTO hosts_cumulated_daily (host, host_activity_datelist, snapshot_date)
SELECT
    COALESCE(t.host, y.host) AS host,
    COALESCE(y.host_activity_datelist, ARRAY[]::DATE[])
    ||
    CASE
        WHEN t.host IS NOT NULL THEN ARRAY[t.today_date]
        ELSE ARRAY[]::DATE[]
    END AS host_activity_datelist,
    COALESCE(t.today_date, y.snapshot_date + INTERVAL '1 day')::DATE AS snapshot_date
FROM yesterday y
FULL OUTER JOIN today t ON t.host = y.host;
