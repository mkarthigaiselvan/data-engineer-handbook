WITH browser_date_list AS (
  SELECT
    e.user_id,
    d.browser_type,
    array_agg(DISTINCT DATE(e.event_time) ORDER BY DATE(e.event_time)) AS active_dates
  FROM events e
  JOIN devices d ON e.device_id = d.device_id
   WHERE e.user_id IS NOT NULL
  GROUP BY e.user_id, d.browser_type

)

INSERT INTO user_devices_cumulated (user_id, device_activity_datelist)
SELECT
  user_id,
  jsonb_object_agg(browser_type, to_jsonb(active_dates)) AS device_activity_datelist
FROM browser_date_list
GROUP BY user_id;
