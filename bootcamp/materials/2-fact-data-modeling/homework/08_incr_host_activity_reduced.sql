WITH input_day AS (
  SELECT DATE '2023-01-03' AS day_to_process
),

daily_stats AS (
  SELECT
    DATE_TRUNC('month', e.event_time::timestamp)::DATE AS month,
    e.host,
    EXTRACT(DAY FROM e.event_time::timestamp)::INT AS day_index,
    COUNT(*) AS hits,
    COUNT(DISTINCT e.user_id) AS unique_visitors
  FROM events e, input_day i
  WHERE DATE(e.event_time::timestamp) = i.day_to_process
  GROUP BY 1, 2, 3
),

existing AS (
  SELECT h.*
  FROM host_activity_reduced h
  WHERE h.month = DATE '2023-01-03' - INTERVAL '1 day' * (EXTRACT(DAY FROM DATE '2023-01-03')::INT - 1)
),

merged AS (
  SELECT
    COALESCE(e.month, d.month) AS month,
    COALESCE(e.host, d.host) AS host,
    
    -- Merge hit_array
    CASE
      WHEN e.hit_array IS NOT NULL THEN
        CASE
          WHEN array_length(e.hit_array, 1) >= d.day_index THEN
            -- Replace value at position
            array_cat(
              COALESCE(e.hit_array[1:d.day_index-1], '{}'),
              ARRAY[d.hits]
            ) || COALESCE(e.hit_array[d.day_index+1:], '{}')
          ELSE
            -- Pad and append
            e.hit_array || array_fill(0, ARRAY[d.day_index - array_length(e.hit_array, 1) - 1]) || ARRAY[d.hits]
        END
      ELSE
        array_fill(0, ARRAY[d.day_index - 1]) || ARRAY[d.hits]
    END AS hit_array,

    -- Merge unique_visitors_array
    CASE
      WHEN e.unique_visitors_array IS NOT NULL THEN
        CASE
          WHEN array_length(e.unique_visitors_array, 1) >= d.day_index THEN
            array_cat(
              COALESCE(e.unique_visitors_array[1:d.day_index-1], '{}'),
              ARRAY[d.unique_visitors]
            ) || COALESCE(e.unique_visitors_array[d.day_index+1:], '{}')
          ELSE
            e.unique_visitors_array || array_fill(0, ARRAY[d.day_index - array_length(e.unique_visitors_array, 1) - 1]) || ARRAY[d.unique_visitors]
        END
      ELSE
        array_fill(0, ARRAY[d.day_index - 1]) || ARRAY[d.unique_visitors]
    END AS unique_visitors_array
  FROM daily_stats d
  FULL OUTER JOIN existing e
    ON e.host = d.host AND e.month = d.month
),

deleted AS (
  DELETE FROM host_activity_reduced
  WHERE (month, host) IN (SELECT month, host FROM merged)
)

INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors_array)
SELECT month, host, hit_array, unique_visitors_array
FROM merged;
