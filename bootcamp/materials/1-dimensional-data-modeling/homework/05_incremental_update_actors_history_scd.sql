-- Step 1: Update current SCD rows that have changed
WITH new_data AS (
  SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    MAKE_DATE(year, 1, 1) AS start_date
  FROM actors
  WHERE year = 1973
)

UPDATE actors_history_scd scd
SET end_date = nd.start_date - INTERVAL '1 day',
    is_current = FALSE
FROM new_data nd
WHERE scd.actorid = nd.actorid
  AND scd.is_current = TRUE
  AND (
    scd.quality_class IS DISTINCT FROM nd.quality_class OR
    scd.is_active IS DISTINCT FROM nd.is_active
);

-- Step 2: Insert new SCD rows
INSERT INTO actors_history_scd (
  actorid,
  actor,
  quality_class,
  is_active,
  start_date,
  end_date,
  is_current
)
SELECT
  actorid,
  actor,
  quality_class,
  is_active,
  MAKE_DATE(year, 1, 1),
  '9999-12-31'::DATE,
  TRUE
FROM actors
WHERE year = 1973;
