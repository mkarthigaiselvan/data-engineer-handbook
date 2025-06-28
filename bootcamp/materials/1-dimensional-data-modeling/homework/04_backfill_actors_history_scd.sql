-- Backfill entire SCD history from existing actors table
WITH base_data AS (
  SELECT 
    actorid,
    actor,
    quality_class,
    is_active,
    year,
    LEAD(year) OVER (PARTITION BY actorid ORDER BY year) AS next_year
  FROM actors
),

formatted_data AS (
  SELECT 
    actorid,
    actor,
    quality_class,
    is_active,
    MAKE_DATE(year, 1, 1) AS start_date,
    COALESCE(MAKE_DATE(next_year, 1, 1) - INTERVAL '1 day', '9999-12-31'::DATE) AS end_date,
    next_year IS NULL AS is_current
  FROM base_data
)

INSERT INTO actors_history_scd
SELECT * FROM formatted_data;
