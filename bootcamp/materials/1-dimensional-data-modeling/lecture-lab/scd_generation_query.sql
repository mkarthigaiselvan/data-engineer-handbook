
INSERT INTO players_scd_table
WITH streak_started AS (
    SELECT player_name,
           current_season,
           scoring_class,
           is_active,
           LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) <> scoring_class
               OR LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) IS NULL
               AS did_change
    FROM players
    where current_season <= 2021
),
     streak_identified AS (
         SELECT
            player_name,
                scoring_class,
                current_season,
                is_active,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            player_name,
            is_active,
            scoring_class,
            MIN(current_season) AS start_date,
            MAX(current_season) AS end_date,
            2021 as current_season
         FROM streak_identified
         GROUP BY player_name,streak_identifier,is_active,scoring_class
     )
     SELECT player_name, scoring_class, is_active, start_date, end_date, current_season
     FROM aggregated