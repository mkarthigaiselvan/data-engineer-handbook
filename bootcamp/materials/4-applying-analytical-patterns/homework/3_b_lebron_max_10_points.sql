WITH lebron_games AS (
  SELECT
    game_id,
    player_name,
    pts,
    CASE WHEN pts >= 10 THEN 1 ELSE 0 END AS scored_10_plus,
    ROW_NUMBER() OVER (ORDER BY game_id) AS row_num,
    SUM(CASE WHEN pts >= 10 THEN 0 ELSE 1 END)
      OVER (ORDER BY game_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS reset_group
  FROM game_details
  WHERE player_name = 'LeBron James'
),
streaks AS (
  SELECT
    reset_group,
    COUNT(*) AS streak_length
  FROM lebron_games
  WHERE scored_10_plus = 1
  GROUP BY reset_group
)
SELECT MAX(streak_length) AS max_10_point_streak FROM streaks;