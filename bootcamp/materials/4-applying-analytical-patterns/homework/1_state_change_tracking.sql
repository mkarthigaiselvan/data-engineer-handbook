SELECT
  player_name,
  current_season,
  LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_is_active,
  is_active,
  CASE
    WHEN LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) IS NULL
         AND is_active THEN 'New'
    WHEN LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) = TRUE
         AND is_active = FALSE THEN 'Retired'
    WHEN LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) = FALSE
         AND is_active = TRUE THEN 'Returned from Retirement'
    WHEN LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) = TRUE
         AND is_active = TRUE THEN 'Continued Playing'
    WHEN LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) = FALSE
         AND is_active = FALSE THEN 'Stayed Retired'
  END AS state_change
FROM players_scd_table
ORDER BY player_name, current_season;
