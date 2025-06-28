  WITH unnested AS(
 SELECT player_name,
         UNNEST(seasons)::season_stats AS season_stats
  FROM players
  WHERE current_season = 2001
  AND player_name = 'Michael Jordan'
  ) 
  SELECT 
  player_name,
  (season_stats[1]::season_stats).*
FROM unnested   