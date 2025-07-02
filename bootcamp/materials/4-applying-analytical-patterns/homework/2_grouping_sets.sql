SELECT
  gd.player_name,
  gd.team_abbreviation,
  g.season,
  SUM(COALESCE(gd.pts, 0)) AS total_points,
  CASE
    WHEN GROUPING(gd.player_name) = 0 AND GROUPING(gd.team_abbreviation) = 0 AND GROUPING(g.season) = 1 THEN 'Player-Team'
    WHEN GROUPING(gd.player_name) = 0 AND GROUPING(g.season) = 0 AND GROUPING(gd.team_abbreviation) = 1 THEN 'Player-Season'
    WHEN GROUPING(gd.player_name) = 1 AND GROUPING(gd.team_abbreviation) = 0 AND GROUPING(g.season) = 1 THEN 'Team Only'
  END AS grouping_level
FROM game_details gd
JOIN games g ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
  (gd.player_name, gd.team_abbreviation),
  (gd.player_name, g.season),
  (gd.team_abbreviation)
)
ORDER BY total_points DESC;
