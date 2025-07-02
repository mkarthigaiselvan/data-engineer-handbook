WITH team_game_results AS (
  SELECT
    season,
    game_date_est,
    team_id_home AS team_id,
    game_id,
    home_team_wins AS is_win,
    ROW_NUMBER() OVER (PARTITION BY team_id_home ORDER BY game_date_est) AS rn
  FROM games
),
rolling_90_window AS (
  SELECT
    a.team_id,
    a.rn,
    a.game_date_est,
    SUM(b.is_win) AS win_count
  FROM team_game_results a
  JOIN team_game_results b
    ON a.team_id = b.team_id
   AND b.rn BETWEEN a.rn AND a.rn + 89
  GROUP BY a.team_id, a.rn, a.game_date_est
)
SELECT
  team_id,
  MAX(win_count) AS max_wins_in_90_games
FROM rolling_90_window
GROUP BY team_id
ORDER BY max_wins_in_90_games DESC;
