-- Replace 1971 and 1972 with desired years
WITH last_year_data AS (
  SELECT 
    actorid,
    actor,
    films,
    quality_class,
    is_active
  FROM actors
  WHERE year = 1971
),

this_year_films AS (
  SELECT
    actorid,
    actor,
    film,
    year,
    votes,
    rating,
    filmid
  FROM actor_films
  WHERE year = 1972
),

this_year_grouped AS (
  SELECT
    actorid,
    year,
    MAX(actor) AS actor,
    ARRAY_AGG(ROW(film, year, votes, rating, filmid)::film_type) AS new_films,
    AVG(rating) AS avg_rating
  FROM this_year_films
  GROUP BY actorid, year
)

INSERT INTO actors
SELECT
  COALESCE(ly.actorid, ty.actorid) AS actorid,
  COALESCE(ly.actor, ty.actor) AS actor,
  1972 AS year,
  COALESCE(ly.films, ARRAY[]::film_type[]) || COALESCE(ty.new_films, ARRAY[]::film_type[]) AS films,
  CASE 
    WHEN ty.avg_rating IS NOT NULL THEN
      CASE 
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 THEN 'good'
        WHEN ty.avg_rating > 6 THEN 'average'
        ELSE 'bad'
      END::quality_class
    ELSE ly.quality_class
  END AS quality_class,
  (ty.actorid IS NOT NULL) AS is_active
FROM last_year_data ly
FULL OUTER JOIN this_year_grouped ty ON ly.actorid = ty.actorid;
