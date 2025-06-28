# Dimensional Data Modeling - Week 1

This week's assignment involves working with the `actor_films` dataset. Your task is to construct a series of SQL queries and table definitions that will allow us to model the actor_films dataset in a way that facilitates efficient analysis. This involves creating new tables, defining data types, and writing queries to populate these tables with data from the actor_films dataset

## Dataset Overview
The `actor_films` dataset contains the following fields:

- `actor`: The name of the actor.
- `actorid`: A unique identifier for each actor.
- `film`: The name of the film.
- `year`: The year the film was released.
- `votes`: The number of votes the film received.
- `rating`: The rating of the film.
- `filmid`: A unique identifier for each film.

The primary key for this dataset is (`actor_id`, `film_id`).

## Assignment Tasks

1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
    - `films`: An array of `struct` with the following fields:
		- film: The name of the film.
		- votes: The number of votes the film received.
		- rating: The rating of the film.
		- filmid: A unique identifier for each film.

    - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
		- `star`: Average rating > 8.
		- `good`: Average rating > 7 and ≤ 8.
		- `average`: Average rating > 6 and ≤ 7.
		- `bad`: Average rating ≤ 6.
    - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).

CREATE TYPE film_type AS (
  film TEXT,
  year INTEGER,
  votes INTEGER,
  rating FLOAT,
  filmid TEXT
);

CREATE TYPE quality_class AS ENUM('star','good','average', 'bad');
CREATE TABLE actors(
actorid TEXT,
actor TEXT,
year INTEGER,
films film_type[],
quality_class quality_class,
is_active BOOLEAN,
PRIMARY KEY(actorid, year)
);

2. **Cumulative table generation query:** Write a query that populates the `actors` table one year at a time.
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

3. **DDL for `actors_history_scd` table:** Create a DDL for an `actors_history_scd` table with the following features:
    - Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
    - Tracks `quality_class` and `is_active` status for each actor in the `actors` table.
CREATE TABLE actors_history_scd (
  actorid TEXT,
  actor TEXT,
  quality_class quality_class,
  is_active BOOLEAN,
  start_date DATE,
  end_date DATE,
  is_current BOOLEAN DEFAULT TRUE,
  PRIMARY KEY (actorid, start_date)
);
      
4. **Backfill query for `actors_history_scd`:** Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
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
   
5. **Incremental query for `actors_history_scd`:** Write an "incremental" query that combines the previous year's SCD data with new incoming data from the `actors` table.
-- Assume current_year = 1973
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

