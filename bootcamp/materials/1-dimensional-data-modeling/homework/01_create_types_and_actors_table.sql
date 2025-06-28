-- Create custom types
CREATE TYPE film_type AS (
  film TEXT,
  year INTEGER,
  votes INTEGER,
  rating FLOAT,
  filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- Create main actors table
CREATE TABLE actors (
  actorid TEXT,
  actor TEXT,
  year INTEGER,
  films film_type[],
  quality_class quality_class,
  is_active BOOLEAN,
  PRIMARY KEY (actorid, year)
);
