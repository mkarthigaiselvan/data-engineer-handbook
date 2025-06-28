-- Create SCD Type 2 tracking table
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
