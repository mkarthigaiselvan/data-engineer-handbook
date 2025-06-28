CREATE TABLE host_activity_reduced (
    month DATE,                            -- First day of the month
    host TEXT,
    hit_array INTEGER[],                   -- hit count per day (1–31)
    unique_visitors_array INTEGER[]        -- distinct users per day (1–31)
);
