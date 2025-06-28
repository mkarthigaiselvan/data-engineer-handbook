ALTER TABLE user_devices_cumulated ADD COLUMN datelist_int INTEGER[];
UPDATE user_devices_cumulated
SET datelist_int = (
  SELECT array_agg(DISTINCT TO_CHAR(date_elem::date, 'YYYYMMDD')::INT ORDER BY TO_CHAR(date_elem::date, 'YYYYMMDD')::INT)
  FROM jsonb_each(device_activity_datelist) AS browser_dates(browser, date_array),
       jsonb_array_elements_text(date_array) AS date_elems(date_elem)
);