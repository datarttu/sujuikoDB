/*
 * Creating a general view that expands trip templates into individual trips.
 */
\timing on
BEGIN;

CREATE VIEW test_trips_view AS (
WITH
 unnest_dates AS (
   SELECT
     ttid,
     route,
     dir,
     start_times,
     unnest(dates)  AS service_date
   FROM sched.trip_templates
 ),
 unnest_start_times AS (
   SELECT
     ttid,
     route,
     dir,
     service_date,
     unnest(start_times) AS start_time
   FROM unnest_dates
 )
SELECT *
FROM unnest_start_times
ORDER BY route, dir, service_date, start_time
);

SELECT * FROM test_trips_view LIMIT 5;

SELECT 'Total trips:' AS title, count(*) FROM test_trips_view;

WITH trips_per_date AS (
  SELECT count(*) AS cnt, service_date
  FROM test_trips_view
  GROUP BY service_date
)
SELECT avg(cnt), min(cnt), max(cnt)
FROM trips_per_date;

SELECT * FROM test_trips_view
WHERE
  route LIKE '4571%'
  AND extract(isodow FROM service_date) IN (2, 3, 4)
  AND start_time BETWEEN make_interval(hours => 9) AND make_interval(hours => 12)
LIMIT 20;

SELECT count(*) FROM test_trips_view
WHERE
  route LIKE '4571%'
  AND extract(isodow FROM service_date) IN (2, 3, 4)
  AND start_time BETWEEN make_interval(hours => 9) AND make_interval(hours => 12);

SELECT count(*)
FROM test_trips_view
INNER JOIN sched.segments
USING (ttid)
WHERE
  route LIKE '4571%'
  AND extract(isodow FROM service_date) IN (2, 3, 4)
  AND start_time BETWEEN make_interval(hours => 9) AND make_interval(hours => 12);

ROLLBACK;
\timing off
