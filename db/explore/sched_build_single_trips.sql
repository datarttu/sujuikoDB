/*
 * Test how to build a single trips from trip templates.
 */

\set routesel '1039'
\set dirsel 1
\set datesel '2019-11-17'
\set time_1 '08:00:00'
\set time_2 '10:30:00'

WITH
  unnest_dates AS (
    SELECT
      ttid,
      route,
      dir,
      start_times,
      unnest(dates)  AS op_date
    FROM sched.trip_templates
    WHERE route = :'routesel'
      AND dir = :dirsel
  ),
  unnest_start_times AS (
    SELECT
      ttid,
      route,
      dir,
      op_date,
      unnest(start_times) AS start_time
    FROM unnest_dates
    WHERE op_date = :'datesel'::date
  ),
  filter_start_times AS (
    SELECT
      ttid,
      route,
      dir,
      op_date,
      start_time
    FROM unnest_start_times
    WHERE start_time >= :'time_1'::interval
      AND start_time <= :'time_2'::interval
  ),
  add_segments AS (
    SELECT
      tt.ttid, tt.route, tt.dir, tt.op_date, tt.start_time,
      tt.start_time + s.i_time  AS i_time,
      tt.start_time + s.j_time  AS j_time,
      s.linkid
    FROM filter_start_times   AS tt
    INNER JOIN sched.segments AS s
      ON tt.ttid = s.ttid
    WHERE tt.start_time + s.j_time <= :'time_2'::interval
  )

SELECT
  linkid,
  min(op_date) || ' - ' || max(op_date) AS date_rng,
  string_agg(DISTINCT route || '_' || dir, ', ') AS routes,
  count(*) AS n_trips
FROM add_segments
GROUP BY linkid
ORDER BY linkid;
